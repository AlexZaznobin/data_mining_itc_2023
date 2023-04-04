import random
import pymysql
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import datetime
import json
import logging
import pandas as pd
import re
import os
import threading
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from capcha_speech_recognition import g_capcha_solver
from interface import get_date_range
from interface import get_airport
from mysql_scraper import set_up_db
from mysql_scraper import fill_airport_table
from mysql_scraper import fill_city_table
from mysql_scraper import fill_aircompany_table
from mysql_scraper import make_references
from mysql_scraper import fill_ticket_table

CONFIG_NAME = 'conf.json'
BATCH_SIZE = 1
PARENTING_LIMIT = 20


class Airport :
    def __init__ (self, code=None, city=None) :
        self.code = code
        self.fill_name = None
        self.city = city

    def __str__ (self) :
        try :
            return ",".join((str(self.code),
                             str(self.city)
                             ))
        except :
            return "N/A"


class Ticket :
    def __init__ (self,
                  start_airport=None,
                  date=None,
                  dest_airport=None,
                  price=None,
                  air_company=None,
                  start_time=None,
                  duration=None,
                  layover_info=None) :
        self.start_airport = start_airport
        self.date = date
        self.start_time = start_time
        self.dest_airport = dest_airport
        self.price = price
        self.air_company = air_company
        self.duration = duration
        self.stamp = datetime.datetime.now()
        self.layover_info = layover_info

    def __str__ (self) :
        try :
            return ",".join((str(self.start_airport),
                             # str(self.date),
                             str(self.dest_airport),
                             str(self.price),
                             str(self.air_company),
                             str(self.start_time),
                             str(self.stamp),
                             str(self.duration),
                             str(self.layover_info)
                             ))
        except :
            return "N/A"


def get_config (conf_name) :
    """
    Creates json file from the configuration file

    Args:
        conf_name: name of configuration file

    Returns:
       json file with the parameters of configuration
    """

    try :
        file = open(conf_name)
        return json.load(file)
    except FileNotFoundError :
        raise FileNotFoundError(f"File {conf_name} not found in current path")


def get_airport_codes (file_name) :
    """Load airport codes from a CSV file"""
    if not os.path.isfile(file_name) :
        raise FileNotFoundError(f"File {file_name} not found")
    return pd.read_csv(file_name)


def extract_data_page (driver, current_ticket, config) :
    currency, currency_position = currency_check(driver)
    try :
        time.sleep(random.random())
        # Wait up to 20 seconds for the element to be present on the page
        wait = WebDriverWait(driver, 15)
        prices = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-test-id="price"]')))
        for price in prices :
            ticket_web_element = get_full_ticket_we(price, config)
            if ticket_web_element != None :
                return_price = price.text
                current_ticket.price = return_price
                extract_aicompany(ticket_web_element, current_ticket)
                extract_city(ticket_web_element, current_ticket)
                extract_time(ticket_web_element, current_ticket)
                extract_layover(ticket_web_element, current_ticket, config)
                break
    except :
        return_price = None

    return return_price


def extract_time (ticket_web_element, current_ticket) :
    ticket_info = ticket_web_element.text.split('\n')
    start = 0
    for item in ticket_info :
        if (item.find(":", start) != -1) & (item.find("Duration:", start) == -1) :
            time_info_list = item.split(":")
            minutes = time_info_list[1][:2]
            ampm = time_info_list[1][-2 :]
            minutes = int(minutes)
            if ampm == "pm" :
                hours = int(time_info_list[0]) + 12
            else :
                hours = int(time_info_list[0])
            month = int(current_ticket.date[2 :])
            day = int(current_ticket.date[:2])
            current_ticket.start_time = datetime.datetime(
                2023,
                month,
                day,
                hours,
                minutes,
                0)

        if (item.find("Duration:", start) != -1) :
            current_ticket.duration = item
            break


def extract_layover (ticket_web_element, current_ticket, config) :
    stop_elements = ticket_web_element.find_elements(By.XPATH, "//div[contains(@class, 'segment-route__stop')]")
    layover_count = 0
    if len(stop_elements) > 0 :
        for stop_element in stop_elements :
            ticket_containing_stop_element = get_full_ticket_we(stop_element, config)
            if ticket_containing_stop_element == ticket_web_element :
                layover_count = +1
            if (layover_count > 0) & (ticket_containing_stop_element == None) :
                break
    current_ticket.layover_info = layover_count


def get_full_ticket_we (webelement, config) :
    child_element = webelement
    ticket_element = None
    for i in range(config['depth_of_parenting_ticket_widget']) :
        try :
            parent_element = child_element.find_element(By.XPATH, "parent::div")
            child_element = parent_element
            if parent_element.get_attribute("class") == "ticket-desktop" :
                ticket_element = parent_element
        except :
            break
    return ticket_element


def currency_check (driver) :
    current_url = driver.current_url
    if "https://www.aviasales.ru/" in current_url :
        return ('₽', -1)
    if "https://www.aviasales.com/" in current_url :
        return ("$", 0)


def extract_aicompany (driver, current_ticket) :
    wait = WebDriverWait(driver, 1)
    text_elements = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-test-id="text"]')))
    for i, text_element in enumerate(text_elements) :
        if (text_element.text == current_ticket.price) :
            current_ticket.air_company = text_elements[i + 3].text
            return True


def extract_city (driver, current_ticket) :
    wait = WebDriverWait(driver, 1)
    city_elements = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-test-id="city"]')))
    current_ticket.start_airport.city = city_elements[0].text
    current_ticket.dest_airport.city = city_elements[1].text


def page_processing_slnm (url, config) :
    """
    get price from page
    Args:
        url: url for combination of start point, date, and en point
    Use selenium
    Returns:
        price for the cheapest ticket
    """
    parameters = re.search('.*request', url).group(0)[-18 :-7]
    start_airport = Airport(code=parameters[:3])
    dest_airport = Airport(code=parameters[-4 :-1])
    page_ticket = Ticket(start_airport=start_airport,
                         date=parameters[3 :-4],
                         dest_airport=dest_airport)
    service = Service('/usr/local/bin/chromedriver')
    chrome_options = Options()
    for chome_arg in config['chome_args'] :
        chrome_options.add_argument(chome_arg)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(url)

    if extract_data_page(driver, page_ticket, config) == None :
        g_capcha_solver(driver, logging)
        extract_data_page(driver, page_ticket, config)

    with open(config['result_file'], 'a') as result_file :
        result_file.write(str(page_ticket) + '\n')
    print(str(page_ticket))
    logging.info(str(page_ticket) + '\n')
    return page_ticket


def m_thread_batch_scraping (list_of_urls, config) :
    """
    print prices to log and output
    Args:
        list_of_urls:  list of urls for combination of start point, date, and en point
    Use selenium
    Returns:
    """
    threads = []
    for index, url in enumerate(list_of_urls) :
        t = threading.Thread(target=page_processing_slnm, args=(url, config))
        threads.append(t)
    for t in threads :
        t.start()
    for t in threads :
        t.join()


def get_url_list (start_code, start_date, days_number, config, end_list, pass_num="1") :
    """
    make a list of urls for request
    Args:
        start_code: start airport code (string)
        start_date: start search date
        days_number: range size in days
        config: constant info for scraping
        end_list: end  airport code (list of string )
        pass_num="1": passengers number 1 by default

    Returns:
        list_of_url to search
    """
    logging.info(f"start get_url_list with: {start_code, start_date, days_number, config, end_list}")
    list_of_url = []
    end_of_url = "request_source=search_form"
    if days_number == 0 :
        data_code = ((start_date).strftime('%d%m'))
        for end_code in end_list :
            new_link = config[
                           "link_constructor"] + start_code + data_code + end_code + pass_num + end_of_url

    for day in range(days_number) :
        delta = datetime.timedelta(days=day)
        data_code = ((start_date + delta).strftime('%d%m'))
        for end_code in end_list :
            new_link = config["link_constructor"] + start_code + data_code + end_code + pass_num + end_of_url
            list_of_url.append(new_link)
    print('get_url_list', len(list_of_url))
    logging.info(f"finish get_url_list of {len(list_of_url)} items")
    return list_of_url


def load_scraper_config () :
    logging.basicConfig(format='%(asctime)s  function_mane: %(funcName)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S',
                        filename='scraper_script.log',
                        filemode='a',
                        level=logging.INFO)
    try :
        config = get_config(CONFIG_NAME)
        logging.info(f"Config file {CONFIG_NAME} opened successfully")
        return config
    except FileNotFoundError as fnfer :
        logging.error(f"Config file {CONFIG_NAME} was not opened successfully. ERROR {fnfer}")
        return
    except json.decoder.JSONDecodeError as json_er :
        logging.error(f"Config file {CONFIG_NAME} was not opened successfully. ERROR {json_er}")
        return


def main () :
    config = load_scraper_config()
    with open(config['result_file'], 'w') as result_file :
        result_file.write("start_airport_code,"
                          "start_city_name,"
                          "end_airport_code,"
                          "end_city_name,"
                          "price,aircompany_name,"
                          "flight_date_time,"
                          "scraping_timestamp,"
                          "duration_time,"
                          "layovers\n")
    airport_df = get_airport_codes(config["airports"])
    cursor = set_up_db(config["db_name"], logging)
    pd.set_option('display.max_columns', None)
    start = datetime.datetime.now()
    start_aero_code = get_airport(airport_df, "start")
    start_date, days_number = get_date_range()
    end_point = [get_airport(airport_df, "end")]
    try :
        if end_point == ["any"] :
            end_point = airport_df['code'].values
    except :
        pass
    url_list = get_url_list(start_aero_code, start_date, days_number, config, end_point)
    start = datetime.datetime.now()
    logging.info(f" send batch of urls size {len(url_list[:config['batch_size']])} ")
    logging.info(f" Config file {CONFIG_NAME} opened successfully")
    batch_number = int(len(url_list) / config['batch_size'])
    if batch_number < 1 :
        batch_number = 1

    for i in range(batch_number) :
        m_thread_batch_scraping(url_list[:config['batch_size']], config)
        url_list = url_list[config['batch_size'] :]
        if len(url_list) > 0 and len(url_list) < config['batch_size'] :
            m_thread_batch_scraping(url_list, config)
    end = datetime.datetime.now()
    logging.info(f"this takes: {end - start} sec ")

    city_df=fill_city_table(logging, config)
    airport_df=fill_airport_table(logging, config)
    aircompany_df=fill_aircompany_table(logging, config)
    ticket_df=fill_ticket_table(logging, config,airport_df,aircompany_df )
    make_references(logging,config)

if __name__ == "__main__" :
    main()
