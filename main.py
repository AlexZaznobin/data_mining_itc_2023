from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException

from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import time
import os
import requests
import datetime
import json
import logging
import sys
import pandas as pd
import re
import requests



#
# from gevent import monkey
# monkey.patch_all()
# import grequests
CONFIG_NAME = 'conf.json'


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


def get_airpots_codes (file_name) :
    try :
        aiports_codes_df = pd.read_csv(file_name)
        return aiports_codes_df
    except FileNotFoundError :
        raise FileNotFoundError(f"File {file_name} not found in current path")


def get_airport (airport_df) :
    start_point = input("in what airport you want to start: ")
    while start_point != "stop" :
        filtered_df = airport_df[airport_df['name'].str.contains(start_point, case=False)]

        if not filtered_df.empty :
            if filtered_df.shape[0] == 1 :
                return filtered_df.iloc[0]['code']
            else :
                print("too many options, choose one airport")
                for i in range(filtered_df.shape[0]) :
                    print(filtered_df.iloc[i][['time_zone_id']][0],
                          filtered_df.iloc[i][['name']][0])
        else :
            print("String not found. Please enter airpot name")
        start_point = input("input city one more time (or "'stop'"):")


def get_date_range () :
    incorrect_date = True
    start_point = input("insert date range DDMMDDMM:")
    while incorrect_date :
        try :
            start_date = int(start_point[:2])
            start_month = int(start_point[2 :4])
            end_date = int(start_point[4 :6])
            end_month = int(start_point[6 :])
            start = datetime.date(year=2022, month=start_month, day=start_date)
            end = datetime.date(year=2022, month=end_month, day=end_date)
            days_nomber = (end - start).days
            incorrect_date = False
        except :
            start_point = input("insert correct date range DDMMDDMM or stop:")
            if start_point == "stop" :
                incorrect_date = False
    print(start, days_nomber)
    return (start, days_nomber+1)

def page_processing(response):
    time.sleep(2)
    spec_soup = BeautifulSoup(response.text, 'html.parser')
    price_div = spec_soup.find_all('div', class_='web-app')

    print(spec_soup.get_text())
    # class_price = spec_soup.find_all('div', class_="ticket-mobile")
    print("price_div", price_div)
    # rating_num = rating.split('#')[1]
    # movie_info = (spec_soup.find_all('meta')[2].get('content'))
    # movie_name = movie_info.split(":")
    # movie = movie_name[0]
    # director = movie_name[1].split('.')[0].replace("Directed by ", "")
    # result_string = rating_num + ' - ' + movie + ' - ' + director
    return
def page_selen(url):
    service = Service('/usr/local/bin/chromedriver')
    driver = webdriver.Chrome(service=service)
    driver.get(url)
    time.sleep(10)
    try:
        price= driver.find_elements(By.CSS_SELECTOR,'[data-test-id="price"]')[3].text
    except:
        price=None
    return price
def get_ticket_price (list_of_urls, config) :

    ua = UserAgent(browsers=config["browsers"])
    headers = {"User-Agent" : ua.random}
    # reqs = (grequests.get(u, headers=headers) for u in list_of_urls)

    tickets_dict = {}
    for index, url in enumerate(list_of_urls):
        price=page_selen(url)
        parameters = re.search('.*request', url).group(0)[-18 :-7]
        tickets_dict[parameters] = price
        print(parameters, price )
        logging.info(f"{parameters},{price}")

    return tickets_dict

def get_url_list (start_code, start_date, days_nomber, config, end_list, pass_num="1") :
    logging.info(f"start get_url_list with: {start_code, start_date, days_nomber, config, end_list}")
    list_of_url = []
    end_of_url="request_source=search_form"
    if days_nomber == 0 :
        data_code = ((start_date).strftime('%d%m'))
        for end_code in end_list :
            new_link = config[
                           "link_constructor"] + start_code + data_code + end_code + pass_num + end_of_url

    for day in range(days_nomber) :
        delta = datetime.timedelta(days=day)
        data_code = ((start_date + delta).strftime('%d%m'))
        for end_code in end_list :
            new_link = config["link_constructor"] + start_code + data_code + end_code + pass_num+end_of_url
            list_of_url.append(new_link)
    print('get_url_list',len(list_of_url) )
    logging.info(f"finish get_url_list of {len(list_of_url)} items")
    return list_of_url


def main () :

    logging.basicConfig(format='%(asctime)s  function_mane: %(funcName)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S',
                        filename='tickets.log',
                        filemode='w',
                        level=logging.INFO)

    try :
        config = get_config(CONFIG_NAME)
        logging.info(f"Config file {CONFIG_NAME} opened successfully")
    except FileNotFoundError as fnfer :
        logging.error(f"Config file {CONFIG_NAME} was not opened successfully. ERROR {fnfer}")
        return
    except json.decoder.JSONDecodeError as json_er :
        logging.error(f"Config file {CONFIG_NAME} was not opened successfully. ERROR {json_er}")
        return
    airport_df = get_airpots_codes(config["airports"])
    pd.set_option('display.max_columns', None)

    start = datetime.datetime.now()
    start_code = (get_airport(airport_df))
    start_date, days_nomber = get_date_range()
    end_point = airport_df['code']
    # end_point ="TLV"
    # start_code="TLV"

    # start_date= datetime.datetime.strptime("2022-04-03", '%Y-%m-%d')
    url_list = get_url_list(start_code, start_date, days_nomber, config, end_point)
    get_ticket_price (url_list, config)

if __name__ == "__main__" :
    main()
