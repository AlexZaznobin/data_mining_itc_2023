import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
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
import requests

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


def get_airport_codes (file_name) :
    """Load airport codes from a CSV file"""
    if not os.path.isfile(file_name) :
        raise FileNotFoundError(f"File {file_name} not found")
    return pd.read_csv(file_name)


def see_country_id (airport_df) :
    """
    Print the count of airports per country.

    Args:
        airport_df: A pandas DataFrame containing airport codes.

    Returns:
        None.
    """
    grouped_airport_df = airport_df[['country_id', 'time_zone_id']].groupby('country_id').count()
    print(grouped_airport_df.to_string())


def see_airports (airport_df, country_id) :
    """
    Args:
        airport_df: A pandas DataFrame containing airport codes.

        country_id: country -Two capital letters

    print  all airport for country_id, or call see_country_id() function which print list of country_id

    Returns:
        None.
    """
    if country_id == "" :
        see_country_id(airport_df)
    else :
        print(airport_df[airport_df['country_id'] == country_id][['name']])


def get_airport (airport_df, start_or_end) :
    """
    Args:
        airport_df: A pandas DataFrame containing airport codes.

        start_or_end: Flag - start or end?

    takes input from the user to find out what from/ to what airport she wants to fly

    Returns:
        code if airport, or 'any' flag
    """

    if start_or_end == "end" :
        start_end_point = input(
            f"Type first letters (e.g 'tbil') of the airport you want to {start_or_end},\n \
            or type 'any' for all destinations in the world\n ")
        if start_end_point == "any" :
            return start_end_point
    else :
        start_end_point = input(f"Type first letters (e.g 'ben gu') of the airport you want to {start_or_end}:")
    while start_end_point != "stop" :
        if start_end_point == "" :
            country_code = input("enter country code (two letters capital):")
            see_airports(airport_df, country_code)
            start_end_point = input("input city one more time (or "'stop'"):")
            continue
        filtered_df = airport_df[airport_df['name'].str.contains(start_end_point, case=False)]

        if not filtered_df.empty :
            if filtered_df.shape[0] == 1 :
                print(filtered_df.iloc[0]['name'])
                return filtered_df.iloc[0]['code']
            else :
                print("too many options, choose one airport")
                for i in range(filtered_df.shape[0]) :
                    print(filtered_df.iloc[i][['time_zone_id']][0],
                          filtered_df.iloc[i][['name']][0])
        else :
            print("String not found. Please enter airport name")
        start_end_point = input("input city one more time (or "'stop'"):")


def get_date_range () :
    """
    get input from user in format DDMMDDMM

    Returns:
        start: first day in suitable range of dates
        days_number: - number of days in suitable range
    """
    incorrect_date = True
    start_point = input("insert date range DDMMDDMM (for April 2023 - 01043004):")
    while incorrect_date :
        try :
            start_date = int(start_point[:2])
            start_month = int(start_point[2 :4])
            end_date = int(start_point[4 :6])
            end_month = int(start_point[6 :])
            start = datetime.date(year=2022, month=start_month, day=start_date)
            end = datetime.date(year=2022, month=end_month, day=end_date)
            days_number = (end - start).days + 1
            incorrect_date = False
        except :
            start_point = input("insert correct date range DDMMDDMM or stop:")
            if start_point == "stop" :
                incorrect_date = False

    return (start, days_number)


def page_processing_bs4 (url, config) :
    """
    get price from page
    Args:
        url: url for combination of start point, date, and en point
        config: Config file
    try to use bs4
    Returns:
        price for the cheapest ticket
    """
    ua = UserAgent(browsers=config['browsers'])
    headers = {"User-Agent" : ua.random}
    try :
        spec_response = requests.get(url, headers=headers)
        logging.info(f"We got response from the specific:  {url} ")
    except requests.exceptions.ConnectionError :
        logging.info(f"We did not get response from the specific:  {url} ")
        raise requests.exceptions.ConnectionError
    spec_soup = BeautifulSoup(spec_response.text, 'html.parser')
    with open('example.txt', 'w') as f :
        f.write(spec_response.text)
    prices = spec_soup.find_all("price")
    if prices == [] :
        return None
    else :
        return prices[0].text


def page_processing_slnm (url) :
    """
    get price from page
    Args:
        url: url for combination of start point, date, and en point
    Use selenium
    Returns:
        price for the cheapest ticket
    """
    service = Service('/usr/local/bin/chromedriver')
    chrome_options = Options()
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(url)
    time.sleep(15)

    # click_checkbox(driver, logging) plan to realise capcha handling later
    try :
        price = driver.find_elements(By.CSS_SELECTOR, '[data-test-id="price"]')[0].text
        time.sleep(random.random()*3)
    except :
        price = None


    parameters = re.search('.*request', url).group(0)[-18 :-7]
    print(f"{parameters[:3]},{parameters[3 :-4]}, {parameters[-4 :-1]}, {parameters[-1 :]},{price}")
    logging.info(f"{parameters[:3]},{parameters[3 :-4]}, {parameters[-4 :-1]}, {parameters[-1 :]},{price}")
    return


def get_ticket_price (list_of_urls, config) :
    """
    print prices to log and output
    Args:
        list_of_urls:  list of urls for combination of start point, date, and en point
    Use selenium
    Returns:
    """
    threads=[]
    for index, url in enumerate(list_of_urls) :
        t = threading.Thread(target=page_processing_slnm, args=(url,))
        threads.append(t)

    # start the threads
    for t in threads :
        t.start()

    # wait for the threads to finish
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
    airport_df = get_airport_codes(config["airports"])
    pd.set_option('display.max_columns', None)

    start = datetime.datetime.now()

    # # input_data_block
    start_aero_code = get_airport(airport_df, "start")
    start_date, days_number = get_date_range()
    end_point = [get_airport(airport_df, "end")]
    try :
        if end_point == ["any"] :
            end_point = airport_df['code'].values
    except :
        pass

    url_list = get_url_list(start_aero_code, start_date, days_number, config, end_point)
    start= datetime.datetime.now()
    get_ticket_price(url_list,config)
    end = datetime.datetime.now()
    print(f"get_ticket_price takes: {end-start} sec ")



if __name__ == "__main__" :
    main()
