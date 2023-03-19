
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


def get_airpots_codes( file_name) :

            try :
                aiports_codes_df = pd.read_csv(file_name)
                return aiports_codes_df
            except FileNotFoundError :
                raise FileNotFoundError(f"File {file_name} not found in current path")



def input_parameters(airport_df) :
    print((airport_df['time_zone_id']))
    start_point = input("in what airport you want to start: ")
    while start_point!="stop":
        filtered_df = airport_df[airport_df['name'].str.contains(start_point, case=False)]

        if not filtered_df.empty :
            if filtered_df.shape[0]==1:
                return filtered_df.iloc[0]
            else:
                print("too many options, choose one airport")
                for i in range(filtered_df.shape[0]):
                    print(filtered_df.iloc[i][['time_zone_id']][0],
                          filtered_df.iloc[i][['name']][0])

        else :
            print("String not found. Please enter airpot name")
        start_point = input("input city one more time (or "'stop'"):")

def main():
    pass
    try :
        config = get_config(CONFIG_NAME)
        logging.info(f"Config file {CONFIG_NAME} opened successfully")
    except FileNotFoundError as fnfer :
        logging.error(f"Config file {CONFIG_NAME} was not opened successfully. ERROR {fnfer}")
        return
    except json.decoder.JSONDecodeError as json_er :
        logging.error(f"Config file {CONFIG_NAME} was not opened successfully. ERROR {json_er}")
        return
    airport_df=get_airpots_codes(config["airports"])
    pd.set_option('display.max_columns', None)
    print(airport_df.columns)

    print(input_parameters(airport_df))





if __name__=="__main__":
    main()
