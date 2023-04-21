import random
import requests
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import re
import threading

from selenium.webdriver.common.proxy import Proxy, ProxyType
import time
BREAK_PROXY_RANDOMIZER_INDEX = 100
def get_api_proxy_link (config) :
    """
    Creates a connection engine to the MySQL database using the provided config.

    Args:
        config (dict): A dictionary containing the necessary configuration settings.

    Returns:
       link to proxy url api
    """
    if config['use_proxy_api']==1:
        proxy_api_link_file_name=config['proxy_api_link_file']
        with open(proxy_api_link_file_name, 'r') as file :
            proxy_link = file.read()
    return proxy_link

def save_file_api_proxy_list (config) :
    """
    Creates a connection engine to the MySQL database using the provided config.

    Args:
        config (dict): A dictionary containing the necessary configuration settings.

    Returns:
       link to proxy url api
    # """
    api_url=get_api_proxy_link(config)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome = webdriver.Chrome(options=chrome_options)
    chrome.get(api_url)
    body_text = chrome.page_source
    proxi_text = re.sub(r'<.*>', '', body_text)

    with open("proxy_list.txt", 'r') as result_file :
        old_list_of_proxy = result_file.readlines()
    list_of_proxy= proxi_text.split('\n')+old_list_of_proxy
    list_of_proxy=list(set(list_of_proxy))
    good_list_of_proxy=[]
    m_thread_proxy_check(list_of_proxy, good_list_of_proxy,'http://checkip.amazonaws.com/' , config)

    with open("proxy_list.txt", 'w') as result_file :
        result_file.write('\n'.join(good_list_of_proxy))



def create_proxy_list (number_of_proxy,config) :
    """
    Create proxy list of given length
    Args:
        number_of_proxy: number of needed proxies
    Returns:
        list of good proxies of  given length  or list of 0 of  given length
    """
    save_file_api_proxy_list(config)
    proxy_list = []
    with open("proxy_list.txt", 'r') as result_file :
        proxy_list = result_file.readlines()

    good_proxy_list = []
    break_index = 0
    while (len(good_proxy_list) != number_of_proxy) and (break_index != BREAK_PROXY_RANDOMIZER_INDEX) :
        break_index = break_index + 1
        random_element = random.choice(proxy_list)
        if check_proxy(random_element[:-1], 'http://httpbin.org/ip') :
            good_proxy_list.append(random_element[:-1])
    if break_index == BREAK_PROXY_RANDOMIZER_INDEX :
        good_proxy_list = [0 for i in range(number_of_proxy)]
    return good_proxy_list



def check_request_proxy (proxy_str ,url, good_proxy_list) :
    """
    Check if proxies are good
    Args:
        proxy_str: adres of proxy
    Returns:
        status: True or false
    """
    status = False
    proxy = {
        'http' : 'http://' + proxy_str,
        'https' : 'https://' + proxy_str
    }
    try :
        response = requests.get(url, proxies=proxy, timeout=3)
        print(proxy_str,response)
        if response.status_code == 200 :
            status = True
            good_proxy_list.append(proxy_str)
    except :
        pass
    return status

def proxy(proxy_ip_port,url):

    # change 'ip:port' with your proxy's ip and port
    proxy = Proxy()
    proxy.proxy_type = ProxyType.MANUAL
    proxy.http_proxy = proxy_ip_port
    proxy.ssl_proxy = proxy_ip_port

    capabilities = webdriver.DesiredCapabilities.CHROME
    proxy.add_to_capabilities(capabilities)

    # replace 'your_absolute_path' with your chrome binary absolute path
    driver = webdriver.Chrome( desired_capabilities=capabilities)
    driver.get(url)
    driver.quit()


def check_proxy_responce(url, config):
    service = Service('/usr/local/bin/chromedriver')
    chrome_options = Options()
    sucess_connection=0
    break_index = 0
    while sucess_connection==0 and break_index<100:
        with open("proxy_list.txt", 'r') as result_file :
            proxy_list = result_file.readlines()
        random_proxy = random.choice(proxy_list)[:-1]
        try:
            for chome_arg in config['chome_args'] :
                chrome_options.add_argument(chome_arg)
                if config['use_proxy'] == 1 :
                    chrome_options.add_argument(f"--proxy-server={random_proxy}")
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.get(url)
            if (driver.title[:1]=='$' or driver.title[:1]=='C'):
                sucess_connection=1
                with open("great_proxy_list.txt", 'a') as result_file :
                    result_file.write(random_proxy+'\n')
        except:
            break_index=break_index+1
            driver.close()

    return driver


def m_thread_proxy_check (list_of_proxy,good_list_of_proxy,url,config):
    """

    """

    threads = []

    for index, proxy in enumerate(list_of_proxy) :
        t = threading.Thread(target=check_request_proxy, args=(proxy[:-1],url, good_list_of_proxy))
        threads.append(t)
    for t in threads :
        t.start()
    for t in threads :
        t.join()
