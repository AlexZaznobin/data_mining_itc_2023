import random
import time
import requests
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import re
import threading
import os
import urllib3


def get_api_proxy_link (config) :
    """
     Retrieves the API link to obtain a list of proxies, if configured to use an API.

     Args:
         config (dict): A dictionary containing the necessary configuration settings.

     Returns:
         str or None: The API link to obtain a list of proxies, or None if not configured to use an API.
     """

    proxy_api_link_file_name = config['proxy_api_link_file']
    with open(proxy_api_link_file_name, 'r') as file :
        proxy_link = file.read()
    return proxy_link


def save_file_api_proxy_list (config) :
    """
    Retrieves a list of proxies from an API, and adds them to a file.

    Args:
        config (dict): A dictionary containing the necessary configuration settings.

    Returns:
        None
    """
    proxi_text = ""
    if config['use_proxy_api'] == 1 :
        api_url = get_api_proxy_link(config)
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome = webdriver.Chrome(options=chrome_options)
        chrome.get(api_url)
        body_text = chrome.page_source
        proxi_text = re.sub(r'<.*>', '', body_text)

    with open(config['good_proxy'], 'r') as result_file :
        old_list_of_proxy = result_file.readlines()
    list_of_proxy = proxi_text.split('\n') + old_list_of_proxy
    list_of_proxy = list(set(list_of_proxy))
    good_list_of_proxy = []
    m_thread_proxy_check(list_of_proxy, good_list_of_proxy, 'http://checkip.amazonaws.com/')

    with open(config['good_proxy'], 'w') as result_file :
        result_file.write('\n'.join(good_list_of_proxy))


def check_request_proxy (proxy_str, url, good_proxy_list) :
    """
       Checks if a proxy is responding to requests.

       Args:
           proxy_str (str): The address of the proxy to check.
           url (str): The URL to request using the proxy.
           good_proxy_list (list): A list to add the proxy to if it is responding.

       Returns:
           bool: True if the proxy is responding, False otherwise.
    """
    status = False
    proxy = {
        'http' : 'http://' + proxy_str,
        'https' : 'https://' + proxy_str
    }

    try :
        response = requests.get(url, proxies=proxy, timeout=3)
        if response.status_code == 200 :
            status = True
            good_proxy_list.append(proxy_str)
    except :
        pass
    return status


def check_proxy_response (url, config) :
    """
      Checks if a proxy can successfully connect to a URL, and saves it to a file if it can.

      Args:
          url (str): The URL to connect to using the proxy.
          config (dict): A dictionary containing the necessary configuration settings.

      Returns:
          selenium.webdriver.Chrome: The Chrome driver used to connect to the URL.
      """
    success_connection = 0
    break_index = 0
    if config['use_proxy'] == 1 :
        while success_connection == 0 and break_index < config['break_proxy_index'] :
            with open(config['good_proxy'], 'r') as result_file :
                proxy_list = list(set(result_file.readlines()))
            random_proxy = random.choice(proxy_list)[:-1]
            urllib3_test(random_proxy, url)
            driver = set_up_driver(config, random_proxy)
            print('check_proxy_response',driver)
            driver.get(url)
            try :

                data_header = driver.title[2] == '.'
                price_found = driver.title[:1] == '$'
                cheap_header = driver.title.split()[0] == 'Cheap'
                if (cheap_header or price_found or data_header) :
                    success_connection = 1
                    with open(config['great_proxy'], 'r') as result_file :
                        proxy_list = list(set(result_file.readlines()))
                    with open(config["great_proxy"], 'w') as result_file :
                        result_file.writelines("%s" % item for item in proxy_list)
                    with open(config["great_proxy"], 'a') as result_file :
                        result_file.write(random_proxy + '\n')
                else :
                    driver.close()
            except :
                break_index = break_index + 1
                driver.close()

    else :
        driver = set_up_driver(config)
        driver.get(url)
        time.sleep(config['page_load_timeout'])
    return driver


def set_up_driver (config, proxy=None) :
    """
    Set up a Chrome WebDriver with the given configuration and proxy.

    Args:
        config (dict): A dictionary containing the configuration options for the Chrome WebDriver.
             Contain a 'chrome_args' key with a list of Chrome arguments to use.
             Contain a 'use_proxy' key with a value of 1 to enable proxying.
        proxy (str): The proxy server address to use, in the form "host:port".

    Returns:
        A Chrome WebDriver instance configured with the specified options and proxy.

    Raises:
        FileNotFoundError: If the ChromeDriver binary is not found at the expected location.
    """
    path = '/usr/local/bin/chromedriver'

    if os.path.exists(path) and os.path.isfile(path) :
        print("ChromeDriver is in the correct location.")
    else :
        print("ChromeDriver not found at the specified location.")


    service_chromedriver = Service('/usr/local/bin/chromedriver')
    chrome_options = Options()
    for chrome_arg in config['chrome_args'] :
        chrome_options.add_argument(chrome_arg)
        if config['use_proxy'] == 1 :
            chrome_options.add_argument(f"--proxy-server={proxy}")
            rand_limit = config['size_of_window'][2]
            random_value = random.randint(1, rand_limit)
            x = config['size_of_window'][0] + random_value
            y = config['size_of_window'][1] + random_value
            chrome_options.add_argument(f"--window-size={x},{y}")

    return webdriver.Chrome(executable_path=path, options=chrome_options)


def m_thread_proxy_check (list_of_proxy, good_list_of_proxy, url) :
    """
    Checks if a list of proxies are responding, and adds the ones that are to a list.
    Args:
    list_of_proxy (list): A list of proxies to check.
    good_list_of_proxy (list): A list to add the responsive proxies to.
    url (str): The URL to use for checking the proxies.
    config (dict): A dictionary containing the necessary configuration settings.

    Returns:
    None
    """
    threads = []

    for index, proxy in enumerate(list_of_proxy) :
        t = threading.Thread(target=check_request_proxy, args=(proxy[:-1], url, good_list_of_proxy))
        threads.append(t)
    for t in threads :
        t.start()
    for t in threads :
        t.join()


def urllib3_test (proxy_url, target_url) :
    print('urllib3_test target_url', target_url)
    proxy_str = 'http://' + proxy_url
    print('urllib3_test proxy_url', proxy_str)
    http = urllib3.ProxyManager(proxy_str)
    print('urllib3_test http', http)
    response = http.request('GET', target_url)
    print('urllib3_test response.status', response.status)

