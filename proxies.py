import random
import requests
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import re
import threading
BREAK_PROXY_RANDOMIZER_INDEX = 100
def get_api_proxy_link (config) :
    """
     Retrieves the API link to obtain a list of proxies, if configured to use an API.

     Args:
         config (dict): A dictionary containing the necessary configuration settings.

     Returns:
         str or None: The API link to obtain a list of proxies, or None if not configured to use an API.
     """
    if config['use_proxy_api']==1:
        proxy_api_link_file_name=config['proxy_api_link_file']
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





def check_request_proxy (proxy_str ,url, good_proxy_list) :
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


def check_proxy_responce(url, config):
    """
      Checks if a proxy can successfully connect to a URL, and saves it to a file if it can.

      Args:
          url (str): The URL to connect to using the proxy.
          config (dict): A dictionary containing the necessary configuration settings.

      Returns:
          selenium.webdriver.Chrome: The Chrome driver used to connect to the URL.
      """
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
        t = threading.Thread(target=check_request_proxy, args=(proxy[:-1],url, good_list_of_proxy))
        threads.append(t)
    for t in threads :
        t.start()
    for t in threads :
        t.join()
