# Aviasales Web Scraping Project
## Alexander Zaznobin
This Python script is designed to scrape ticket prices from a website using a combination of libraries such as requests, BeautifulSoup, selenium, and others. It processes the data and outputs the prices and airline company information for specific routes and dates. 


**Aviasales**:[https://www.aviasales.com/]
### What is inside directory: 
* aviasales_scrpr.py -  executable file 
* conf.json - configuration
* airports.csv - airports codes and names data 
* requrements.txt - installed modules <i>.python3 -m pip freeze > requirements.txt<i>

#### Data collected:
![aviasales scraper](https://user-images.githubusercontent.com/127748062/229361563-90ac371a-07fe-4161-9f31-4864221a1f79.png)

#### The code creates tickets.log as output. 


#### Interface: 
-----------------
The script accepts the following command-line arguments to define the search parameters:

1. -sac, --start_ariport_code: (Optional) The 3-letter airport code of the starting airport (e.g., TLV). If not provided, the default value is 'TLV'.

2. -eac, --end_ariport_code: (Optional) The 3-letter airport code of the destination airport(s) (e.g., TBS). You can provide multiple airport codes separated by commas. If not provided, a default set of destination airports will be used.

3. -dr, --daterange: (Optional) The date range for the search, provided as a single string in the format DDMMDDMM

4. -db, --database: (Optional) A flag indicating if the database should be used. Include this flag if you want to use the database; otherwise, the database will not be used.

Example Usage:
1. To save results to database, you need to insert your mysql password to the conf.json file in the mysql_pwd parameter. 

2. To use the script with default settings, simply run:
   python script_name.py

3. To search for flights from TLV to JFK between September 1st and September 9th, 2023, and use the database:
   python aviasales_scrpr.py -sac TLV -eac JFK -dr 01090909 -db

Note: Make sure you have the required libraries installed and the necessary configuration set up before running the script.

#### Deafult parameters: 
1. -sac TLV
2. -eac ['SVO', 'TBS','EVN','ALA','BEG','GYD','TAS', 'PEK','JFK', 'SIN', 'HND', 'ICN', 'DOH', 'CDG', 'NRT', 'LHR', 'IST', 'DXB', 'MAD', 'MUC', 'ATL', 'AMS',
        'FCO', 'LGW', 'CPH']
3. -dr 14091409
4. -db False


#### Proxy:
The source site can try to block you scraping process by CAPCHA, if you are trying to get a more than some small amount of tickets per day.
If you want - you can use a proxy to reach the data.
To use proxy you have to: 
1) set "use_proxy": 1 in config file
2) fill the file proxy_list.txt with the actually working proxies from the source you would like.
e.g https://free-proxy-list.net/ provide free list of proxy.

#### Proxy API.
Additionaly you can use API to get list of proxies if you have one.
To do this you have to: 
1) set ""use_proxy_api": 1 in config file
2) Make a text file with you proxy API URL.
3) set the path to your proxy API URL text file "proxy_api_link_file": "path/proxy_api.txt",in config file 