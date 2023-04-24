# Aviasales Web Scraping Project
## Alexander Zaznobin
This Python script is designed to scrape ticket prices from a website using a combination of libraries such as requests, BeautifulSoup, selenium, and others. It processes the data and outputs the prices and airline company information for specific routes and dates. 


**Aviasales**:[https://www.aviasales.com/]
### What is inside directory: 

* conf.json - configuration
* requrements.txt - installed modules <i>.python3 -m pip freeze > requirements.txt<i>
* airports.csv - airports codes and names data 
* aviasales_scrpr.py -  executable file 
* interface.py - interface module
* mysql_scraper.py - mysql module
* proxies.py - usage of proxies functional 
* capcha_speech_recognition.py - capcha recognition functional 
* api_module.py - api module

#### Data collected:
![aviasales scraper (1)](https://user-images.githubusercontent.com/127748062/234117187-9f581b40-225b-4c8e-b143-35db9d8ac0da.png)

#### The code creates tickets.log as output. 

### Used API: 
https://traveltables.com/compare/russia/moscow/vs/israel/tel-aviv-yafo/cost-of-living#estimate
I collect average taxi Start, Normal Tariff for cities in table city of mysql DB.
The free API is limited to 15 request, so taxi tariff table can be shorter than start cities and end cities of tickets list.


#### Interface: 
-----------------
The script accepts the following command-line arguments to define the search parameters:

1. -sac, --start_ariport_code:  The 3-letter airport code of the starting airport (default value-  TLV). You can provide multiple airport codes separated by indent.

2. -eac, --end_ariport_code: The 3-letter airport code of the destination airport(s) (default value-  TLV). You can provide multiple airport codes separated by indent. 

3. -dr, --daterange: The date range for the search, provided as a single string in the format DDMMDDMM (default value-  07070707)

4. -db, --database: A flag indicating if the database should be used. Include this flag if you want to use the database; otherwise, the database will not be used.

Example Usage:
1. To save results to database, you need to insert your mysql password to the conf.json file in the mysql_pwd parameter. 

2. To use the script with default settings, simply run:
   python script_name.py

3. To search for flights from TLV to JFK between September 1st and September 9th, 2023, and use the database:
   python aviasales_scrpr.py -sac TLV -eac JFK -dr 01090909 -db

Note: Make sure you have the required libraries installed and the necessary configuration set up before running the script.

#### if no arguments are proceed demotsration parameters are used: 
  --start_ariport_code ['SVO', 'TBS', 'EVN', 'ALA', 'BEG', 'GYD', 'TAS', 'PEK', 'JFK', 'SIN', 'HND', 'ICN', 'DOH',
                           'CDG', 'NRT', 'LHR', 'IST', 'DXB', 'MAD', 'MUC', 'ATL', 'AMS',
                           'FCO', 'LGW', 'CPH', 'VNO', 'DME', 'VKO', 'ZIA', 'SAW', 'ISL']
  --daterange: 13061306
  --end_ariport_code:  ['TLV']
  need_database = True

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
