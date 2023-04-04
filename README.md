# Aviasales Web Scraping Project
## Alexander Zaznobin
This Python script is designed to scrape ticket prices from a website using a combination of libraries such as requests, BeautifulSoup, selenium, and others. It processes the data and outputs the prices and airline company information for specific routes and dates. 


**Aviasales**:[https://www.aviasales.com/]
### What is inside directory: 
* aviasales_scrpr.py -  executable file 
* conf.json - configuration
* airports.csv - airports codes and names data 
* requrements.txt - installed modules <i>.python3 -m pip freeze > requirements.txt<i>

#### User parameters:
- Airport to start
- first day of range to search for tickets 
- range size in days 
- Destination Airport 
#### Options:
if Destination Airport is set to 'any', data is collected for all destinations.

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

1. To use the script with default settings, simply run:
   python script_name.py

2. To search for flights from TLV to JFK between September 1st and September 9th, 2023, and use the database:
   python aviasales_scrpr.py -sac TLV -eac JFK -dr 01090909 -db


Note: Make sure you have the required libraries installed and the necessary configuration set up before running the script.

#### Deafult parameters: 
1. -sac TLV
2. -eac ['SVO', 'TBS','EVN','ALA','BEG','GYD','TAS', 'PEK','JFK', 'SIN', 'HND', 'ICN', 'DOH', 'CDG', 'NRT', 'LHR', 'IST', 'DXB', 'MAD', 'MUC', 'ATL', 'AMS',
        'FCO', 'LGW', 'CPH']
3. -dr 14091409
4. -db False