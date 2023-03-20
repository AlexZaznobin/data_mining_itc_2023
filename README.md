# Aviasales Web Scraping Project
## Alexander Zaznobin
I used python’s Selenium library to scrape the Aviasales website (see requirements.txt for versions). 
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
- Airport to start traveling 
- Date of the ticket
- Destination Airport 
- Ticket price 

#### The code creates tickets.log as output. 
