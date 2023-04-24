from mysql_scraper import get_table_to_df
from mysql_scraper import add_dataframe_to_sqltable
from mysql_scraper import get_engine
from mysql_scraper import make_reference
from mysql_scraper import get_mysql_cursor
import requests
import json
import pandas as pd



def api_city () :
    """
    Retrieve a list of cities with their corresponding details using the 'cost-of-living-and-prices' API.

    Returns:
    pd.DataFrame: A Pandas DataFrame containing details of various cities, such as their name, country, region, and
    coordinates.
    """

    url = "https://cost-of-living-and-prices.p.rapidapi.com/cities"

    headers = {
        "content-type" : "application/octet-stream",
        "X-RapidAPI-Key" : "39c686ff8cmsh7047957fa958c6ep1044f0jsndf0245b716fb",
        "X-RapidAPI-Host" : "cost-of-living-and-prices.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers)
    result_of_response = response.json()
    cities = result_of_response['cities']
    api_data_cities = pd.DataFrame.from_dict(cities)
    return api_data_cities


def api_prices (querystring) :
    """
    Returns a dictionary of prices data for a given city and product type.

    Args:
        querystring (dict): A dictionary of query parameters that includes 'city' and 'product'.

    Returns:
        dict: A dictionary of prices data for the specified city and product type.
    """

    url = "https://cost-of-living-and-prices.p.rapidapi.com/prices"

    headers = {
        "content-type" : "application/octet-stream",
        "X-RapidAPI-Key" : "39c686ff8cmsh7047957fa958c6ep1044f0jsndf0245b716fb",
        "X-RapidAPI-Host" : "cost-of-living-and-prices.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    return response


def get_taxi_price (row) :
    """
     This function takes a row of data containing city and country names, sends an API request to get taxi prices
     for the city, and returns the average taxi price for starting a ride in normal tariff.

     Args:
     row (pandas.Series): A pandas Series containing 'city_name' and 'country_name' as keys.

     Returns:
     float: The average taxi price for starting a ride in normal tariff in the given city and country.
     If the API request fails, the reason for the failure is returned.

     """
    series = row[["city_name", "country_name"]]
    dict_city = series.to_dict()
    response = api_prices(dict_city)
    try :
        json_response = response.json()
        json_df = pd.DataFrame(json_response['prices'])
        json_df.to_csv('json_df.csv', index=False)
        taxi_avg = json_df.loc[json_df["item_name"] == "Taxi Start, Normal Tariff", 'usd'].values['avg']
        return taxi_avg
    except :
        return 'no_data_from_api'


def make_api_price_request (config, logging) :
    """
    This function retrieves city data from the database and an API endpoint,
    merges them, calculates the average taxi price per kilometer for each city,
    and saves the resulting data to table

    Args:
    config (dict): A dictionary containing configuration data for connecting to the database and the API endpoint.

    Returns:
    None

    """

    city_db = get_table_to_df(config, 'city')
    api_city_df = pd.read_csv('api_city_df.csv')
    city_wide_data = pd.merge(city_db, api_city_df, how='inner', left_on='name', right_on='city_name')
    city_wide_data['taxi_price_per_km'] = city_wide_data.apply(get_taxi_price, axis=1)
    df_to_load = city_wide_data.loc[:, ['id', 'taxi_price_per_km']]
    df_to_load = df_to_load.loc[df_to_load['taxi_price_per_km'] != 'no_data_from_api', :]
    df_to_load = df_to_load.rename(columns={'id' : 'city_id'})
    engine = get_engine(config)
    add_dataframe_to_sqltable(df_to_load, engine, 'taxi', False, logging)
    cursor = get_mysql_cursor()
    make_reference(cursor, "taxi", "city_id", "city", "id")
