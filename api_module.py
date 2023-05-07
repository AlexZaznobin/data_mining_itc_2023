from mysql_scraper import get_table_to_df
from mysql_scraper import add_dataframe_to_sqltable
from mysql_scraper import get_engine
from mysql_scraper import make_reference
from mysql_scraper import get_newitems
import requests
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
    try:
        cities = result_of_response['cities']
        api_data_cities = pd.DataFrame.from_dict(cities)
        return api_data_cities
    except:
        return 'no_data'


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
        # json_df.to_csv('json_df.csv', index=False)
        # json_df=pd.read_csv('json_df.csv')

        normal_tariff = json_df.loc[json_df["item_name"] == "Taxi Start, Normal Tariff"]
        normal_tariff_usd=normal_tariff['usd'].values[0]

        # normal_tariff_usd_dict = ast.literal_eval(normal_tariff_usd)
        taxi_avg_usd=normal_tariff_usd['avg']

        return taxi_avg_usd
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
    city_db = get_newitems(dataframe=city_db, df_column_name='id',
                           db_table_name='taxi', table_column='city_id',
                           config=config)

    api_city_df=pd.read_csv('api_city_df.csv')
    if type(api_city_df)!= str:
        city_wide_data = pd.merge(city_db, api_city_df, how='inner', left_on='name', right_on='city_name')
        city_wide_data = check_duplicated_cities(config, city_wide_data)
        city_wide_data['taxi_start_normal_tariff'] = city_wide_data.apply(get_taxi_price, axis=1)
        df_to_load = city_wide_data.loc[:, ['id', 'taxi_start_normal_tariff']]
        df_to_load = df_to_load.loc[df_to_load['taxi_start_normal_tariff'] != 'no_data_from_api', :]
        df_to_load = df_to_load.rename(columns={'id' : 'city_id'})
        engine = get_engine(config)
        add_dataframe_to_sqltable(dataframe=df_to_load,
                                  config=config,
                                  db_table_name='taxi',
                                  table_column='city_id',
                                  logging=logging)
        make_reference(config, "taxi", "city_id", "city", "id")


def check_duplicated_cities(config,city_wide_data):
    """
      Checks for duplicated cities in the city_wide_data DataFrame,
      removes duplicates based on the squared Euclidean distance
      between the city and its airport,
      and returns a new DataFrame with the city name, country name.

      Parameters:
      config (dict): A dictionary containing configuration parameters, including the path to the airports CSV file.
      city_wide_data (pandas.DataFrame):A Pandas DataFrame containing information about cities,
      including a unique city ID and a city code that corresponds to an airport code.

      Returns:
      pandas.DataFrame: A new DataFrame with the following columns: city_name, country_name, min_square_dist.
      """

    airport_sql = get_table_to_df(config, 'airport')
    city_wide_data_airports = pd.merge(city_wide_data, airport_sql, how='inner', left_on='id', right_on='city_id')
    airport_df = pd.read_csv(config['airports'])
    airport_df_extract= airport_df.loc[:,['code', 'location']]
    airport_df_extract['airport_lng']=airport_df_extract['location'].apply(lambda x: float(x.split()[1][1:]))
    airport_df_extract['airport_lat']=airport_df_extract['location'].apply(lambda x: float(x.split()[2][:-1]))
    city_wide_data_airports_locations = pd.merge(city_wide_data_airports,
                                                 airport_df_extract,
                                                 how='inner',
                                                 left_on='code',
                                                 right_on='code')
    city_wide_data_airports_locations = city_wide_data_airports_locations.rename(columns={'lat' : 'city_lat',
                                                                                          'lng' : 'city_lng',
                                                                                          'id_x' : 'id'})

    city_wide_data_airports_locations["min_square_dist"]=city_wide_data_airports_locations.apply(get_dist, axis=1)

    city_data=city_wide_data_airports_locations.loc[:,['id','city_name','country_name','min_square_dist']]

    city_data=city_data.sort_values('min_square_dist')
    city_data = city_data.drop_duplicates( subset=['city_name'])
    city_data = city_data.loc[:,['id','city_name','country_name']]
    return city_data




def get_dist(row):
    """
     Calculates the distance between an airport and a city using the coordinates of their longitudes and latitudes.

     Parameters:
     row (pandas.Series): A Pandas Series containing the following columns: airport_lng, city_lng, airport_lat, city_lat.

     Returns:
     float: The squared Euclidean distance between the airport and the city.
     """
    x1=row['airport_lng']
    x2=row['city_lng']
    y1=row['airport_lat']
    y2=row['city_lat']
    return (x1-x2)**2+(y1-y2)**2


