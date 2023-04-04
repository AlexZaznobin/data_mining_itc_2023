import pymysql
import pandas as pd
from sqlalchemy import create_engine


def coursor_execution (cursor, query, logging) :
    try :
        cursor.execute(query)
        logging.info(f'executed:\n {query} ')
    except :
        logging.info(f'does not work:\n {query} ')
        pass
    return


def get_engine (config) :
    with open('/Users/alexanderzaznobin/Desktop/python/pwdfld/mysql.txt', 'r') as file :
        mysql_password = file.read()
    engine = create_engine(f"mysql+pymysql://root:{mysql_password}@localhost/{config['db_name']}")
    return engine


def get_mysql_cursor () :
    with open('/Users/alexanderzaznobin/Desktop/python/pwdfld/mysql.txt', 'r') as file :
        mysql_password = file.read()

    connection = pymysql.connect(
        host='localhost',
        user='root',
        password=mysql_password,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = connection.cursor()
    return cursor


def create_db (cursor, database_name, logging) :
    try :
        query = f"CREATE DATABASE {database_name};"
        coursor_execution(cursor, query, logging)
    except :
        query = F"USE {database_name};"
        coursor_execution(cursor, query, logging)


def set_up_db (db_name, logging) :
    cursor = get_mysql_cursor()
    create_db(cursor, db_name, logging)
    return cursor


def fill_airport_table (logging, config) :
    tickets_df = pd.read_csv(config['result_file'])
    start_city = tickets_df.loc[:, ['start_city_name', 'start_airport_code']]
    start_city.columns = ['city_name', 'airport_code']
    end_city = tickets_df.loc[:, ['end_city_name', 'end_airport_code']]
    end_city.columns = ['city_name', 'airport_code']
    all_cities = start_city.append(end_city).reset_index(drop=True)
    all_cities = all_cities.reset_index()
    all_cities.columns = ['city_id', 'city_name', 'airport_code']
    column_mapping = {'name' : 'fullname'}
    airport_df = pd.read_csv(config['airports'])
    airport_selected = airport_df.rename(columns=column_mapping).loc[:, ('code', 'fullname')]
    airport_selected = pd.merge(airport_selected, all_cities, left_on='code', right_on='airport_code')
    airport_selected = airport_selected.loc[:, ['code', 'fullname', 'city_id']]

    airport_selected.index.name = 'id'
    engine = get_engine(config)

    try :
        airport_selected.to_sql('airport', con=engine, if_exists='replace', index=True)
        engine
        logging.info(f" airport table in DB was created successfully")

    except :
        logging.warning(f" airport table in DB was NOT created successfully")
    return airport_selected



def fill_aircompany_table (logging, config) :
    tickets_df = pd.read_csv(config['result_file'])
    aircompany = pd.Series(tickets_df['aircompany_name'].unique()).to_frame()
    aircompany.columns = ['name']
    aircompany.index.name = 'id'
    engine = get_engine(config)
    try :
        aircompany.to_sql('aircompany', con=engine, if_exists='replace', index=True)
        logging.info(f" aircompany table in DB was created successfully")
    except :
        logging.warning(f" aircompany table in DB was NOT created successfully")
    pass
    return aircompany


def fill_city_table (logging, config) :
    tickets_df = pd.read_csv(config['result_file'])
    start_city_series = pd.Series(tickets_df['start_city_name'].unique())
    end_city_series = pd.Series(tickets_df['end_city_name'].unique())
    all_cities = start_city_series.append(end_city_series).to_frame().reset_index(drop=True)
    all_cities.columns = ['name']
    all_cities.index.name = 'id'

    engine = get_engine(config)
    try :
        all_cities.to_sql('city', con=engine, if_exists='replace', index=True)
        logging.info(f" city table in DB was created successfully")
    except :
        logging.warning(f" city table in DB was NOT created successfully")
    return all_cities


def fill_ticket_table (logging, config, airport_df, aircompany_df) :
    tickets_df = pd.read_csv(config['result_file'])
    airport_df = airport_df.reset_index()
    aircompany_df = aircompany_df.reset_index()
    tickets_df = pd.merge(tickets_df, airport_df, left_on='start_airport_code', right_on='code')
    tickets_df = tickets_df.loc[:,
                 ['id',
                  'price',
                  'flight_date_time',
                  'end_airport_code',
                  'aircompany_name',
                  'scraping_timestamp',
                  'duration_time',
                  'layovers']]
    tickets_df.columns = ['start_airport_id',
                          'price',
                          'flight_date_time',
                          'end_airport_code',
                          'aircompany_name',
                          'scraping_timestamp',
                          'duration_time',
                          'layovers']
    tickets_df = pd.merge(tickets_df, airport_df, left_on='end_airport_code', right_on='code')
    tickets_df = tickets_df.loc[:,
                 ['start_airport_id',
                  'id',
                  'price',
                  'flight_date_time',
                  'aircompany_name',
                  'scraping_timestamp',
                  'duration_time',
                  'layovers']]
    tickets_df.columns = ['start_airport_id',
                          'end_airport_id',
                          'price',
                          'flight_date_time',
                          'aircompany_name',
                          'scraping_timestamp',
                          'duration_time',
                          'layovers']

    tickets_df = pd.merge(tickets_df, aircompany_df, left_on='aircompany_name', right_on='name')
    tickets_df = tickets_df.loc[:,
                 ['start_airport_id',
                  'end_airport_id',
                  'id',
                  'price',
                  'flight_date_time',
                  'scraping_timestamp',
                  'duration_time',
                  'layovers']]
    tickets_df.columns =['start_airport_id',
                         'end_airport_id',
                         'aircompany_id',
                         'price',
                         'flight_date_time',
                         'scraping_timestamp',
                         'duration_time',
                         'layovers']
    tickets_df.index.name = 'id'
    engine = get_engine(config)

    try :
        tickets_df.to_sql('ticket', con=engine, if_exists='replace', index=True)
        logging.info(f" airport table in DB was created successfully")

    except :
        logging.warning(f" airport table in DB was NOT created successfully")

    return tickets_df

def make_references(logging,config):
    make_references_airport_city(logging, config)
    make_references_ticket(logging, config)
def make_references_airport_city (logging, config) :
    cursor = get_mysql_cursor()
    query = F"USE {config['db_name']};"
    cursor.execute(query)
    query = "ALTER TABLE airport ADD CONSTRAINT fk_city_id FOREIGN KEY (city_id) REFERENCES city(id);"
    try:
        cursor.execute(query)
    except:
        drop_query= "ALTER TABLE airport DROP FOREIGN KEY fk_city_id;"
        cursor.execute(drop_query)
        cursor.execute(query)

def make_references_ticket (logging, config) :
    cursor = get_mysql_cursor()
    query = F"USE {config['db_name']};"
    cursor.execute(query)
    try :
        drop_query_start = "ALTER TABLE airport DROP FOREIGN KEY fk_start_airport_id;"
        drop_query_end= "ALTER TABLE airport DROP FOREIGN KEY fk_end_airport_id;"
        drop_query_aircompany= "ALTER TABLE airport DROP FOREIGN KEY fk_aircompany_id;"
        cursor.execute(drop_query_start)
        cursor.execute(drop_query_end)
        cursor.execute(drop_query_aircompany)
    except :
        pass
    query = "ALTER TABLE ticket ADD CONSTRAINT fk_start_airport_id FOREIGN KEY (start_airport_id) REFERENCES airport(id);"
    cursor.execute(query)
    query = "ALTER TABLE ticket ADD CONSTRAINT fk_end_airport_id FOREIGN KEY (end_airport_id) REFERENCES airport(id);"
    cursor.execute(query)
    query = "ALTER TABLE ticket ADD CONSTRAINT fk_aircompany_id FOREIGN KEY (aircompany_id) REFERENCES aircompany(id);"
    cursor.execute(query)