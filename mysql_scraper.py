import pymysql
import pandas as pd
from sqlalchemy import create_engine


def coursor_execution (cursor, query, logging) :
    """
    Executes a query using the provided cursor and logs the result.

    Args:
        cursor (pymysql.Cursor): The cursor for the MySQL connection.
        query (str): The SQL query to be executed.
        logging (logging.Logger): The logger for logging the execution result.

    Returns:
        None
    """
    try :
        cursor.execute(query)
        logging.info(f'executed:\n {query} ')
    except :
        logging.info(f'does not work:\n {query} ')
        pass
    return


def get_engine (config) :
    """
    Creates a connection engine to the MySQL database using the provided config.

    Args:
        config (dict): A dictionary containing the necessary configuration settings.

    Returns:
        sqlalchemy.engine.Engine: A connection engine to the MySQL database.
    """
    if config['mysql_pwd']!="":
        mysql_password=config['mysql_pwd']
    else:
        with open(config['mysql_pwd_file'], 'r') as file :
            mysql_password = file.read()
    engine = create_engine(f"mysql+pymysql://root:{mysql_password}@localhost/{config['db_name']}")
    return engine


def get_mysql_cursor () :
    """
      Creates a MySQL connection and returns a cursor to interact with the database.

      Returns:
          pymysql.cursors.DictCursor: A cursor for the MySQL connection.
      """
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
    """
       Creates a new MySQL database with the given name or uses the existing one.

       Args:
           cursor (pymysql.Cursor): The cursor for the MySQL connection.
           database_name (str): The name of the database to be created or used.
           logging (logging.Logger): The logger for logging the execution result.

       Returns:
           None
       """
    try :
        query = f"CREATE DATABASE {database_name};"
        coursor_execution(cursor, query, logging)
    except :
        query = F"USE {database_name};"
        coursor_execution(cursor, query, logging)


def set_up_db (db_name, logging) :
    """
      Sets up a new MySQL database with the given name or uses the existing one.

      Args:
          db_name (str): The name of the database to be set up or used.
          logging (logging.Logger): The logger for logging the execution result.

      Returns:
          pymysql.cursors.DictCursor: A cursor for the MySQL connection.
      """

    cursor = get_mysql_cursor()
    create_db(cursor, db_name, logging)
    return cursor


def fill_airport_table (logging, config, unique_cities, airport_cities_key) :
    """
      Creates and fills the 'airport' table in the MySQL database using the provided config.

      Args:
          logging (logging.Logger): The logger for logging the execution result.
          config (dict): A dictionary containing the necessary configuration settings.

      Returns:
          pd.DataFrame: The populated 'airport' table as a DataFrame.
      """
    airport_df = pd.read_csv(config['airports'])
    column_mapping = {'name' : 'fullname'}
    airport_selected = airport_df.rename(columns=column_mapping).loc[:, ('code', 'fullname')]
    airport_cities_key.columns = ['city_name', 'airport_code']
    airport_selected = pd.merge(airport_selected, airport_cities_key, left_on='code', right_on='airport_code')
    airport_selected = airport_selected.loc[:, ['code', 'fullname', 'city_name']]
    airport_selected = pd.merge(airport_selected, unique_cities, left_on='city_name', right_on='name')
    airport_selected = airport_selected.loc[:, ['code', 'fullname', 'id']]
    airport_selected.columns = ['code', 'fullname', 'city_id']

    airport_selected.index.name = 'id'
    engine = get_engine(config)

    try :
        airport_selected.to_sql('airport', con=engine, if_exists='replace', index=True)
        logging.info(f" airport table in DB was created successfully")

    except :
        logging.warning(f" airport table in DB was NOT created successfully")
    return airport_selected


def fill_aircompany_table (logging, config) :
    """
     Creates and fills the 'aircompany' table in the MySQL database using the provided config.

     Args:
         logging (logging.Logger): The logger for logging the execution result.
         config (dict): A dictionary containing the necessary configuration settings.

     Returns:
         pd.DataFrame: The populated 'aircompany' table as a DataFrame.
     """
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
    """
       Creates and fills the city table in the database with unique city names and their corresponding IDs.
       Filters out rows with missing or 'None' values in the name or airport_code columns.

       Args:
           logging (logging.Logger): A Logger object to log messages during the function execution.
           config (dict): A configuration dictionary containing settings and file paths.

       Returns:
           tuple: A tuple containing the following DataFrames:
               - unique_cities (pd.DataFrame): A DataFrame with unique city names and their corresponding IDs.
               - airport_cities_key (pd.DataFrame): A DataFrame with city names and their corresponding airport codes.
       """

    tickets_df = pd.read_csv(config['result_file'])
    start_city = tickets_df.loc[:, ['start_city_name', 'start_airport_code']]
    start_city.columns = ['name', 'airport_code']
    end_city = tickets_df.loc[:, ['end_city_name', 'end_airport_code']]
    end_city.columns = ['name', 'airport_code']
    all_cities = start_city.append(end_city)
    all_cities = all_cities.drop_duplicates()
    all_cities = all_cities.dropna(how='any')
    all_cities = all_cities[all_cities['name'] != 'None']
    all_cities = all_cities[all_cities['airport_code'] != 'None']
    unique_cities = all_cities.loc[:, ['name']].drop_duplicates()
    unique_cities = unique_cities.reset_index(drop=True)
    unique_cities.index.name = 'id'
    airport_cities_key = all_cities.loc[:, ['name', 'airport_code']]

    engine = get_engine(config)
    try :
        unique_cities.to_sql('city', con=engine, if_exists='replace', index=True, index_label='id')
        logging.info(f" city table in DB was created successfully")
    except :
        logging.warning(f" city table in DB was NOT created successfully")

    unique_cities = unique_cities.reset_index()
    return unique_cities, airport_cities_key


def fill_ticket_table (logging, config, airport_df, aircompany_df) :
    """
       Creates and fills the 'ticket' table in the MySQL database using the provided config and DataFrames.

       Args:
           logging (logging.Logger): The logger for logging the execution result.
           config (dict): A dictionary containing the necessary configuration settings.
           airport_df (pd.DataFrame): The populated 'airport' table as a DataFrame.
           aircompany_df (pd.DataFrame): The populated 'aircompany' table as a DataFrame.

       Returns:
           pd.DataFrame: The populated 'ticket' table as a DataFrame.
       """
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
    tickets_df.columns = ['start_airport_id',
                          'end_airport_id',
                          'aircompany_id',
                          'price',
                          'flight_date_time',
                          'scraping_timestamp',
                          'duration_time',
                          'layovers']
    tickets_df.index.name = 'id'
    engine = get_engine(config)
    tickets_df=tickets_df[tickets_df['flight_date_time']!='None']
    tickets_df['flight_date_time'] = pd.to_datetime(tickets_df['flight_date_time'])
    tickets_df['scraping_timestamp'] = pd.to_datetime(tickets_df['scraping_timestamp'])
    tickets_df['scraping_timestamp']=tickets_df['scraping_timestamp'].dt.round('S')

    try :
        tickets_df.to_sql('ticket', con=engine, if_exists='replace', index=True)
        logging.info(f" airport table in DB was created successfully")

    except :
        logging.warning(f" airport table in DB was NOT created successfully")

    return tickets_df


def make_references (logging, config) :
    """
    Calls functions to create foreign key references in the database tables.

    Args:
        logging (logging.Logger): A Logger object to log messages during the function execution.
        config (dict): A configuration dictionary containing settings and file paths.
    """
    make_references_airport_city(logging, config)
    make_references_ticket(logging, config)


def make_references_airport_city (logging, config) :
    """
    Creates a foreign key reference between the airport and city tables in the database.

    Args:
        logging (logging.Logger): A Logger object to log messages during the function execution.
        config (dict): A configuration dictionary containing settings and file paths.
    """
    cursor = get_mysql_cursor()
    query = F"USE {config['db_name']};"
    cursor.execute(query)
    try :
        query = "ALTER TABLE airport ADD CONSTRAINT fk_city_id FOREIGN KEY (city_id) REFERENCES city(id);"
        cursor.execute(query)
    except :
        drop_query_airport = "ALTER TABLE airport DROP FOREIGN KEY fk_city_id;"
        cursor.execute(drop_query_airport)
        query = "ALTER TABLE airport ADD CONSTRAINT fk_city_id FOREIGN KEY (city_id) REFERENCES city(id);"
        cursor.execute(query)


def make_references_ticket (logging, config) :
    """
       Creates foreign key references in the ticket table in the database.

       Args:
           logging (logging.Logger): A Logger object to log messages during the function execution.
           config (dict): A configuration dictionary containing settings and file paths.
       """

    cursor = get_mysql_cursor()
    query = F"USE {config['db_name']};"
    cursor.execute(query)
    query = "ALTER TABLE ticket ADD CONSTRAINT fk_start_airport_id FOREIGN KEY (start_airport_id) REFERENCES airport(id);"
    cursor.execute(query)
    query = "ALTER TABLE ticket ADD CONSTRAINT fk_end_airport_id FOREIGN KEY (end_airport_id) REFERENCES airport(id);"
    cursor.execute(query)
    query = "ALTER TABLE ticket ADD CONSTRAINT fk_aircompany_id FOREIGN KEY (aircompany_id) REFERENCES aircompany(id);"
    cursor.execute(query)


def save_results_in_database (config, logging) :
    """
     Sets up the database, drops existing tables if needed, fills the tables with data, and creates foreign key references.

     Args:
         config (dict): A configuration dictionary containing settings and file paths.
         logging (logging.Logger): A Logger object to log messages during the function execution.
     """
    cursor = set_up_db(config["db_name"], logging)
    drop_tables(config)
    unique_cities, airport_cities_key = fill_city_table(logging, config)
    airport_df = fill_airport_table(logging, config, unique_cities, airport_cities_key)
    aircompany_df = fill_aircompany_table(logging, config)
    ticket_df = fill_ticket_table(logging, config, airport_df, aircompany_df)
    make_references(logging, config)


def drop_tables (config) :
    """
    Drops the airport, city, aircompany, and ticket tables from the database if they exist.

    Args:
        config (dict): A configuration dictionary containing settings and file paths.
    """
    cursor = get_mysql_cursor()
    query = F"USE {config['db_name']};"
    cursor.execute(query)
    try :
        drop_city = "DROP TABLES city"
        drop_airport = "DROP TABLES airport"
        drop_aircompany = "DROP TABLES aircompany"
        drop_ticket = "DROP TABLES ticket"
        cursor.execute(drop_airport)
        cursor.execute(drop_city)
        cursor.execute(drop_aircompany)
        cursor.execute(drop_ticket)
    except :
        pass
