import pymysql
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text


def coursor_execution (cursor, query, logging) :
    """
    Executes a query using the provided cursor and logs the result.

    Args:
        cursor (pymysql.Cursor): The cursor for the MySQL connection.
        query (str): The SQL query to be executed.
        logging (logging.Logger): The logger for logging the execution result.

    Returns:
        status (boolean)
    """
    try :
        cursor.execute(query)
        logging.info(f'executed:\n {query} ')
        return  True
    except :
        logging.info(f'does not work:\n {query} ')
        return  False


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
    query = f"CREATE DATABASE {database_name};"
    if not coursor_execution(cursor, query, logging):
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
          :param unique_cities:
          :param airport_cities_key:
      """
    airport_df = pd.read_csv(config['airports'])
    airport_cities_key.columns = ['city_name', 'airport_code']
    airport_selected = pd.merge(airport_df, airport_cities_key, left_on='code', right_on='airport_code')
    airport_selected = airport_selected.loc[:, ['code', 'name', 'city_name']]
    engine = get_engine(config)
    airport_selected = add_id_from_sql(dataframe=airport_selected,
                                       df_column='city_name',
                                       db_column='name',
                                       db_table_name='city',
                                       engine=engine)
    column_mapping = {'name_x' : 'name', 'id': 'city_id'}
    airport_selected = airport_selected.rename(columns=column_mapping)
    airport_selected= airport_selected.loc[:, ['code', 'name', 'city_id']]
    add_dataframe_to_sqltable(dataframe=airport_selected,
                              engine=engine,
                              db_table_name='airport',
                              table_column='name',
                              logging=logging)

    return airport_selected

def add_id_from_sql(dataframe, df_column, db_column,db_table_name,engine):
    inspector = inspect(engine)
    if inspector.has_table(db_table_name):
        sql_query = f"SELECT {db_column}, id FROM {db_table_name}"
        with engine.connect() as conn :
            query = conn.execute(text(sql_query))
        sql_df= pd.DataFrame(query.fetchall())
        return dataframe.merge(sql_df, how='inner', left_on=df_column, right_on=db_column)




def fill_aircompany_table (logging, config) :
    """
     Creates and fills the 'aircompany' table in the MySQL database using the provided config.

     Args:
         logging (logging.Logger): The logger for logging the execution result.
         config (dict): A dictionary containing the necessary configuration settings.

     Returns:
         pd.DataFrame: The populated 'aircompany' table as a DataFrame.
     """
    tickets_df = pd.read_csv(config['last_request_data'])
    aircompany = pd.Series(tickets_df['aircompany_name'].unique()).to_frame()
    aircompany.columns = ['name']
    aircompany = aircompany[aircompany['name'] != 'None']
    engine = get_engine(config)
    add_dataframe_to_sqltable(dataframe=aircompany,
                              engine=engine,
                              db_table_name='aircompany',
                              table_column='name',
                              logging=logging)

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

    tickets_df = pd.read_csv(config['last_request_data'])
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
    airport_cities_key = all_cities.loc[:, ['name', 'airport_code']]
    engine = get_engine(config)
    add_dataframe_to_sqltable(unique_cities,engine,'city','name',logging)
    unique_cities = unique_cities.reset_index()
    return unique_cities, airport_cities_key

def add_dataframe_to_sqltable(dataframe, engine, db_table_name, table_column, logging):
    """
       Adds a Pandas DataFrame to a SQL database table and adds an ID column with AUTO_INCREMENT property.
       Adds only those data that is not doubled in table_column and data frame column.

       Args:
           dataframe (pandas.DataFrame): The DataFrame containing the data to add to the table.
           engine (sqlalchemy.engine.base.Engine): The database engine object to use for the connection.
           db_table_name (str): The name of the database table to insert the data into.
           table_column (str): The name of the column in the database table to compare with the DataFrame.
           logging (logging.Logger): The logger object to use for logging.

       Returns:
           None

       Raises:
           None

       """
    if table_column!=False:
        dataframe=get_newitems(dataframe, engine,db_table_name,table_column)
    try :
        dataframe.to_sql(db_table_name, con=engine, if_exists='append', index=False)
        with engine.connect() as conn :
                conn.execute(text(f"ALTER TABLE {db_table_name} ADD id INT PRIMARY KEY AUTO_INCREMENT"))
        logging.info(f" {db_table_name} table in DB was created successfully")
    except :
        logging.warning(f" city table in DB was NOT created successfully")



def get_newitems(dataframe, engine, db_table_name, table_column ):
    """
    Returns a Pandas Series containing the elements in `series` that are not present in the specified SQL table column.

    Args:
        series (pandas.Series): The Pandas Series to compare with the SQL table column.
        engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine used to connect to the SQL database.
        db_table_name (str): The name of the database table containing the column to compare with.
        table_column (str): The name of the column in the database table to compare with.

    Returns:
        data frame with new items only
    """

    inspector = inspect(engine)
    if inspector.has_table(db_table_name):

        sql_query = f"SELECT {table_column} FROM {db_table_name}"
        with engine.connect() as conn :
            query = conn.execute(text(sql_query))
        set_df = set(dataframe[table_column].unique())
        set_sql = set(pd.DataFrame(query.fetchall()).squeeze().unique())
        new_items= set_df.difference(set_sql)
        dataframe=dataframe[dataframe[table_column].isin(new_items)]
    return dataframe




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
    tickets_df = pd.read_csv(config['last_request_data'])
    engine = get_engine(config)
    tickets_df = add_id_from_sql(dataframe=tickets_df, df_column='aircompany_name',
                                 db_column='name',db_table_name='aircompany',engine=engine)
    tickets_df = add_id_from_sql(dataframe=tickets_df, df_column='start_airport_code',
                                 db_column='code', db_table_name='airport',engine=engine)
    tickets_df = add_id_from_sql(dataframe=tickets_df, df_column='end_airport_code',
                                 db_column='code',db_table_name='airport',engine=engine)
    column_mapping = {'id_x': 'aircompany_id', 'id_y':'start_airport_id', 'id':'end_airport_id'}
    tickets_df = tickets_df.rename(columns=column_mapping)
    tickets_df =tickets_df.loc[:,['start_airport_id', 'end_airport_id', 'aircompany_id', 'price','flight_date_time',
                                  'scraping_timestamp','duration_time','layovers']]
    tickets_df=tickets_df[tickets_df['flight_date_time']!='None']
    tickets_df['flight_date_time'] = pd.to_datetime(tickets_df['flight_date_time'])
    tickets_df['scraping_timestamp'] = pd.to_datetime(tickets_df['scraping_timestamp'])
    tickets_df['scraping_timestamp']=tickets_df['scraping_timestamp'].dt.round('S')
    tickets_df['price']=tickets_df['price'].astype('int')
    add_dataframe_to_sqltable(tickets_df,engine,'ticket', False,logging)

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
        query = "ALTER TABLE airport MODIFY city_id INT;"
        cursor.execute(query)
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

    query = "ALTER TABLE ticket MODIFY start_airport_id INT;"
    cursor.execute(query)
    query = "ALTER TABLE ticket MODIFY end_airport_id INT;"
    cursor.execute(query)
    query = "ALTER TABLE ticket MODIFY aircompany_id INT;"
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
    unique_cities, airport_cities_key = fill_city_table(logging, config)
    airport_df = fill_airport_table(logging, config, unique_cities, airport_cities_key)
    aircompany_df = fill_aircompany_table(logging, config)
    ticket_df = fill_ticket_table(logging, config, airport_df, aircompany_df)
    make_references(logging, config)


