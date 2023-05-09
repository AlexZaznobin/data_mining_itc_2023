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
        return True
    except :
        logging.info(f'does not work:\n {query} ')
        return False


def get_engine (config) :
    """
    Creates a connection engine to the MySQL database using the provided config.

    Args:
        config (dict): A dictionary containing the necessary configuration settings.

    Returns:
        sqlalchemy.engine.Engine: A connection engine to the MySQL database.
    """
    mysql_password = get_mysql_pwd(config)
    host = get_host(config)
    user = get_user(config)
    db_name= get_dbname (config)
    engine = create_engine(f"mysql+pymysql://{user}:{mysql_password}@{host}/{db_name}")
    return engine


def get_dbname (config) :
    """
    Retrieve the host  from the given configuration.
    """
    db_name = "avia_scraper"
    if config['db_name'] != "" :
        db_name = config['db_name']
    return db_name


def get_host (config) :
    """
    Retrieve the host  from the given configuration.
    """
    host = "localhost"
    if config['mysql_host'] != "" :
        host = config['mysql_host']
    return host


def get_user (config) :
    """
    Retrieve the user  from the given configuration.
    """
    user = "root"
    if config['mysql_user'] != "" :
        user = config['mysql_user']
    return user


def get_mysql_pwd (config) :
    """
      Retrieve the MySQL password from the given configuration.

      This function first checks if the 'mysql_pwd' key in the config dictionary
      contains a non-empty string. If it does, the function returns that string.
      Otherwise, it reads the password from the file specified by the 'mysql_pwd_file' key
      in the config dictionary. If the password is still empty, a message is printed
      instructing the user to enter the password in the configuration file.

      Args:
          config (dict): A dictionary containing the configuration information, including
              the 'mysql_pwd' and 'mysql_pwd_file' keys.

      Returns:
          str: The MySQL password.

      Example:
          config = {
              'mysql_pwd': '',
              'mysql_pwd_file': 'path/to/mysql_password.txt'
          }
          password = get_mysql_pwd(config)

      """
    mysql_password = ""
    if config['mysql_pwd'] != "" :
        mysql_password = config['mysql_pwd']
    else :
        with open(config['mysql_pwd_file'], 'r') as file :
            mysql_password = file.read()
    if mysql_password == "" :
        print('please enter password in conf.json file to variable mysql_pwd')
    return mysql_password


def get_mysql_cursor (config) :
    """
      Creates a MySQL connection and returns a cursor to interact with the database.

      Returns:
          pymysql.cursors.DictCursor: A cursor for the MySQL connection.
      """
    mysql_password = get_mysql_pwd(config)
    host = get_host(config)
    user = get_user(config)
    connection = pymysql.connect(
        host=host,
        user=user,
        password=mysql_password,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    print()
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
    if not coursor_execution(cursor, query, logging) :
        query = F"USE {database_name};"
        coursor_execution(cursor, query, logging)


def set_up_db (config, logging) :
    """
      Sets up a new MySQL database with the given name or uses the existing one.

      Args:
          db_name (str): The name of the database to be set up or used.
          logging (logging.Logger): The logger for logging the execution result.

      Returns:
          pymysql.cursors.DictCursor: A cursor for the MySQL connection.
      """

    cursor = get_mysql_cursor(config)
    create_db(cursor, config['db_name'], logging)
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
    # print('airport_selected 1',airport_selected)
    airport_selected = add_id_from_sql(dataframe=airport_selected,
                                       df_column='city_name',
                                       db_column='name',
                                       db_table_name='city',
                                       engine=engine)
    column_mapping = {'name_x' : 'name', 'id' : 'city_id'}

    # print('airport_selected 2',airport_selected)
    airport_selected = airport_selected.rename(columns=column_mapping)
    airport_selected = airport_selected.loc[:, ['code', 'name', 'city_id']]
    add_dataframe_to_sqltable(dataframe=airport_selected,
                              config=config,
                              db_table_name='airport',
                              table_column='name',
                              logging=logging)

    return airport_selected


def add_id_from_sql (dataframe, df_column, db_column, db_table_name, engine) :
    """
      Args:
      dataframe (pandas.DataFrame): The DataFrame to which the ID column will be added.
      df_column (str): The name of the column in the DataFrame that contains the values to be mapped to IDs.
      db_column (str): The name of the column in the SQL table that contains the values to be mapped to IDs.
      db_table_name (str): The name of the SQL table that contains the mapping between values and IDs.
      engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object that connects to the database.

      Returns:
      pandas.DataFrame

      """
    inspector = inspect(engine)
    if inspector.has_table(db_table_name) :
        sql_query = f"SELECT {db_column}, id FROM {db_table_name}"
        with engine.connect() as conn :
            query = conn.execute(text(sql_query))
        sql_df = pd.DataFrame(query.fetchall())
        sql_df.columns = query.keys()
        # print('add_id_from_sql query.fetchall() \n',sql_df)
        # print('dataframe',dataframe)
        # print('df_column',df_column,'db_column', db_column)
        try:
            result=dataframe.merge(sql_df, how='inner', left_on=df_column, right_on=db_column)
        except:
            result=dataframe
        return result


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
                              config=config,
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
    # print()
    start_city = tickets_df.loc[:, ['start_city_name', 'start_airport_code']]
    start_city.columns = ['name', 'airport_code']
    end_city = tickets_df.loc[:, ['end_city_name', 'end_airport_code']]
    end_city.columns = ['name', 'airport_code']
    print('fill_city_table end_city\n',end_city)
    print('fill_city_table start_city\n',start_city)
    # all_cities = start_city.append(end_city)
    all_cities = pd.concat([start_city, end_city], axis=0, ignore_index=True)
    all_cities = all_cities.drop_duplicates()
    all_cities = all_cities.dropna(how='any')
    all_cities = all_cities[all_cities['name'] != 'None']
    all_cities = all_cities[all_cities['airport_code'] != 'None']
    unique_cities = all_cities.loc[:, ['name']].drop_duplicates()
    unique_cities = unique_cities.reset_index(drop=True)
    airport_cities_key = all_cities.loc[:, ['name', 'airport_code']]
    add_dataframe_to_sqltable(unique_cities, config, 'city', 'name', logging)
    unique_cities = unique_cities.reset_index()
    return unique_cities, airport_cities_key


def add_dataframe_to_sqltable (dataframe, config, db_table_name, table_column, logging) :
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
    engine = get_engine(config)
    if table_column != False :
        dataframe = get_newitems(dataframe, config, db_table_name, table_column)

    dataframe.to_sql(db_table_name, con=engine, if_exists='append', index=False)
    try :
        with engine.connect() as conn :
            conn.execute(text(f"ALTER TABLE {db_table_name} ADD id INT PRIMARY KEY AUTO_INCREMENT"))
        logging.info(f" {db_table_name} table in DB was created successfully")
    except :
        logging.info(f" {db_table_name} table in DB was not created")


def get_newitems (dataframe, config, db_table_name, table_column, df_column_name=None) :
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
    if df_column_name == None :  df_column_name = table_column
    engine = get_engine(config)
    inspector = inspect(engine)
    if inspector.has_table(db_table_name) and dataframe.shape[0] != 0 :
        set_df = set(dataframe[df_column_name].unique())
        sql_query = f"SELECT {table_column} FROM {db_table_name}"
        with engine.connect() as conn :
            query = conn.execute(text(sql_query))

        df_sql = pd.DataFrame(query.fetchall())
        set_sql=[]
        if df_sql.shape[0] > 1 : set_sql = set(df_sql.squeeze())

        if df_sql.shape[0] == 1 : set_sql = set(df_sql.loc[0, :])
        new_items = set_df.difference(set_sql)
        dataframe = dataframe[dataframe[df_column_name].isin(new_items)]
    return dataframe


def fill_ticket_table (logging, config) :
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
                                 db_column='name', db_table_name='aircompany', engine=engine)
    tickets_df = add_id_from_sql(dataframe=tickets_df, df_column='start_airport_code',
                                 db_column='code', db_table_name='airport', engine=engine)
    tickets_df = add_id_from_sql(dataframe=tickets_df, df_column='end_airport_code',
                                 db_column='code', db_table_name='airport', engine=engine)
    column_mapping = {'id_x' : 'aircompany_id', 'id_y' : 'start_airport_id', 'id' : 'end_airport_id'}
    tickets_df = tickets_df.rename(columns=column_mapping)
    tickets_df = tickets_df.loc[:, ['start_airport_id', 'end_airport_id', 'aircompany_id', 'price', 'flight_date_time',
                                    'scraping_timestamp', 'duration_time', 'layovers']]
    tickets_df = tickets_df[tickets_df['flight_date_time'] != 'None']
    tickets_df = tickets_df[tickets_df['price'] != 'None']
    tickets_df = tickets_df[tickets_df['layovers'] != 'None']
    tickets_df['flight_date_time'] = pd.to_datetime(tickets_df['flight_date_time'])
    tickets_df['scraping_timestamp'] = pd.to_datetime(tickets_df['scraping_timestamp'])
    tickets_df['scraping_timestamp'] = tickets_df['scraping_timestamp'].dt.round('S')
    tickets_df['price'] = tickets_df['price'].astype('int')
    tickets_df['layovers'] = tickets_df['layovers'].astype('int64')
    tickets_df = tickets_df.drop_duplicates(
        subset=tickets_df.columns[tickets_df.columns != 'scraping_timestamp'],
        keep='last')
    tickets_df_sql = get_table_to_df(config, 'ticket')

    if type(tickets_df_sql) != str :
        tickets_df_sql['layovers'] = tickets_df_sql['layovers'].astype('int64')
        new_tickets = get_new_items_multiple(tickets_df, tickets_df_sql, 'scraping_timestamp')
    else :
        new_tickets = tickets_df
    add_dataframe_to_sqltable(new_tickets, config, 'ticket', False, logging)
    return tickets_df


def get_new_items_multiple (last_req_df, sql_df, column_to_exclude) :
    """
            Get new items in a DataFrame by comparing it with another DataFrame based on specified columns.

            This function performs a left merge on the specified columns (excluding the column_to_exclude) and
            returns a new DataFrame containing the rows that exist only in the left DataFrame (last_req_df).

            Args:
                last_req_df (pd.DataFrame): The DataFrame to find new items in.
                sql_df (pd.DataFrame): The DataFrame to compare with.
                column_to_exclude (str): The column to exclude from the merge.

            Returns:
                pd.DataFrame: A new DataFrame containing the new items found in last_req_df.

            Raises:
                None: If an error occurs, the original DataFrame (last_req_df) is returned.
            """
    try :
        merge_columns = list(
            last_req_df.loc[:, last_req_df.columns[last_req_df.columns != column_to_exclude]].columns.values)
        new_df = pd.merge(last_req_df, sql_df,
                          on=merge_columns,
                          how='left', indicator=True)
        new_df = new_df[new_df['_merge'] == "left_only"].drop(['_merge'], axis=1)
        column_mapping = {'scraping_timestamp_x' : 'scraping_timestamp'}
        new_df = new_df.rename(columns=column_mapping)
        new_df = new_df.loc[:, new_df.columns.isin(last_req_df.columns.values)]
    except :
        new_df = last_req_df
    return new_df


def make_references (config) :
    """
    Calls functions to create foreign key references in the database tables.

    Args:
        logging (logging.Logger): A Logger object to log messages during the function execution.
        config (dict): A configuration dictionary containing settings and file paths.
    """
    make_references_airport_city(config)
    make_references_ticket(config)


def make_references_airport_city (config) :
    """
    Creates a foreign key reference between the airport and city tables in the database.

    Args:
        logging (logging.Logger): A Logger object to log messages during the function execution.
        config (dict): A configuration dictionary containing settings and file paths.
    """
    make_reference(config, "airport", "city_id", "city", "id")


def make_references_ticket (config) :
    """
       Creates foreign key references in the ticket table in the database.

       Args:
           logging (logging.Logger): A Logger object to log messages during the function execution.
           config (dict): A configuration dictionary containing settings and file paths.
       """

    make_reference(config, "ticket", "start_airport_id", "airport", "id")
    make_reference(config, "ticket", "end_airport_id", "airport", "id")
    make_reference(config, "ticket", "aircompany_id", "aircompany", "id")


def get_table_to_df (config, db_table_name) :
    engine = get_engine(config)
    inspector = inspect(engine)
    if inspector.has_table(db_table_name) :
        sql_query = f"SELECT * FROM {db_table_name}"
        with engine.connect() as conn :
            query = conn.execute(text(sql_query))
        dataframe = pd.DataFrame(query.fetchall())
        dataframe.columns = query.keys()
        return dataframe
    else :
        return 'no table exist'


def make_reference (config, table_fk, column_fk, table_pk, column_pk) :
    """
      Creates a foreign key constraint between a foreign key column in a table and a primary key column in another table.

      Parameters:
      config (dict): A dictionary containing configuration parameters, including the name of the database.
      table_fk (str): The name of the table containing the foreign key column.
      column_fk (str): The name of the foreign key column.
      table_pk (str): The name of the table containing the primary key column.
      column_pk (str): The name of the primary key column.

      Returns:
      None.
      """

    cursor = get_mysql_cursor(config)
    query = F"USE {config['db_name']};"
    cursor.execute(query)
    query_reference = f"ALTER TABLE {table_fk} " \
                      f"ADD CONSTRAINT fk_{table_fk}_{column_fk} " \
                      f"FOREIGN KEY ({column_fk}) " \
                      f"REFERENCES {table_pk}({column_pk});"
    query_set_type = f"ALTER TABLE {table_fk} MODIFY {column_fk} INT;"

    try :
        cursor.execute(query_set_type)
        cursor.execute(query_reference)
    except :
        drop_query_airport = f"ALTER TABLE {table_fk} " \
                             f"DROP FOREIGN KEY fk_{table_fk}_{column_fk};"
        cursor.execute(drop_query_airport)
        cursor.execute(query_set_type)
        cursor.execute(query_reference)


def save_results_in_database (config, logging) :
    """
     Sets up the database, drops existing tables if needed, fills the tables with data, and creates foreign key references.

     Args:
         config (dict): A configuration dictionary containing settings and file paths.
         logging (logging.Logger): A Logger object to log messages during the function execution.
     """
    set_up_db(config, logging)
    unique_cities, airport_cities_key = fill_city_table(logging, config)
    fill_airport_table(logging, config, unique_cities, airport_cities_key)
    fill_aircompany_table(logging, config)
    fill_ticket_table(logging, config)
    make_references(config)
