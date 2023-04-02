import pymysql

def get_mysql_cursor():

    with open('/Users/alexanderzaznobin/Desktop/python/pwdfld/mysql.txt', 'r') as file:
        mysql_password=file.read()

    connection = pymysql.connect(
        host='localhost',
        user='root',
        password=mysql_password,
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = connection.cursor()
    return cursor

def create_db(cursor, database_name):
    cursor.execute(f"CREATE DATABASE {database_name}")
    query = """
              CREATE TABLE ticket (
              id INT NOT NULL AUTO_INCREMENT,
              start_airport_id INT,
              flight_date_time DATETIME ,
              price INT,
              aircompany_id INT,
              scraping_timestamp DATETIME,
              layover_number INT,
              duration_time TIME
              PRIMARY KEY (id)
              );
    """
    cursor.execute(query)