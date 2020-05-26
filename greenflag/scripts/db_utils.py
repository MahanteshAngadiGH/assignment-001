import logging
from config import config
import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)

        status = {"Status": "Success"}

        return status

    except Error as e:
        print(e)


def load_data(conn, weather):
    """
    Loads the into the weather table
    :param conn:
    :param weather:
    :return: 
    """
    try:
        sql = ''' INSERT INTO projects(name,begin_date,end_date)
                  VALUES(?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, weather)
    
        status = {"Status": "Success"}
    
        return status

    except Error as e:
        print(e)


# The main() function
def weather_table_create_sql(logger):
    try:
        sql_create_weather_table = """ CREATE TABLE IF NOT EXISTS weather (
                                           ForecastSiteCode integer  NOT NULL,
                                           ObservationTime integer NOT NULL,
                                           ObservationDate text NOT NULL,
                                           WindDirection integer,
                                           WindSpeed integer,
                                           WindGust integer,
                                           Visibility integer,
                                           ScreenTemperature decimal(5,2),
                                           Pressure integer,
                                           SignificantWeatherCode integer,
                                           SiteName text,
                                           Latitude decimal(8,4),
                                           Longitude decimal(8,4),
                                           Region text,
                                           Country text
                                        ); """
    
        msg = "Weather Table Creation SQL Statement ... \n{}".format(sql_create_weather_table)
        print(msg)
        logger.error(msg)
    
        return sql_create_weather_table;

    except Error as e:
        print(e)
