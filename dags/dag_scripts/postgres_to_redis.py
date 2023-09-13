"""Retrieves data from PostgreSQL
and stores it in Redis for caching

Currently uses the total price of all items
for the current week as an example piece of data
to store in redis
"""
# Standard Library
import logging

# 3rd Party Imports
import pandas as pd
import redis
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine, text


def main():
    """main"""
    redis_conn = get_redis_connection()
    postgres_connection = connect_to_postgresql()
    sql_query_str = get_sql_query_current_pricing()
    sql_query_result = execute_sql_query(postgres_connection, sql_query_str)
    set_redis_value(redis_conn, sql_query_result)


def get_redis_connection(host="redis_project_data", port=6380, decode_responses=True):
    """Connect to the redis database

    Input Parameters:
    1. Host
    2. Port
    3. Decode responses

    Returns
    1. Connection to the redis database
    """
    redis_connection = redis.Redis(
        host=host, port=port, decode_responses=decode_responses
    )
    return redis_connection


def connect_to_postgresql(
    host="postgres_project_data", database="supermarket_data", user="postgres", password="postgres",port="5433"
):
    """Creates a connection to the target PostgreSQL database

    Parameters:
    1. host: PostgreSQL host
    2. database: Database within PostgreSQL to connect to
    3. user: Database username
    4. password: Database password

    Returns:
    1. engine: sqlalchemy engine to use to connect to PostgreSQL
    """

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
    postgres_connection = engine.connect()
    return postgres_connection


def get_sql_query_current_pricing() -> str:
    """Defines a SQL query for PostgreSQL
    that returns the current week's price of
    purchasing all items

    Parameters: None

    Returns: PostgreSQL query as a string
    """

    raw_query = """SELECT
                SUM(price_aud)
                FROM 
                mart_pricing_over_time
                WHERE 
                pricing_date = 
                (SELECT 
 	                (MAX(pricing_date)) 
                FROM 
                mart_pricing_over_time)"""

    query = text(raw_query)

    return query


def execute_sql_query(postgres_connection, sql_query_str):
    """Execute the sql query and load it directly into pandas
    Raise an excepton if the query is not read properly
    """
    try:
        query_result = pd.read_sql(sql_query_str, postgres_connection)
    except sqlalchemy.exc.OperationalError:
        logging.error("Error with SQL query - please check your query string")
    return query_result


def set_redis_value(redis_connection, sql_query_result: pd.DataFrame):
    """Sets the price of all the current week's items
    in the redis database

    Parameters:
    1. redis_connection: connection to redis set with the
        get_redis_connection() function
    2. sql_query_result: sql query result as a single float
        from running the query in the get_sql_query_current_pricing()
        function against the postgresql database
    """
    try:
        redis_sum_value_float = sql_query_result["sum"][0]
    except KeyError:
        logging.error(
            "Error with SQL query result - column does not exist in query result"
        )
    redis_connection.set("current_weekly_price_all_items", redis_sum_value_float)


if __name__ == "__main__":
    main()
