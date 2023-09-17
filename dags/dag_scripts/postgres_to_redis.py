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
import sqlalchemy
from sqlalchemy import text

from dag_scripts.db_connections import db_connection_funcs


def main():
    """main"""
    postgres_engine = db_connection_funcs.create_postgresql_engine()
    sql_query_str = get_sql_query_current_pricing()
    sql_query_result = execute_sql_query(postgres_engine, sql_query_str)
    postgres_engine.dispose()
    redis_conn = db_connection_funcs.get_redis_connection()
    set_redis_value(redis_conn, sql_query_result)
    redis_conn.close()


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
    except sqlalchemy.exc.OperationalError as e:
        logging.error("Error with SQL query - please check your query string")
        raise ValueError from e
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
    except KeyError as e:
        logging.error(
            "Error with SQL query result - column does not exist in query result"
        )
        raise KeyError from e
    redis_connection.set("current_weekly_price_all_items", redis_sum_value_float)


if __name__ == "__main__":
    main()
