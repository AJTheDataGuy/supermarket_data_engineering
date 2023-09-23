"""Tests for the Postgres to Redis connection

These tests are intended to be run with the Pytest Library
"""
import sys
sys.path.append('/opt/airflow/dags')

from dag_scripts.postgres_to_redis import get_sql_query_current_pricing, execute_sql_query
from dag_scripts.db_connections import db_connection_funcs
import psycopg2

def test_sql_query_for_redis():
    """Test that the SQL query in PostgreSQL is valid SQL
    and won't cause an error
    """
    try:
        postgres_engine = db_connection_funcs.create_postgresql_engine()
        sql_query_str = get_sql_query_current_pricing()
        execute_sql_query(postgres_engine, sql_query_str)
        postgres_engine.dispose()
    except psycopg2.Error:
        assert False
