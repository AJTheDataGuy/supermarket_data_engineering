"""Tests for the MongoDB to Postgres script

Tests are intended to be ran with the PyTest librarys
"""
import sys
sys.path.append('/opt/airflow/dags')

from dag_scripts import mongodb_to_postgres
from dag_scripts.db_connections import db_connection_funcs
import pandas as pd

def test_columns_exist():
    """Test that the columns exist when data types are specified
    for PostgreSQL columns before export
    """
    postgres_engine = db_connection_funcs.create_postgresql_engine()
    data_types_dict = mongodb_to_postgres.define_column_data_types_for_sql()
    expected_columns_set = set(data_types_dict.keys())
    actual_dataframe = pd.read_sql("SELECT * FROM raw_supermarket_staging",
                                   postgres_engine)
    actual_columns_set = set(actual_dataframe.columns)
    assert len(expected_columns_set.difference(actual_columns_set))==0
    postgres_engine.dispose()

