"""Test to ensure all modules used are imported correctly

Tests are intended to be run with the PyTest library
"""
import sys
sys.path.append('/opt/airflow/dags')

def test_connections_import():
    """Test the db connections import works"""
    try:
        from dag_scripts.db_connections import db_connection_funcs
    except (ImportError, ModuleNotFoundError):
        assert False

def test_clear_mongo_import():
    """Test the mongodb staging clearing import works"""
    try:
        from dag_scripts import clear_mongodb_staging
    except (ImportError, ModuleNotFoundError):
        assert False

def test_web_to_mongo_import():
    """Test the web to mongo import works"""
    try:
        from dag_scripts import website_to_mongodb
    except (ImportError, ModuleNotFoundError):
        assert False

def test_mongo_to_postgres_import():
    """Test the web to mongo import works"""
    try:
        from dag_scripts import mongodb_to_postgres
    except (ImportError, ModuleNotFoundError):
        assert False

def test_postgres_to_redis_import():
    """Test the web to mongo import works"""
    try:
        from dag_scripts import mongodb_to_postgres
    except (ImportError, ModuleNotFoundError):
        assert False

def test_pandas_import():
    """Test the pandas import works"""
    try:
        import pandas
    except (ImportError, ModuleNotFoundError):
        assert False

def test_psycopg2_import():
    """Test the psycopg2 import works"""
    try:
        import pandas
    except (ImportError, ModuleNotFoundError):
        assert False

def test_sqlalchemy_import():
    """Test the sqlalchemy import works"""
    try:
        import sqlalchemy
    except (ImportError, ModuleNotFoundError):
        assert False

def test_redis_import():
    """Test the redis import works"""
    try:
        import redis
    except (ImportError, ModuleNotFoundError):
        assert False

def test_redis_import():
    """Test the redis import works"""
    try:
        import redis
    except (ImportError, ModuleNotFoundError):
        assert False

def test_pymongo_import():
    """Test the pymongo import works"""
    try:
        import pymongo
    except (ImportError, ModuleNotFoundError):
        assert False

def test_requests_import():
    """Test the requests import works"""
    try:
        import requests
    except (ImportError, ModuleNotFoundError):
        assert False
