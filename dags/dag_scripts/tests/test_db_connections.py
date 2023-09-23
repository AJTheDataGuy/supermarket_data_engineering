"""Tests to ensure database connections work as intended

These tests are intend to be run with the PyTest library
"""
import sys
sys.path.append('/opt/airflow/dags')
from dag_scripts.db_connections import db_connection_funcs
from dag_scripts import clear_mongodb_staging
from sqlalchemy.exc import SQLAlchemyError
from redis.exceptions import ConnectionError

def test_mongodb_server():
    """Test connection to the mongodb server"""
    mongo_client = db_connection_funcs.get_mongodb_client()
    assert mongo_client.server_info() is not None
    mongo_client.close()

def test_mongodb_database_exists():
    """Test that the mongodb database exists"""
    mongo_client = db_connection_funcs.get_mongodb_client()
    mongo_database = db_connection_funcs.get_mongodb_database(mongo_client)
    assert mongo_database.name in mongo_client.list_database_names()
    mongo_client.close()

def test_mongodb_collection_exists():
    """Test that the mongodb collection exists"""
    mongo_client = db_connection_funcs.get_mongodb_client()
    mongo_database = db_connection_funcs.get_mongodb_database(mongo_client)
    mongo_collection = db_connection_funcs.get_mongodb_collection(mongo_database)
    assert mongo_collection.name in mongo_database.list_collection_names()
    mongo_client.close()

def test_mongodb_insert():
    """Test that a document can be inserted into the mongodb database"""
    mongo_client = db_connection_funcs.get_mongodb_client()
    mongo_database = db_connection_funcs.get_mongodb_database(mongo_client)
    mongo_collection = db_connection_funcs.get_mongodb_collection(mongo_database)
    test_document = {"test_key": "test_value"}
    insert_result = mongo_collection.insert_one(test_document)
    assert insert_result.acknowledged
    clear_mongodb_staging.main()
    mongo_client.close()

def test_mongodb_retrieval():
    """Test that a document can be retrieved from the mongodb database"""
    mongo_client = db_connection_funcs.get_mongodb_client()
    mongo_database = db_connection_funcs.get_mongodb_database(mongo_client)
    mongo_collection = db_connection_funcs.get_mongodb_collection(mongo_database)
    test_document = {"test_key": "test_value"}
    insert_result = mongo_collection.insert_one(test_document)
    retrieved_document = mongo_collection.find_one({"test_key": "test_value"})
    assert retrieved_document is not None
    clear_mongodb_staging.main()
    mongo_client.close()

def test_postgres_connection():
    """Test that the user can connect to the PostgreSQL database"""
    postgresql_conn = db_connection_funcs.create_postgresql_engine()
    try:
        postgresql_conn.connect()
    except SQLAlchemyError:
        postgresql_conn.dispose()
        assert False
    postgresql_conn.dispose()

def test_redis_connection():
    """Test that the user can connect to the Redis database"""
    try:
        redis_conn = db_connection_funcs.get_redis_connection()
        redis_conn.ping()
    except (ConnectionError):
        assert False


    redis_conn.close()