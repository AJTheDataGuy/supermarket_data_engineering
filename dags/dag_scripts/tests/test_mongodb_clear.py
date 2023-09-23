"""Tests for the clear_mongodb_staging scripts"""
import sys
sys.path.append('/opt/airflow/dags')

from dag_scripts import clear_mongodb_staging
from dag_scripts.db_connections import db_connection_funcs

def test_mongo_db_cleared():
    """Tests that the mongodb staging collection is cleared properly"""
    mongo_client = db_connection_funcs.get_mongodb_client()
    mongo_database = db_connection_funcs.get_mongodb_database(mongo_client)
    mongo_collection = db_connection_funcs.get_mongodb_collection(mongo_database)

    test_json_document = {
    "first_name": "Bruce",
    "last_name": "Wayne"
    }

    mongo_collection.insert_one(test_json_document)
    clear_mongodb_staging.main()
    assert mongo_collection.count_documents({}) ==0
    mongo_client.close()
