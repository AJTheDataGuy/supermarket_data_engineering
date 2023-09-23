"""Module to clear the mongodb staging collection
before grabbing the new week of data.
"""
# Custom modules
from dag_scripts.db_connections import db_connection_funcs


def main():
    """main"""
    mongodb_client = db_connection_funcs.get_mongodb_client()
    mongo_database = db_connection_funcs.get_mongodb_database(mongodb_client)
    mongodb_collection = db_connection_funcs.get_mongodb_collection(mongo_database)
    # Clean the mongodb staging area from last run
    mongodb_collection.delete_many({})
    mongodb_client.close()


if __name__ == "__main__":
    main()
