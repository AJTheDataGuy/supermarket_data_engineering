"""Module that contains functions to connect
to the different databases used throughout the project

This module was split out from the original scripts
as the same database connection strings are reused
in multiple scripts

Default connection strings are for this specific
supermarket data project but are parameterised for
easy use in future projects as well.
"""

# 3rd Party Imports
import pymongo
import redis
import psycopg2
from sqlalchemy import create_engine


def get_mongodb_client(
    host: str = "mongodb", port: str = "27017",user="root",pw="root"
):
    """Connct to a mongo db database using the pymongo library

    Returns a MongoDB client object that can further connect
    to a database. Mongodb client creation and linking to the database /
    collection are in seperate functions so the client can be closed directly
    after use with the client.close() function.

    Parameters:
    1. host: host name in mongodb
    2. port: port for MongoDB to connect to

    Returns:
    1. client: connection to a specific MongoDB collection
    """
    connection_string = (f"mongodb://{user}:{pw}@{host}:{port}/",)
    client = pymongo.MongoClient(connection_string)

    return client

def get_mongodb_database(
    mongodb_client,
    db_name: str = "supermarket_data",
):
    """Connect to a mongo database using a mongodb
    client object. Mongodb client creation and linking to the 
    database and collection
    are in seperate functions so the client can be closed directly
    after use with the client.close() function. As well, it is easier
    to test the connections to the database and collection seperately.

    Parameters:
    1. mongodb_client_connection: mongodb client object. Includes
        host and port number
    2. db_name: database name in mongodb

    Returns:
    1. database: connection to a specific MongoDB database
    """
    database = mongodb_client[db_name]

    return database

def get_mongodb_collection(
    database_conn,
    coll_name: str = "supermarket_json",
):
    """Connect to a mongo database using a mongodb
    client object. Mongodb client creation and linking to the 
    database and collection
    are in seperate functions so the client can be closed directly
    after use with the client.close() function. As well, it is easier
    to test the connections to the database and collection seperately.

    Parameters:
    1. mongodb_client_connection: mongodb client object. Includes
        host and port number
    2. db_name: database name in mongodb
    3. coll_name: collection name in mongodb

    Returns:
    1. collection: connection to a specific MongoDB collection
    """
    collection = database_conn[coll_name]

    return collection


def create_postgresql_engine(
    host="postgres_project_data",
    database="supermarket_data",
    user="postgres",
    password="postgres",
    port="5432",
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

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )
    return engine


def get_redis_connection(host="redis_project_data", port=6379, decode_responses=True):
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
