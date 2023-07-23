"""Connect to the local instance of MongoDB
and upload a test document
"""
import pymongo
import datetime


def main():
    print("running")
    client, db, collection = connect()
    test_json = get_test_json()
    collection.insert_one(test_json)

def connect(db_name:str="testing",coll_name:str="python_tests",connection_string= "mongodb://localhost:27017/"):
    """Connct to a mongo db database using the pymongo library

    Returns a (client, database, collection) tuple that
    can further be worked with to insert documents

    Parameters:
    1. db_name: database name in mongodb
    2. coll_name: collection to connect to
    3. connection_string: connection string for mongodb

    While the arguments remain static for this portfolo project I have included
    these function parameters rather than hardcoding the values in so the
    function will be more flexible and can be reused in a future project
    """

    client = pymongo.MongoClient(connection_string)
    database = client[db_name]
    collection = database[coll_name]

    return (client, database, collection)

def get_test_json():
    """Returns some test json that can be inserted into the mongodb"""
    post = {
    "author": "Billy",
    "text": "My first blog post!",
    "tags": ["mongodb", "python", "pymongo"],
    "date": datetime.datetime.now(tz=datetime.timezone.utc),
    }
    return post
    

if __name__ == "__main__":
    main()