"""Connect to the local instance of MongoDB
and upload a test document
"""
import pymongo
import datetime
import re
from typing import Pattern


def main():
    print("running")
    client, db, collection = connect_to_mongodb()
    test_json = get_test_json()
    collection.insert_one(test_json)

def connect_to_mongodb(db_name:str="testing",coll_name:str="python_tests",connection_string= "mongodb://localhost:27017/"):
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

def extract_single_webpage_text(single_webpage_url:str)->str:
    """Retrieve text from a single web page
    on the Coles website using the request library

    Coles Website: https://www.coles.com.au/

    Example single page (as input parameter):
    https://www.coles.com.au/browse/fruit-vegetables?page=2


    Returns a mix of unwanted text and
    valuable json data. Valuable json includes:

    1. Item name
    2. Item cost
    3. Standard Item costs (usually per kg)
    etc.

    Returns the raw text for further processing

    Input Parameters:
    1. url: url for which the raw text will be retrieved
    In the context of the Coles website this may be fruit&veg page 3,
    meat page 5, etc. 

    Function Return:
    1. Raw text including unwanted text and valuable json data
    """
    data_from_request = requests.get(single_webpage_url)
    raw_text = data_from_request.text
    return raw_text

def compile_regex_for_initial_json_extraction()->Pattern[str]:
    """Compiles the regular expression for
    extraction out json items out of the raw website text
    for a single webpage

    Items on the page are defined in the JSON as follows:

    {'_type':'PRODUCT'...JSON_DATA_HERE...}}
    where the single quotes are actually double quotes
    and the '...JSON_DATA_HERE...' piece represents
    all the data stored such as item names, prices, etc.

    Requires non-greedy matching to retrieve all the items


    """
    json_regex = re.compile(r"""
                            \{\"_type\"\:\"PRODUCT\"
                            .*?
                            \}\}
                            """
                            ,re.I|re.X)
    return json_regex
    

if __name__ == "__main__":
    main()