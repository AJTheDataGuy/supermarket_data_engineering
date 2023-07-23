"""Load Data From MongoDB into PostgreSQL

TO DO: Move hardcoded column filters to config file
"""

import json
import psycopg2
import pymongo
import sys
import pandas as pd
from tkinter import filedialog
import os
from copy import copy


def main():

    client, db, coll = connect_to_mongodb()

    # Parse the JSON data
    # NOTE: json normalize function deletes raw json data after use
    # So a copy of the object is needed to get the smaller tables
    json_data = coll.find({})
    online_heirs = copy(json_data)
    aisles = copy(json_data)

    df = pd.json_normalize(json_data)
    df[["Standard Price","Standard Size"]] = df["pricing.comparable"].str.split(" per ",expand=True)
    df[["Standard Price"]] = df[["Standard Price"]].replace("[$,]", "", regex=True).astype(float)
    # NOTE: Think about final table normalisation before filtering
    #df = filter_main_table(df)
    df2 = pd.json_normalize(online_heirs,record_path=["onlineHeirs"],meta=["_id"])

    df3 = pd.json_normalize(aisles,record_path=["locations"],meta=["_id"])
    print(df3)
    full_table = pd.merge(pd.merge(df, df2, on='_id'), df3, on='_id')
    print(full_table)

    folder = filedialog.askdirectory(title="select directory")
    os.chdir(folder)
    full_table.to_csv(r"full_table.csv")

    #df[["Standard Price","Standard Size"]] = df["pricing.comparable"].str.split(" per ",expand=True)
    #df[["Standard Price"]] = df[["Standard Price"]].replace("[$,]", "", regex=True).astype(float)
    #print(df.dtypes)
    #print(df2)
    #folder = filedialog.askdirectory(title="select directory")
    #os.chdir(folder)
    #df.to_csv(r"normalised_json.csv")


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

def filter_main_table(table:pd.DataFrame):
    keep_columns = [
        "_id",
        "id",
        "featured",
        "name",
        "brand",
        "description",
        "size",
        "availability",
        "availabilityType",
        "locations",
        "onlineHeirs",
        "date_extracted",
        "merchandiseHeir.categoryGroup",
        "merchandiseHeir.category",
        "merchandiseHeir.subCategory",
        "merchandiseHeir.className",
        "pricing.now",
        "pricing.was",
        "pricing.unit.quantity",
        "pricing.unit.ofMeasureQuantity",
        "pricing.unit.ofMeasureUnits",
        "pricing.unit.price",
        "pricing.unit.ofMeasureType",
        "pricing.unit.isWeighted",
        "pricing.comparable",
        "pricing.onlineSpecial",
        "pricing.promotionType",
        "pricing.specialType",
        "pricing.offerDescription",
        "pricing.multiBuyPromotion.type",
        "pricing.multiBuyPromotion.id",
        "pricing.multiBuyPromotion.minQuantity",
        "pricing.multiBuyPromotion.reward",
        "pricing.promotionDescription",
        "Standard Price",
        "Standard Size"
    ]
    return table [keep_columns] 

if __name__ == "__main__":
    main()