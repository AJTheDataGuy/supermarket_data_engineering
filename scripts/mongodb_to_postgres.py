""" Script to load data From MongoDB into PostgreSQL
TO DO: add error logging

Program Flow:

1. Connect to MongoDB
2. Connect to PostgreSQL
2. Retrieve all records in the MongoDB collection
3. Flatten the JSON records into a flat record
4. Check if the specials columns are present
    If not present - add the specials columns
5. Define SQL data types for certain columns
6. Upload the data frame to PostgreSQL
"""

# 3rd Party Imports
import pymongo
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine


def main():
    """main"""
    mongodb_collection = connect_to_mongodb()
    json_data_list = mongodb_collection.find({})

    flattened_json_df = pd.json_normalize(json_data_list)
    flattened_json_df_w_specials = add_specials_columns_if_not_present(
        flattened_json_df
    )

    schema_has_changed_bool = test_if_schema_has_changed(flattened_json_df_w_specials)
    if schema_has_changed_bool:
        raise ValueError("Schema has changed from expected. Check the errors log.")

    cleaned_data_types_df = change_data_types_for_problem_columns(
        flattened_json_df_w_specials
    )
    sql_data_type_definitions_dict = define_column_data_types_for_sql()
    postgres_engine = create_postgresql_engine()
    cleaned_data_types_df.to_sql(
        name="raw_supermarket_staging",
        con=postgres_engine,
        schema="public",
        if_exists="replace",
        index=False,
        method="multi",
        dtype=sql_data_type_definitions_dict,
    )


def connect_to_mongodb(
    db_name: str = "testing",
    coll_name: str = "python_tests",
    connection_string="mongodb://localhost:27017/",
):
    """Connct to a mongo db database using the pymongo library

    Returns a MongoDB collection object that objects can be
    retrieved from

    In the future, the function can be adjusted to
    returns a (client, database, collection) tuple to
    work more directly with the database and client

    Parameters:
    1. db_name: database name in mongodb
    2. coll_name: collection to connect to
    3. connection_string: connection string for mongodb

    Returns:
    1. collection: connection to a specific MongoDB collection
    """

    client = pymongo.MongoClient(connection_string)
    database = client[db_name]
    collection = database[coll_name]

    return collection


def create_postgresql_engine(
    host="localhost", database="supermarket_data", user="postgres", password="postgres"
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

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{database}")
    return engine


def define_column_data_types_for_sql() -> dict:
    """This overall script uses the pd.to_sql function to insert data into PostgreSQL
    which infers column types unless otherwise defined

    Numeric columns should be defined as numeric before being inserted into Postgres
    Other columns are OK to be text.

    If mixed data is present - such as a decimal and a text value in the same column
    an error should be raised before being inserted so that can be corrected

    Dictionary is provided in pairs of {"column":"data type"}

    Text columns have been left out of this function for now. If space was a
    requirement in the database, the text column datatypes could be made smaller,
    for example going from text --> VARCHAR(25).

    Also in the future this function could be moved to a config file

    Parameters: None

    Returns:
    1. data_types_definitions_dict: dictionary of how certain (but not all)
    columns present in the MongoDB JSON data should be represented in the PostgreSQL data
    """
    sqlalchemy_int = sqlalchemy.types.INTEGER()
    sqlalchemy_bool = sqlalchemy.types.BOOLEAN()
    sqlalchemy_numeric = sqlalchemy.types.NUMERIC()

    data_type_definitions_dict = {
        "id": sqlalchemy_int,
        "date_extracted": sqlalchemy.types.DATE(),
        "featured": sqlalchemy_bool,
        "restrictions.retailLimit": sqlalchemy_int,
        "restrictions.promotionalLimit": sqlalchemy_int,
        "restrictions.liquorAgeRestrictionFlag": sqlalchemy_bool,
        "restrictions.tobaccoAgeRestrictionFlag": sqlalchemy_bool,
        "restrictions.restrictedByOrganisation": sqlalchemy_bool,
        "pricing.now": sqlalchemy_numeric,
        "pricing.was": sqlalchemy_numeric,
        "pricing.unit.quantity": sqlalchemy_int,
        "pricing.unit.ofMeasureQuantity": sqlalchemy_int,
        "pricing.unit.price": sqlalchemy_numeric,
        "pricing.unit.isWeighted": sqlalchemy_bool,
        "pricing.onlineSpecial": sqlalchemy_bool,
        "pricing.multiBuyPromotion.id": sqlalchemy_int,
        "pricing.multiBuyPromotion.minQuantity": sqlalchemy_int,
        "pricing.multiBuyPromotion.reward": sqlalchemy_numeric,
    }
    return data_type_definitions_dict


def add_specials_columns_if_not_present(
    raw_mongodb_table: pd.DataFrame,
) -> pd.DataFrame:
    """It is possible that the entire list of documents in the MongoDB
    collection do not contain specials. In this instance, the specials
    columns should be added as fully null columns

    Parameters:
    1. raw_mongodb_table: The flattened mongodb dataframe that may or may not contain the
        pricing specials columns

    Returns:
    1. raw_mongodb_table: MongoDB flattened data with specials columns added in the dataframe
    """
    expected_specials_columns_set = {
        "pricing.promotionType",
        "internalDescription",
        "pricing.specialType",
        "pricing.offerDescription",
        "pricing.multiBuyPromotion.type",
        "pricing.multiBuyPromotion.id",
        "pricing.multiBuyPromotion.minQuantity",
        "pricing.multiBuyPromotion.reward",
        "pricing.promotionDescription",
    }

    raw_mongodb_column_set = set(raw_mongodb_table.columns)
    specials_not_present_set = expected_specials_columns_set.difference(
        raw_mongodb_column_set
    )

    for special_column in specials_not_present_set:
        raw_mongodb_table[special_column] = ""
    return raw_mongodb_table


def test_if_schema_has_changed(raw_mongo_db_df: pd.DataFrame):
    """Check that the schema is as expected before inserting into PostgreSQL
    If the schema has changed that needs to be checked and manually revised
    as that will affect the downstream dbt models.

    For example, if a need half_off_specials key is added to the JSON, that should be
    reflected in the PostgreSQL table.

    There are types of changes to look for:

    1. New keys in the JSON not present in PostgreSQL (more serious)
    2. Old keys present in PostgreSQL that are no longer present in the JSON

    Parameters:
    1. list_of_expected_columns: All column names in the target PostgreSQL table
    2. raw_mongo_db_df: dataframe with data from MongoDB.

    Returns:
    1. True / False. Boolean True / False on whether the schema has changed.
        Returns True if there has been a change, false if not.
    """
    list_of_expected_postgres_columns = get_expected_postgres_columns_list()
    expected_columns_set = set(list_of_expected_postgres_columns)
    raw_mongo_columns_set = set(raw_mongo_db_df.columns)
    new_columns_bool = test_schema_change_if_new_columns(
        expected_columns_set, raw_mongo_columns_set
    )

    deprecated_columns_bool = test_schema_change_if_deprecated_columns(
        expected_columns_set, raw_mongo_columns_set
    )
    if new_columns_bool or deprecated_columns_bool:
        return True

    return False


def test_schema_change_if_new_columns(
    expected_columns_set: set, raw_mongo_columns_set: set
) -> bool:
    """Test to determine if new columns have been added to the raw
    JSON data in MongoDB. Return true if new columns are present.

    Parameters:
    1. Expected columns set: Set of expected columns in the target PostgreSQL table
    2. raw_mongo_columns_set: Set of flattened keys in the MongoDB JSON data

    Returns:
    1. Boolean True / False if new columns are present.
        True if new columns are present, False if not.
    """
    try:
        assert len(expected_columns_set.difference(raw_mongo_columns_set)) == 0
    except AssertionError:
        new_columns_flag = "new_columns"
        write_errors_file(new_columns_flag)
        return True

    return False


def test_schema_change_if_deprecated_columns(
    expected_columns_set: set, raw_mongo_columns_set: set
):
    """Test to determine if deprecated columns are no longer
    represented in the underlying JSON data

    Test is present to prevent the downstream dbt
    model from trying to grab columns that don't exist

    Parameters:
    1. Expected columns set: Set of expected columns in the target PostgreSQL table
    2. raw_mongo_columns_set: Set of flattened keys in the MongoDB JSON data

    Returns:
    1. True / False. Boolean True / False if deprecated columns are present.
        True if deprecated columns are present, False if not.
    """
    try:
        assert len(expected_columns_set.difference(raw_mongo_columns_set)) == 0
    except AssertionError:
        deprecated_columns_flag = "deprecated_columns"
        write_errors_file(deprecated_columns_flag)
        return True
    return False


def get_expected_postgres_columns_list() -> list:
    """Get the list of expected columns in the PostgreSQL raw schema
    This list is then called into the check_if_schema_has_changed() function
    to determine if the schema has changed

    Schema changes should be looked at manually before inserting into PostgreSQL
    as the changes will flow downstream into the dbt data models

    In the future this can be moved to a config file

    Parameters: None

    Returns:
    1. expected_columns_list: List of expected columns in the PostgreSQL target table
    for the MongoDB data
    """
    expected_columns_list = [
        "_id",
        "_type",
        "id",
        "adId",
        "adSource",
        "featured",
        "name",
        "brand",
        "description",
        "size",
        "availability",
        "availabilityType",
        "imageUris",
        "locations",
        "onlineHeirs",
        "date_extracted",
        "restrictions.retailLimit",
        "restrictions.promotionalLimit",
        "restrictions.liquorAgeRestrictionFlag",
        "restrictions.tobaccoAgeRestrictionFlag",
        "restrictions.restrictedByOrganisation",
        "restrictions.delivery",
        "merchandiseHeir.tradeProfitCentre",
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
        "internalDescription",
        "pricing.specialType",
        "pricing.offerDescription",
        "pricing.multiBuyPromotion.type",
        "pricing.multiBuyPromotion.id",
        "pricing.multiBuyPromotion.minQuantity",
        "pricing.multiBuyPromotion.reward",
        "pricing.promotionDescription",
    ]

    return expected_columns_list


def write_errors_file(flag: str):
    """Write errors to a log file if one or multiple of the following occurs:

    1. Schema change: new unexpected columns were added to the schema
    2. Schema change: old expected columns are no longer used in the schema
    3. Data type errors when inserting into PostgreSQL

    Parameters: To be defined

    Returns: To be defined
    """
    message = """Error logging not yet implemented

    In the meantime, one of the following has likely happened:

    1. Schema change: new unexpected columns were added to the schema
    2. Schema change: old expected columns are no longer used in the schema
    3. Data type errors when inserting into PostgreSQL
    """
    raise NotImplementedError(message)


def change_data_types_for_problem_columns(raw_dataframe: pd.DataFrame):
    """Changes the data types of problem columns
    so the overall dataframe can be ready correctly
    by the pandas to_sql function with sqlalchemy.

    For example, it seems that the MongoDB data type of ObjectID
    is retained when converting a dataframe and causes an error

    As another example, sqlalchemy tries to read some columns as dictionaries
    so they need to be explicitly converted to string before
    sqlalchemy will accept them and load the dataframe into PostgreSQL.

    Parameters:
    1. raw_dataframe: raw dataframe containing columns with problematic data types

    Returns:
    1. type_cleaned_dataframe: dataframe with problematic column data types converted
        explicitly to something that pandas and sqlalchemy can read and upload into PostgreSQL
    """
    type_cleaned_dataframe = raw_dataframe.astype(
        {
            "_id": str,
            "_type": str,
            "imageUris": str,
            "locations": str,
            "onlineHeirs": str,
            "restrictions.delivery": str,
        }
    )
    return type_cleaned_dataframe


if __name__ == "__main__":
    main()
