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
# Standard Library Imports
import logging

# 3rd Party Imports
import pandas as pd
import sqlalchemy

# Custom modules
from dag_scripts.db_connections import db_connection_funcs
from dag_scripts.tests import test_mongodb_schema_change


def main():
    """main"""
    mongodb_client = db_connection_funcs.get_mongodb_client()
    mongodb_collection = db_connection_funcs.get_mongodb_collection(mongodb_client)
    json_data_list = mongodb_collection.find({})
    flattened_json_df = pd.json_normalize(json_data_list)
    mongodb_client.close()

    flattened_json_df_w_specials = add_specials_columns_if_not_present(
        flattened_json_df
    )
    schema_has_changed_bool = test_mongodb_schema_change.test_if_schema_has_changed(
        flattened_json_df_w_specials
    )
    if schema_has_changed_bool:
        raise ValueError("Schema has changed from expected. Check the errors log.")

    cleaned_data_types_df = change_data_types_for_problem_columns(
        flattened_json_df_w_specials
    )
    sql_data_type_definitions_dict = define_column_data_types_for_sql()
    postgres_engine = db_connection_funcs.create_postgresql_engine()
    cleaned_data_types_df.to_sql(
        name="raw_supermarket_staging",
        con=postgres_engine,
        schema="public",
        if_exists="replace",
        index=False,
        method="multi",
        dtype=sql_data_type_definitions_dict,
    )
    postgres_engine.dispose()


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
    try:
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
    except TypeError as e:
        logging.error("Error when converting types of problem dataframe columns")
        raise TypeError from e
    return type_cleaned_dataframe


if __name__ == "__main__":
    main()
