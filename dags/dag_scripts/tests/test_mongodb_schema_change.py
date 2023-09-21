"""Tests if the schema has changed from previous weeks
when moving data from MongoDB to PostgreSQL

Returns 0 if no change
Returns 1 if there is a change

Stops the data pipeline if the schema has changed
"""
# Standard Library Imports
import logging

# 3rd Party Imports
import pandas as pd


def test_if_schema_has_changed(flattened_json_df: pd.DataFrame):
    """Test if the schema has changed in the MongoDB data
    relative to what is currently in PostgreSQL.

    Parameters:
    1. flattened_json_df: Dataframe with data representing columns (keys)
        currently in MongoDB

    Returns:
    1. test_result. Returns True if schema change detected, and False if not.
    """
    actual_columns_set = set(flattened_json_df.columns)
    expected_columns_set = set(get_expected_postgres_columns_list())
    new_columns_flag = test_schema_change_if_new_columns(
        expected_columns_set, actual_columns_set
    )
    deprecated_columns_flag = test_schema_change_if_deprecated_columns(
        expected_columns_set, actual_columns_set
    )

    # Implicit boolean addition
    test_result = new_columns_flag + deprecated_columns_flag
    return test_result


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

    Parameters: To be defined

    Returns: To be defined
    """
    message_dict = {
        "deprecated_columns": "Schema change: old expected columns are no longer used in the schema",
        "new_columns": "Schema change: new unexpected columns were added to the schema",
    }

    message = message_dict[flag]
    logging.error(message)
