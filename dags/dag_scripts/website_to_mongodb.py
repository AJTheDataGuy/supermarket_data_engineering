"""Retrieves data from the Coles supermarket website
and uploads it into a MongoDB database.

Current scope is to retrieve and upload data for all fruits and vegetables.

Program Flow:
1. Create a request session to get text data from the Coles website

2. Compile a regular expression to grab json data from raw website text

3. Create a variable to hold today's date. This is later appended to the
    JSON as metadata for when the data was extracted

4. Loop through the list of fruits and vegetables webpages
    For each web page:

        4A. Grab the raw text with a get request
        4B. Extract the JSON objects out of the raw text using
            regular expression. These raw JSON objects are stored as strings
        4C. Convert the list of json strings to a list of json objects
        4D. Upload the JSON data to the MongoDB database

TO DO: Add better error handling
"""

# Standard Library Imports
from datetime import date
import json
import logging
import re
from time import sleep
from typing import Pattern
import requests

# Custom modules
from dag_scripts.db_connections import db_connection_funcs

REQUEST_PADDING_TIME = 3


def main():
    """main"""
    session = requests.session()
    webpages_list = create_paginated_webpages_list()
    today_str = str(date.today())
    json_regex = compile_regex_for_json_extraction()
    mongodb_client = db_connection_funcs.get_mongodb_client()
    mongodb_collection = db_connection_funcs.get_mongodb_collection(mongodb_client)

    for webpage in webpages_list:
        sleep(REQUEST_PADDING_TIME)
        # Extract
        raw_webpage_text_str = extract_single_webpage_text(webpage, session)
        found_json_text_list = re.findall(json_regex, raw_webpage_text_str)

        # Quit looping through pagination if there is no JSON on the page
        if found_json_text_list == []:
            break

        # Transform
        list_of_json_objs = convert_json_as_strings_to_json_as_objs(
            found_json_text_list
        )
        list_of_json_objs_w_date = append_date_extracted_to_json(
            list_of_json_objs, today_str
        )

        # Load
        mongodb_collection.insert_many(list_of_json_objs_w_date)
    mongodb_client.close()


def extract_single_webpage_text(single_webpage_url: str, session) -> str:
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

    Parameters:
    1. single_webpage_url: url for which the raw text will be retrieved
    In the context of the Coles website this may be fruit&veg page 3,
    meat page 5, etc.
    2. session: requests session objectfor get requests

    Returns:
    1. raw_text_str: Raw text including unwanted text and valuable json data
    """
    try:
        data_from_request_str = session.get(single_webpage_url)
        raw_text_str = data_from_request_str.text
    except requests.exceptions.RequestException as e:
        logging.error("Error occured with get request. Please try again")
        raise requests.exceptions.RequestException from e
    return raw_text_str


def compile_regex_for_json_extraction() -> Pattern[str]:
    """Compiles the regular expression for
    extraction out json items out of the raw website text
    for a single webpage

    Items on the page are defined in the JSON as follows:

    {'_type':'PRODUCT'...JSON_DATA_HERE...}}
    where the single quotes are actually double quotes
    and the '...JSON_DATA_HERE...' piece represents
    all the data stored such as item names, prices, etc.

    Requires non-greedy matching to retrieve all the items

    NOTES ON SPECIALS: When grocery items are on special
    (for example 2 for $5.00 sorts of deals)
    The JSON data returned has a slightly different schema at the end
    It appears to require a 3rd ending bracket '}' above and beyond the normal 2

    Parameters: None

    Returns:
    1. json_regex: Compiled regular expression to further extract JSON data out
    of the raw website text
    """
    json_regex = re.compile(
        r"""
                            \{\"_type\"\:\"PRODUCT\"
                            .*?
                            \}\}\}?
                            """,
        re.I | re.X,
    )
    return json_regex


def convert_json_as_strings_to_json_as_objs(json_found_in_regex_list: list) -> list:
    """Purpose: Loops through a list of JSON as strings and converts them to actual JSON

    Parameters:
    1. json_found_in_regex_list:
        The list of json strings found by the regex function for a single webpage

    Returns:
    1. formatted_json_list: A list of JSON objects
    """
    formatted_json_list = []
    for json_string in json_found_in_regex_list:
        try:
            formatted_json_list.append(json.loads(json_string))
        except ValueError:
            logging.warning(
                "ERROR ON ITEM %s. Item will not be added to MongoDB.",
                json_string
            )
    return formatted_json_list


def append_date_extracted_to_json(list_of_json_objs: list, date_today: str):
    """Takes a list of JSON objects and adds additional meta data
    for when the data was retrieved from the Coles website

    Parameters:
    1. list_of_json_objs: List of JSON objects found in the website raw text
    2. date_today: today's date represented as a string. Added as a parameter
        rather than function variable in case this script is ran close to
        midnight

    Returns
    1. list_of_json_objs: list of JSON objects found in the raw website text
        with additional metadata on when that JSON object was extracted
    """
    for item in list_of_json_objs:
        item["date_extracted"] = date_today
    return list_of_json_objs


def create_paginated_webpages_list(
    base_url: str = "https://www.coles.com.au",
    additional_url: str = "/browse/fruit-vegetables",
    pagination_str="?page=",
    max_page: int = 30,
) -> list:
    """Creates a list of pages to run the program through
    based on the individual parts of a URL.

    Maximum page to get data from is defined as a parameter and initially set as 30

    Parameters:
    1. base_url: Base part of the website to connect to
    2. additional_url: Additional part of the website url that
        has paginated pages - such as fruit-vegetables, etc.
    3. pagination_str: part of the webpage url that includes how to
        get to the next page
    4. max_page: max page to loop up to. An additional pagination safeguard to ensure
    requests aren't continually sent to webpages that don't exit. Safeguard is in
    addition to looping through the webpages with a break statement
    when no json is returned

    Returns:
    1. paginated_webpages: Full list of urls that the get request
        will loop through and grab text from on the website
    """
    template_url_str = "".join([base_url, additional_url, pagination_str])

    str_pages = (str(page_number) for page_number in range(1, max_page + 1))

    paginated_webpages = ["".join([template_url_str, page]) for page in str_pages]

    return paginated_webpages


if __name__ == "__main__":
    main()
