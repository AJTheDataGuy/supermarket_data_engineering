"""Placeholder"""

import requests
import re
from typing import Pattern

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