"""Tests for the Website to MongoDB script

Intended to be used with the PyTest library
"""
import sys
sys.path.append('/opt/airflow/dags')

import re
import requests
from dag_scripts import website_to_mongodb

def test_no_url_changes():
    """Tests that the base URL has not changed from expected"""
    session = requests.session()
    webpage_list = website_to_mongodb.create_paginated_webpages_list(max_page=1)
    response = session.get(webpage_list[0])
    assert response.status_code==200

def test_regex_validity():
    """Test that the regex to extract website JSON data is still valid"""
    session = requests.session()
    webpage_list = website_to_mongodb.create_paginated_webpages_list(max_page=1)
    json_regex = website_to_mongodb.compile_regex_for_json_extraction()
    response = session.get(webpage_list[0])
    raw_text = response.text
    found_json_text_list = re.findall(json_regex, raw_text)
    assert found_json_text_list != []
