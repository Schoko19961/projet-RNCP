import urllib
import json
import urllib3
from urllib3 import PoolManager
import pandas as pd
import os
BASE_URL = "https://data.bordeaux-metropole.fr/geojson/features"

def get_http_client()->PoolManager:
    """
    Returns the HTTP client. Adds proxy configuration if needed.
    """
    if "http_proxy" in os.environ:
        http = urllib3.ProxyManager(
            os.environ["http_proxy"],
        )
    else:
        http = urllib3.PoolManager()
    return http

def create_response_object(response_item):
    res = response_item["properties"]
    if "geometry" in response_item and response_item["geometry"] is not None and "coordinates" in response_item["geometry"]:
        coordinates = {"coordinates": response_item["geometry"]["coordinates"]}
        res.update(coordinates)
    return res

## Create a function which sends an HTTP request to the API based on the table name and query params
def get_data(table, queryparams):
    http = get_http_client()
    query_params = {
        "key": "4789EHLPRZ",
        **queryparams
    }

    query_params_encoded = urllib.parse.urlencode(query_params, quote_via=urllib.parse.quote).replace("%27", "%22")

    url = f"{BASE_URL}/{table}?{query_params_encoded}"
    try:
        response = http.request("GET",url=url,retries=urllib3.Retry(total=30,backoff_factor=0.2))
        result_json = json.loads(response.data)
        result_processed = [create_response_object(item) for item in result_json["features"]]
        return pd.DataFrame(result_processed)
    except Exception as e:
        print("Error while fetching data from API", table, query_params)
        print(e)
        with open("error.json", "a") as f:
            json.dump(queryparams, f, indent=2)
        return pd.DataFrame()
