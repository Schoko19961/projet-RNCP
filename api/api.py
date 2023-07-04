import urllib
import json
import urllib3
import pandas as pd
BASE_URL = "https://data.bordeaux-metropole.fr/geojson/features"


## Create a function which sends an HTTP request to the API based on the table name and query params
def get_data(table, queryparams):
    http = urllib3.PoolManager(retries=urllib3.Retry(total=30,backoff_factor=0.2 , backoff_max=3))
    query_params = {
        "key": "4789EHLPRZ",
        **queryparams
    }

    query_params_encoded = urllib.parse.urlencode(query_params, quote_via=urllib.parse.quote).replace("%27", "%22")

    url = f"{BASE_URL}/{table}?{query_params_encoded}"
    try:
        response = http.request("GET",url)
        result_json = json.loads(response.data)
        result_processed = [entry["properties"] for entry in result_json["features"]]
        return pd.DataFrame(result_processed)
    except Exception as e:
        with open("error.json", "a") as f:
            json.dump(queryparams, f, indent=2)
        return pd.DataFrame()
