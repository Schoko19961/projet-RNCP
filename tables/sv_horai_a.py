import dateutil
from api.api import get_data
from api.request_storage import RequestStorage
from tables.sv_arret_p import load_sv_arret_p
from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta
import multiprocessing as mp

TABLE_NAME = "sv_horai_a"


def load_data(start_date = datetime.now(), step_size=12, max_days=30):
    steps = int(1 * max_days * (24/step_size)) # 1 year * days * 2 (12 hours)
    request_storage = RequestStorage()

    for step in tqdm(range(0,steps)):
        query_date = start_date + timedelta(hours=step_size * step)
        max_date = query_date - timedelta(hours=step_size * 3)
        min_date = max_date - timedelta(hours=step_size)
        min_date_string = min_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        max_date_string = max_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        result = query_data(query_date)
        request_storage.add_request(result)
        if step < 3:
            continue
        data = request_storage.get_combined_dataframe()
        data = data.query("hor_theo <= @max_date_string and hor_theo >= @min_date_string").drop_duplicates(subset=["hor_theo", "etat", "type", "source", "rs_sv_arret_p", "rs_sv_cours_a", "hor_real"])
        data.to_csv(f"data/sv_horai_a/sv_horai_a_{max_date.strftime('%Y-%m-%dT%H:%M:%SZ')}_test.csv", index=False)


def query_data(tmp_date):
    # tmp_date = date - timedelta(hours=step_size * step)
    # min_date = date - timedelta(hours=step_size * step + step_size)
    
    queryparams = {
        "backintime": tmp_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        # "crs": "EPSG:4326",
        # "filter": {
        #     # "rs_sv_arret_p": {
        #     #     "$in": stop
        #     # },
        #     "$and": [
        #         {
        #             "hor_theo": {
        #                 "$lte": dateutil.parser.isoparse("2022-10-20T21:23:52Z").strftime("%Y-%m-%dT%H:%M:%S")
        #                 # "$lte": tmp_date.strftime("%Y-%m-%dT%H:%M:%S")
        #             }
        #         },
        #         {
        #             "hor_theo": {
        #                 "$gte": dateutil.parser.isoparse("2022-10-19T21:23:52Z").strftime("%Y-%m-%dT%H:%M:%S")
        #                 # "$gte": min_date.strftime("%Y-%m-%dT%H:%M:%S")
        #             }
        #         }
        #     ]
        # }
    }

    return get_data(TABLE_NAME, queryparams)

