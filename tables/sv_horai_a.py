import dateutil
from api.api import get_data
from api.request_storage import RequestStorage
#from tables.sv_arret_p import load_sv_arret_p
from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta
import multiprocessing as mp

TABLE_NAME = "sv_horai_a"


def load_data(start_date = datetime.now(), step_size=12, max_days=30, use_request_storage=False):
    steps = int(1 * max_days * (24/step_size)) # 1 year * days * 2 (12 hours)
    request_storage = RequestStorage()
    result_list = []
    for step in tqdm(range(0,steps)):
        query_date = start_date + timedelta(hours=step_size * step)
        result = query_data(query_date)
        if use_request_storage:
            request_storage.add_request(result)
            if step < 3:
                continue
            data = request_storage.get_combined_dataframe()
            max_date = query_date - timedelta(hours=step_size * 3)
        else:
            data = result
            max_date = query_date
        min_date = max_date - timedelta(hours=step_size)
        min_date_string = min_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        max_date_string = max_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        data = data.query("hor_theo <= @max_date_string and hor_theo >= @min_date_string").drop_duplicates(subset=["hor_theo", "etat", "type", "source", "rs_sv_arret_p", "rs_sv_cours_a", "hor_real"])
        result_list.append(data)
    return pd.concat(result_list)


def query_data(tmp_date):
    queryparams = {
        "backintime": tmp_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    return get_data(TABLE_NAME, queryparams)

