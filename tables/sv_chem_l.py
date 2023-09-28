from api.api import get_data

TABLE_NAME = "sv_chem_l"

def load_sv_chem_l(start_date = None):
    query_params = { }
    
    if start_date is not None:
        query_params["backintime"] = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    result = get_data(TABLE_NAME, query_params)
    return result
