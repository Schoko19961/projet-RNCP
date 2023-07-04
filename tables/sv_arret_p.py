from api.api import get_data

TABLE_NAME = "sv_arret_p"

def load_sv_arret_p():
    result = get_data(TABLE_NAME, {})
    return result
