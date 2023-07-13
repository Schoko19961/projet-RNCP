from api.api import get_data

TABLE_NAME = "sv_ligne_a"

def load_sv_ligne_a():
    result = get_data(TABLE_NAME, {})
    return result
