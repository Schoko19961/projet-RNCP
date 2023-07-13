from api.api import get_data

TABLE_NAME = "sv_cours_a"

def load_sv_cours_a():
    result = get_data(TABLE_NAME, {})
    return result
