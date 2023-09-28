import pytest
import pandas as pd

@pytest.fixture
def sample_data_merge_static_tables():
    ligne_data = pd.DataFrame({
        'gid': ['10', '20', '30'],
        'ident': [1, 2, 3],
        'libelle': ['L1', 'L2', 'L3'],
        'vehicule': ['V1', 'V2', 'V3']
    })

    course_data = pd.DataFrame({
        'gid': [4,5,6],
        'rs_sv_ligne_a': [1, 2, 3],
        'rs_sv_chem_l': [101, 102, 103]
        # Add more columns as needed
    })

    chemin_data = pd.DataFrame({
        'gid': [101, 102, 103],
        'libelle': ['C1', 'C2', 'C3'],
        'sens': ['S1', 'S2', 'S3']
    })

    return ligne_data, course_data, chemin_data

@pytest.fixture
def sample_data_date_format():
    data = {
        "hor_theo": ["2023-09-18 10:30:00", "2023-09-18 11:45:00", "2023-09-18 12:00:00"],
        "hor_real": ["2023-09-18 10:31:00", "2023-09-18 11:50:00", "2023-09-18 12:00:00"],
        "mdate": ["2023-09-18", "2023-09-19", "2023-09-18 12:00:00"]
    }
    return pd.DataFrame(data)

@pytest.fixture
def chemin():
    data = {
        'gid': ["4","5","6"],
        'libelle': ['C1', 'C2', 'C3'],
        'sens': ['S1', 'S2', 'S3']
    }
    return pd.DataFrame(data)

@pytest.fixture
def arret():
    return pd.DataFrame({
        "gid": ["1", "2", "3"],
        "arret": ["Arret 1", "Arret 2", "Arret 3"],
        "vehicule": ["Bus", "Metro", "Tram"],
        "type_arret": ["A", "B", "C"],
        "voirie": ["Rue 1", "Rue 2", "Rue 3"]
    })

@pytest.fixture
def ligne():
    return pd.DataFrame({
        "gid": ["1","2", "3"],
        "ligne": ["Ligne 1", "Ligne 2", "Ligne 3"],
        "ident": ["1","2","3"],
        "type_ligne": ["A", "B", "C"],
        "vehicule": ["TRAM", "BUS", "BUS"],
    })

@pytest.fixture
def course():
    return pd.DataFrame({
        "gid": ["1", "2", "3"],
        "rs_sv_chem_l": [4,5,6],
        "rs_sv_ligne_a": [1,2,3],
    })

@pytest.fixture
def batch_df():
    return pd.DataFrame({
        "rs_sv_arret_p": [1, 2, 3],
        "rs_sv_cours_a": [1, 2, 3],
        "etat": ["REALISE", "NON_REALISE", "REALISE"],
        "hor_theo": ["2022-01-01 10:00:00", "2022-01-01 11:00:00", "2022-01-01 12:00:00"],
        "hor_real": ["2022-01-01 10:05:00", "2022-01-01 11:10:00", "2022-01-01 12:15:00"],
        "mdate": ["2022-01-01", "2022-01-01", "2022-01-01"],
        "tempsarret": ["5", "10", "15"],
        "gid": ["1", "2", "3"],
        "hor_app": ["2022-01-01 10:05:00", "2022-01-01 11:10:00", "2022-01-01 12:15:00"]
    })