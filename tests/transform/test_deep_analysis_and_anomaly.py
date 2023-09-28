import pytest
import numpy as np
from transform.deep_analysis_and_anomaly import custom_convert_to_float, resample_data, merge_static_tables, transform_dataframe_deep_analysis
import pandas as pd 

def test_custom_convert_regular_number():
    assert custom_convert_to_float('123.45') == 123.45

def test_custom_convert_p7_number():
    assert custom_convert_to_float('22p7') == 22.7

def test_custom_convert_invalid_string():
    assert np.isnan(custom_convert_to_float('invalid'))

def test_resample_data_datetime_conversion():
    data = {
        'retard': ['1.5', '2.5'],
        'hor_theo': ['2022-01-01 12:00:00', 'not_a_datetime'],
        'annee': ['2022'] * 2,
        'month_name_fr': ['janvier'] * 2,
        'jour_semaine': ["mardi"] * 2,
        'debut_semaine': ["oui"] * 2,
        'ferie': ["non"] * 2,
        'date_du_jour': ["01/01"] * 2,
        'ligne': ["A"] * 2,
        'arret': [1, 2],
        'vehicule': ["bus"] * 2,
        'rs_sv_cours_a': [1] * 2,
    }
    df = pd.DataFrame(data)

    # This should raise an error due to invalid datetime
    with pytest.raises(Exception):
        resample_data(df)

 

def test_resample_data_type_retard_creation():
    data = {
        'retard': ['1.5', '16', '-11'],
        'hor_theo': ['2022-01-01 12:00:00', '2022-01-01 13:00:00', '2022-01-01 14:00:00'],
        'annee': ['2022'] * 3,
        'month_name_fr': ['janvier'] * 3,
        'jour_semaine': ["mardi"] * 3,
        'debut_semaine': ["oui"] * 3,
        'ferie': ["non"] * 3,
        'date_du_jour': ["01/01"] * 3,
        'ligne': ["A"] * 3,
        'arret': [1, 2, 3],
        'vehicule': ["bus"] * 3,
        'rs_sv_cours_a': [1] * 3,
    }
    df = pd.DataFrame(data)
    result_df = resample_data(df)

    # Check type_retard creation
    assert result_df.iloc[0]['type_retard'] == "pas d'anomalie"
    assert result_df.iloc[1]['type_retard'] == "tres en retard"
    assert result_df.iloc[2]['type_retard'] == "tres en avance"


def test_merge_static_tables_valid_data():
    # Mock data for the function
    ligne_data = {
        'ident': [1, 2],
        'libelle': ['L1', 'L2'],
        'vehicule': ['V1', 'V2']
    }
    ligne = pd.DataFrame(ligne_data)

 

    course_data = {
        'rs_sv_ligne_a': [1, 2],
        'gid': [10, 20]
    }
    course = pd.DataFrame(course_data)

 

    # Call the function
    result_df = merge_static_tables(ligne, course)

 

    # Check the merging and renaming of columns
    assert len(result_df) == 2
    assert 'id_ligne' in result_df.columns
    assert 'ligne' in result_df.columns
    assert result_df.iloc[0]['id_ligne'] == 1
    assert result_df.iloc[0]['ligne'] == 'L1'


def test_transform_dataframe_deep_analysis_valid_data():
    # Mock data for the function
    arret_data = {
        'gid_arret': [1, 2],
        'arret': ['StopA', 'StopB'],
        'vehicule': ['V1', 'V2'],
        'type_arret': ['TypeA', 'TypeB'],
        'voirie': ['RoadA', 'RoadB']
    }
    arret = pd.DataFrame(arret_data)

 

    ligne_data = {
        'ident': [1, 2],
        'libelle': ['L1', 'L2'],
        'vehicule': ['V1', 'V2']
    }
    ligne = pd.DataFrame(ligne_data)

 

    course_data = {
        'rs_sv_ligne_a': [1, 2],
        'gid': [10, 20]
    }
    course = pd.DataFrame(course_data)

 

    fusion_data = {
        'rs_sv_arret_p': [1, 2],
        'rs_sv_cours_a': [10, 20],
        'hor_theo': ['2022-01-01 12:00:00', '2022-01-01 13:00:00'],
        "etat": ["REALISE", "REALISE"],
        "gid": ["1", "2"],
        "hor_app": ['2022-01-01 12:00:00', '2022-01-01 13:00:00'],
        "hor_real": ['2022-01-01 12:16:00', '2022-01-01 12:40:00'],
        "mdate": ['2022-01-01 12:00:00', '2022-01-01 13:00:00'],
        "tempsarret": ['5', '5'],
    }
    fusion_df = pd.DataFrame(fusion_data)

 

    # Call the function
    aggregated_df, anomalies_df = transform_dataframe_deep_analysis(arret, ligne, course, fusion_df)

 

    # Check the data transformations and merging
    assert len(aggregated_df) == 2
    assert 'arret' in aggregated_df.columns
    assert 'ligne' in aggregated_df.columns
    assert 'vehicule' in aggregated_df.columns

 

    # Check the identification of anomalies
    assert len(anomalies_df) == 2
    assert anomalies_df.iloc[0]['retard'] == 16
    assert anomalies_df.iloc[1]['retard'] == -20