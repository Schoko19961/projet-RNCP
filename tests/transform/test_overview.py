import pytest
import pandas as pd
from transform.overview import resample_data, groupby_aggregated, transform_dataframe
import numpy as np 

def test_resample_data_datetime_conversion():
    data = {
        'retard': ['1.5', '2.5'],
        'hor_theo': ['2022-01-01 12:00:00', 'not_a_datetime'],
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
        'libelle': ["Bus A"]* 3,
        'vehicule': ["Bus"] * 3,
        'date_du_jour': ['01/01'] * 3
    }
    df = pd.DataFrame(data)
    result_df = resample_data(df)

    # Check type_retard creation
    assert result_df.iloc[0]['retard'] == np.mean([float(x) for x in df["retard"]])

def test_resample_data_overview_valid_data():
    data = {
        'retard': ['1.5', '2.5', '3.5'],
        'hor_theo': ['2022-01-01 12:00:00', '2022-01-01 13:00:00', '2022-01-02 12:00:00'],
        'libelle': ['LibA', 'LibA', 'LibB'],
        'vehicule': ['V1', 'V1', 'V2'],
        'annee': [2022, 2022, 2022],
        'month_name_fr': ['janvier', 'janvier', 'janvier'],
        'jour_semaine': [6, 6, 7],
        'debut_semaine': ['01', '01', '02'],
        'ferie': ['non', 'non', 'non'],
        'date_du_jour': ['01/01', '01/01', '02/01'],
    }
    df = pd.DataFrame(data)
    result_df = resample_data(df)

    # Check that the aggregation was done correctly
    assert len(result_df) == 2
    assert result_df.iloc[0]['retard'] == 2.0  # Average of 1.5 and 2.5
    assert result_df.iloc[1]['retard'] == 3.5
    assert result_df.iloc[1]['libelle'] == "LibB"

def test_groupby_aggregated_valid_data():
    data = {
        'retard': [1.5, 2.5, 3.5, 4.5],
        'libelle': ['LibA', 'LibA', 'LibB', 'LibB'],
        'annee': [2022, 2022, 2022, 2023],
        'month_name_fr': ['janvier', 'février', 'janvier', 'janvier'],
        'vehicule': ['V1', 'V2', 'V3', 'V4'],
        'date_du_jour': ['01/01', '01/01', '02/01', '02/01'],
        'jour_semaine': [6, 6, 7, 7],
        'debut_semaine': ['01', '01', '02', '02'],
        'ferie': ['non', 'non', 'non', 'non'],
    }
    df = pd.DataFrame(data)
    result_df = groupby_aggregated(df)

    # Check that the aggregation was done correctly
    assert len(result_df) == 4
    assert result_df.iloc[0]['retard'] == 1
    assert result_df.iloc[1]['retard'] == 2
    assert result_df.iloc[2]['retard'] == 3
    assert result_df.iloc[2]['annee'] == 2022
    assert result_df.iloc[2]['libelle'] == "LibB"
    assert result_df.iloc[2]['month_name_fr'] == "janvier"

def test_transform_dataframe_valid_data():
    # Mock data for the function
    fusion_data = {
        'retard': ['1.5', '2.5'],
        'etat': ['REALISE', 'REALISE'],
        'hor_theo': ['2022-01-01 12:00:00', '2022-01-01 13:00:00'],
        'hor_real': ['2022-01-01 12:05:00', '2022-01-01 13:10:00'],
        'mdate': ['2022-01-01 12:10:00', '2022-01-01 13:15:00'],
        'gid': [1, 2],
        'hor_app': ['2022-01-01 12:00:00', '2022-01-01 13:00:00'],
        'rs_sv_arret_p': [10, 20],
        'tempsarret': ["3", "4"],
        "libelle": ["Tram A", "Tram B"],
        "vehicule": ["Tram", "Tram"],
    }
    fusion_df = pd.DataFrame(fusion_data)

 

    arret_data = {
        'gid': [10, 20],
        'arret_name': ['StopA', 'StopB']
    }
    arret = pd.DataFrame(arret_data)

 

    # Call the function
    result_df = transform_dataframe(arret, fusion_df)

 

    # Check data filtering and transformations
    assert len(result_df) == 2
    assert 'gid' not in result_df.columns
    assert 'hor_app' not in result_df.columns
    assert 'annee' in result_df.columns
    assert result_df.iloc[0]['annee'] == 2022
