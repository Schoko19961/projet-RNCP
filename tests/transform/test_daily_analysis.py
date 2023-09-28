from transform.daily_analysis import custom_convert_to_float, get_month_name_fr, merge_static_tables, date_format, transform_daily_data, transform_non_realise_daily_data
import math
import pytest 
import pandas as pd 


def test_valid_float_conversion():
    assert custom_convert_to_float("3.14") == 3.14
    assert custom_convert_to_float("0.0") == 0.0
    assert custom_convert_to_float("-5.2") == -5.2
    assert custom_convert_to_float(42) == 42.0
    assert custom_convert_to_float(0) == 0.0

def test_invalid_value_conversion():
    assert math.isnan(custom_convert_to_float("not_a_float"))
    assert math.isnan(custom_convert_to_float(None))

def test_special_case_conversion():
    assert custom_convert_to_float("22p7") == 22.7
    assert custom_convert_to_float("42p7") == 42.7

@pytest.mark.parametrize("month_number, expected_name", [
    (1, "janvier"),
    (2, "février"),
    (3, "mars"),
    (4, "avril"),
    (5, "mai"),
    (6, "juin"),
    (7, "juillet"),
    (8, "août"),
    (9, "septembre"),
    (10, "octobre"),
    (11, "novembre"),
    (12, "décembre"),
])
def test_valid_month_names(month_number, expected_name):
    assert get_month_name_fr(month_number) == expected_name

@pytest.mark.parametrize("invalid_month", [0, 13, "invalid"])
def test_invalid_month_names(invalid_month):
    assert get_month_name_fr(invalid_month) == ""

def test_merge_static_tables(sample_data_merge_static_tables):
    ligne, course, chemin = sample_data_merge_static_tables

    result_df = merge_static_tables(ligne, course, chemin)

    # Add your assertions to check if the function behaves as expected
    assert isinstance(result_df, pd.DataFrame)
    assert set(result_df.columns) == {'gid_course', 'id_ligne', 'ligne', 'sens', 'chemin'}
    assert len(result_df) == len(course)

def test_date_format(sample_data_date_format):
    input_df = sample_data_date_format.copy()  # Make a copy to avoid modifying the original data

    result_df = date_format(input_df)

    # Check if the columns have been converted to datetime objects
    assert all(result_df["hor_theo"].apply(lambda x: isinstance(x, pd.Timestamp)))
    assert all(result_df["hor_real"].apply(lambda x: isinstance(x, pd.Timestamp)))
    assert all(result_df["mdate"].apply(lambda x: isinstance(x, pd.Timestamp)))

def test_transform_daily_data(arret, ligne, course,chemin, batch_df):
    non_realise_df, transformed_df = transform_daily_data(arret, ligne, course, chemin, batch_df)
    
    # Check that non_realise_df has the expected columns
    assert set(non_realise_df.columns) == set(["rs_sv_arret_p", "rs_sv_cours_a", "etat", "hor_theo", "hor_real", "mdate", "tempsarret", "gid", "hor_app", "annee", "mois", "date_du_jour", "arret", "ferie", "type_arret", "month_name_fr", "vehicule", "gid_arret", "jour_semaine", "voirie", "debut_semaine"])
    
    # Check that transformed_df has the expected columns
    assert set(transformed_df.columns) == set(["rs_sv_arret_p", "rs_sv_cours_a", "etat", "hor_theo", "hor_real", "mdate", "tempsarret", "annee", "mois", "jour_semaine", "month_name_fr", "date_du_jour", "debut_semaine", "ferie", "retard", "type_retard", "gid_arret", "arret", "vehicule", "type_arret", "voirie", "gid", "hor_app"])
    
    # Check that transformed_df has the expected number of rows
    assert len(transformed_df) == 3
    
    # Check that the "gid_arret" column has the expected values
    assert transformed_df["gid_arret"].tolist() == [1, 2, 3]
    
    # Check that the "arret" column has the expected values
    assert transformed_df["arret"].tolist() == ["Arret 1", "Arret 2", "Arret 3"]
    
    # Check that the "vehicule" column has the expected values
    assert transformed_df["vehicule"].tolist() == ["Bus", "Metro", "Tram"]
    
    # Check that the "type_arret" column has the expected values
    assert transformed_df["type_arret"].tolist() == ["A", "B", "C"]
    
    # Check that the "voirie" column has the expected values
    assert transformed_df["voirie"].tolist() == ["Rue 1", "Rue 2", "Rue 3"]
    
    # Check that the "annee" column has the expected values
    assert transformed_df["annee"].tolist() == [2022, 2022, 2022]
    
    # Check that the "mois" column has the expected values
    assert transformed_df["mois"].tolist() == [1, 1, 1]
    
    # Check that the "jour_semaine" column has the expected values
    assert transformed_df["jour_semaine"].tolist() == ["samedi", "samedi", "samedi"]
    
    # Check that the "month_name_fr" column has the expected values
    assert transformed_df["month_name_fr"].tolist() == ["janvier", "janvier", "janvier"]
    
    # Check that the "date_du_jour" column has the expected values
    assert transformed_df["date_du_jour"].tolist() == ["01/01", "01/01", "01/01"]
    
    # Check that the "debut_semaine" column has the expected values
    assert transformed_df["debut_semaine"].tolist() == ["2021-12-27", "2021-12-27", "2021-12-27"]
    
    # Check that the "ferie" column has the expected values
    assert transformed_df["ferie"].tolist() == ["oui", "oui","oui"]
    
    # Check that the "retard" column has the expected values
    assert transformed_df["retard"].tolist() == [5, 10, 15]
    
    # Check that the "type_retard" column has the expected values
    assert transformed_df["type_retard"].tolist() == ["pas d'anomalie", "pas d'anomalie", "pas d'anomalie"]

# def test_transform_non_realise_daily_data():
#     arret = pd.DataFrame({'gid': [1, 2], 'libelle': ['arret1', 'arret2'], 'type': ['type1', 'type2'], 'voirie': ['voirie1', 'voirie2']})
#     ligne = pd.DataFrame({'ident': [1, 2], 'libelle': ['ligne1', 'ligne2']})
#     course = pd.DataFrame({'rs_sv_ligne_a': [1, 2], 'rs_sv_chem_l': [1, 2], 'gid': [1, 2], 'rs_sv_arret_p': [1, 2], 'rs_sv_cours_a': [1, 2], 'tempsarret': ['22p7', '23p8'], 'mdate': ['2022-01-01', '2022-01-02'], 'hor_theo': ['2022-01-01 00:00:00', '2022-01-02 00:00:00'], 'hor_real': ['2022-01-01 00:00:00', '2022-01-02 00:00:00']})
#     chemin = pd.DataFrame({'gid': [1, 2], 'libelle': ['chemin1', 'chemin2']})
#     batch_df = pd.DataFrame({'rs_sv_arret_p': [1, 2], 'rs_sv_cours_a': [1, 2], 'mdate': ['2022-01-01', '2022-01-02']})
#     expected_output = pd.DataFrame({'rs_sv_arret_p': [1, 2], 'rs_sv_cours_a': [1, 2], 'mdate': [pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'), pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC')], 'annee': [2022, 2022], 'mois': [1, 1], 'jour_semaine': ['samedi', 'dimanche'], 'month_name_fr': ['janvier', 'janvier'], 'date_du_jour': ['01/01', '02/01'], 'debut_semaine': ['2021-12-27', '2021-12-27'], 'ferie': ['oui', 'non'], 'gid_arret': [1, 2], 'arret': ['arret1', 'arret2'], 'vehicule': [None, None], 'type_arret': ['type1', 'type2'], 'voirie': ['voirie1', 'voirie2'], 'gid_course': [1, 2], 'id_ligne': [1, 2], 'ligne': ['ligne1', 'ligne2'], 'sens': [1, 2], 'chemin': ['chemin1', 'chemin2'], 'tempsarret': [22.7, 23.8], 'hor_theo': [pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'), pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC')], 'hor_real': [pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'), pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC')]})
#     assert transform_non_realise_daily_data(arret, ligne, course, chemin, batch_df).equals(expected_output)