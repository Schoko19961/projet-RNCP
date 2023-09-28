#Step 5 : Overview
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm
import warnings
from pandas.core.common import SettingWithCopyWarning

def custom_convert_to_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        # Gérer les cas spécifiques de conversion pour les valeurs non numériques
        if value.endswith('p7'):
            # Exemple de traitement pour la valeur '22p7'
            value = value.replace('p7', '.7')
            return float(value)
        else:
            # Autres cas de traitement ou renvoyer NaN pour les valeurs non convertibles
            return float('nan')

def get_month_name_fr(month):
    month_names_fr = {
        1: "janvier",
        2: "février",
        3: "mars",
        4: "avril",
        5: "mai",
        6: "juin",
        7: "juillet",
        8: "août",
        9: "septembre",
        10: "octobre",
        11: "novembre",
        12: "décembre"
    }
    return month_names_fr.get(month, "")


import numpy as np

def resample_data(df):
    df['retard'] = df['retard'].astype(float)  # Conversion de la colonne 'retard' en type float
    df.dropna(subset=['retard'], inplace=True)
    
    # Convertir la colonne 'hor_theo' en type datetime
    df['hor_theo'] = pd.to_datetime(df['hor_theo'])

    # Agrégations par jour, par arrêt et avec moyenne du retard et autres colonnes
    aggregated_df = df.groupby(['libelle','vehicule', pd.Grouper(freq='D', key='hor_theo')]).agg({
        'retard': 'mean',
        'annee': 'first',
        'month_name_fr': 'first',
        'jour_semaine': 'first',
        'debut_semaine': 'first',
        'ferie': 'first',
        'date_du_jour': 'first'
    }).reset_index()

    return aggregated_df

def groupby_aggregated(df):
    grouped_df = df.groupby(['libelle','vehicule','date_du_jour']).agg({
        'retard': 'mean',
        'annee': 'first',
        'month_name_fr': 'first',
        'jour_semaine': 'first',
        'debut_semaine': 'first',
        'ferie': 'first'
#         'type_retard': 'first'
    }).reset_index()
    
    # Conversion de la colonne 'retard' en type entier (int)
    grouped_df['retard'] = grouped_df['retard'].astype(int)
    
    # Création de la colonne 'type_retard' basée sur les conditions
    conditions = [
        grouped_df["retard"] > 15,
        grouped_df["retard"] < -10
    ]
    choices = [
        "tres en retard",
        "tres en avance"
    ]
    grouped_df["type_retard"] = np.select(conditions, choices, default="pas d'anomalie")
    
    return grouped_df



def transform_dataframe(arret,fusion_df):
    anomalies_list = []
    aggregated_list = []
    batch_size = 10000  # Nombre de lignes à traiter par lot
    #num_batches = 100
    num_batches = len(fusion_df) // batch_size + 1
    dataframes = []  # Liste pour stocker les dataframes de chaque lot
    #path = f'{TABLE_PATH}{table_name}'
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=SettingWithCopyWarning)
        warnings.filterwarnings("ignore", category=UserWarning, module='pandas')

        for i in tqdm(range(num_batches), desc="Traitement des lots"):
            start_idx = i * batch_size
            end_idx = start_idx + batch_size
            batch_df = fusion_df.loc[start_idx:end_idx].copy()

            # Étape 1 : Filtrer les colonnes avec état == 'REALISE'
            batch_df = batch_df.query('etat == "REALISE"')
            # Étape 2 : Supprimer les colonnes 'gid' et 'hor_app'
            batch_df.drop(["gid", "hor_app"], axis=1, inplace=True)

            # Supprimer les valeurs non-finies dans la colonne "hor_theo" et "hor_real"
            batch_df.dropna(subset=["hor_theo"], inplace=True)
            batch_df.dropna(subset=["hor_real"], inplace=True)
            # Supprimer les valeurs non-finies dans la colonne "mdate"
            batch_df.dropna(subset=["mdate"], inplace=True)

            if batch_df.empty:
                continue  # Ignorer les lots vides
            # Étape 3 : Convertir les colonnes en datetime
            batch_df["hor_theo"] = pd.to_datetime(batch_df["hor_theo"], errors='coerce', utc=True)
            batch_df["hor_real"] = pd.to_datetime(batch_df["hor_real"], errors='coerce', utc=True)
            batch_df["mdate"] = pd.to_datetime(batch_df["mdate"], errors='coerce', utc=True)

            # Étape 4 : Ajouter les colonnes année, mois et jour de semaine
            batch_df["annee"] = -1
            batch_df["mois"] = -1
            batch_df["jour_semaine"] = ""
            batch_df["month_name_fr"] = ""
            batch_df["date_du_jour"] = ""
            batch_df["debut_semaine"] = ""
            batch_df["ferie"] = "non"  # Ajout de la colonne ferie avec la valeur par défaut "non"

            date = batch_df["mdate"]
            try:
                batch_df["annee"] = date.dt.year
                batch_df["mois"] = date.dt.month
                
                # Jour de la semaine en français
                jour_semaine_fr = {
                    0: "lundi",
                    1: "mardi",
                    2: "mercredi",
                    3: "jeudi",
                    4: "vendredi",
                    5: "samedi",
                    6: "dimanche"
                }
                batch_df["jour_semaine"] = date.dt.weekday.map(jour_semaine_fr)
                
                # Mois en français
                batch_df["month_name_fr"] = date.dt.month.map(get_month_name_fr)
                
                # Date du jour au format "dd/mm"
                batch_df["date_du_jour"] = date.dt.strftime("%d/%m")
                
                # Début de semaine (lundi)
                batch_df["debut_semaine"] = (date - pd.to_timedelta(date.dt.weekday, unit='D')).dt.normalize().dt.strftime("%Y-%m-%d")



                # Étape 5 : Calculer le retard en minutes
                batch_df["retard"] = (batch_df["hor_real"] - batch_df["hor_theo"]).dt.total_seconds().astype(int) // 60
                # Étape 6 : Remplacer les retards nuls par la moyenne du retard
                mean_retard = batch_df["retard"].mean()
                batch_df["retard"].fillna(mean_retard, inplace=True)
                #batch_df.loc[(batch_df["retard"] > 15) | (batch_df["retard"] < -15), "retard"] = mean_retard
                
                #Etape 7 : mettre en evidence les anomalies : plus de 15min de retard ou plus de 10min en avance
                conditions = [
                    batch_df["retard"] > 15,
                    batch_df["retard"] < -10
                ]

                choices = [
                    "tres en retard",
                    "tres en avance"
                ]

                batch_df["type_retard"] = np.select(conditions, choices, default="pas d'anomalie")

                # Convertir les colonnes appropriées en type numérique
                numeric_cols = ['tempsarret', 'rs_sv_arret_p']  # Remplacez par les noms des colonnes à convertir
                for col in numeric_cols:
                    batch_df[col] = batch_df[col].apply(custom_convert_to_float)

                # batch[numeric_cols] = batch[numeric_cols].apply(pd.to_numeric, errors='coerce')

                # Chargement des jours fériés en France
                jours_feries = pd.DataFrame({
                    "date_du_jour": ["01/01", "01/05", "08/05", "14/07", "15/08", "01/11", "11/11", "25/12"],
                })
                batch_df["ferie"] = np.where(batch_df["date_du_jour"].isin(jours_feries["date_du_jour"]), "oui", "non")

                # Convertir la colonne 'gid' du DataFrame 'arret' en type 'object'
                arret['gid'] = arret['gid'].astype(int)
                batch_df = batch_df.merge(arret, left_on='rs_sv_arret_p', right_on='gid', how='inner')
                if batch_df.empty:
                    continue
#                 anomalies_list.append(anomalies) #To be added to daily anomalies detection (overwrite) & append to dwh_anomalies_detection_historic(append)
                #Add also in the airflow daily_list which stores daily table extracted (daily analysis Power BI tab)
                
                #Resample data pour l'analyse overview
                resampled_df = resample_data(batch_df)
                aggregated_list.append(resampled_df)
                
            except (AttributeError, ValueError) as e:
                print(e)
                continue


    # Concaténer tous les dataframes en un seul
    aggregated_df = pd.concat(aggregated_list)
    aggregated_df = groupby_aggregated(aggregated_df)
    return aggregated_df
