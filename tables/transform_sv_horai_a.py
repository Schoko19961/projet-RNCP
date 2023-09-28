import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import warnings
from pandas.core.common import SettingWithCopyWarning
import awswrangler as wr
import config

#DSW workspace config settings
DMDG_WORKSPACE_BUCKET = config.DMDG_WORKSPACE_BUCKET
WORKSPACE_PREFIX = config.WORKSPACE_PREFIX
TABLE_PATH = config.TABLE_PATH
DATABASE_NAME =config.DATABASE_NAME

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

def transform_dataframe(fusion_df,table_name):
    batch_size = 10000  # Nombre de lignes à traiter par lot
    num_batches = len(fusion_df) // batch_size + 1
    dataframes = []  # Liste pour stocker les dataframes de chaque lot
    path = f'{TABLE_PATH}{table_name}'
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=SettingWithCopyWarning)
        warnings.filterwarnings("ignore", category=UserWarning, module='pandas')

        for i in tqdm(range(num_batches), desc="Traitement des lots"):
            start_idx = i * batch_size
            end_idx = start_idx + batch_size
            batch_df = fusion_df.iloc[start_idx:end_idx].copy()


            # Étape 1 : Filtrer les colonnes avec état == 'REALISE'
            batch_df = batch_df.query('etat == "REALISE"')
            # Étape 2 : Supprimer les colonnes 'gid' et 'hor_app'
            batch_df.drop(["gid", "hor_app"], axis=1, inplace=True)
            
            # Supprimer les valeurs non-finies dans la colonne "hor_theo" et "hor_real"
            batch_df.dropna(subset=["hor_theo"], inplace=True)
            batch_df.dropna(subset=["hor_real"], inplace=True)
            #Supprimer les valeurs non-finies dans la colonne "mdate"
            batch_df.dropna(subset=["mdate"], inplace=True)
            
            
            if batch_df.empty:
                continue  # Ignorer les lots vides
            # Étape 3 : Convertir les colonnes en datetime
            batch_df["hor_theo"] = pd.to_datetime(batch_df["hor_theo"], errors='coerce',utc=True)
            batch_df["hor_real"] = pd.to_datetime(batch_df["hor_real"], errors='coerce',utc=True)
            batch_df["mdate"] = pd.to_datetime(batch_df["mdate"], errors='coerce',utc=True)

            # Étape 4 : Ajouter les colonnes année, mois et jour de semaine
            batch_df["annee"] = -1
            batch_df["mois"] = -1
            batch_df["jour_semaine"] = ""
            
            #date = pd.to_datetime(batch_df["mdate"])
            date = batch_df["mdate"]
            try:
                batch_df["annee"] = date.dt.year
                batch_df["mois"] = date.dt.month
                batch_df["jour_semaine"] = date.dt.strftime("%A")
                # Étape 5 : Calculer le retard en minutes
                batch_df["retard"] = (batch_df["hor_real"] - batch_df["hor_theo"]).dt.total_seconds().astype(int) // 60
                # Étape 6 : Remplacer les retards supérieurs à 15 minutes ou inférieurs à -15 minutes par la moyenne totale du retard
                mean_retard = batch_df["retard"].mean()
                batch_df["retard"].fillna(mean_retard, inplace=True)
                batch_df.loc[(batch_df["retard"] > 15) | (batch_df["retard"] < -15), "retard"] = mean_retard
                # Convertir les colonnes appropriées en type numérique
                numeric_cols = ['tempsarret','rs_sv_arret_p']  # Remplacez par les noms des colonnes à convertir
                for col in numeric_cols:
                    batch_df[col] = batch_df[col].apply(custom_convert_to_float)

                #batch[numeric_cols] = batch[numeric_cols].apply(pd.to_numeric, errors='coerce')
                
                #Load table
                wr.s3.to_parquet(
                    df=batch_df,
                    path=path,
                    database=DATABASE_NAME,
                    dataset=True,
                    table=table_name,
                    mode='append',
                    index=False,
                    compression=None,
                )
            except (AttributeError,ValueError) as e:
                print(e)
                continue
                
            #print(batch_df)

            #dataframes.append(batch_df)

    #transformed_df = pd.concat(dataframes)  # Concaténer tous les dataframes en un seul

    return True
