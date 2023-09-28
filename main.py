from api.api import get_data
import dateutil.parser
from tables.sv_horai_a import load_data
from tables.sv_arret_p import load_sv_arret_p
from tables.sv_cours_a import load_sv_cours_a
from tables.sv_ligne_a import load_sv_ligne_a
# from tables.transform_sv_horai_a import transform_dataframe
import os
import pandas as pd
from tqdm import tqdm
import awswrangler as wr
import math
from datetime import datetime, timedelta
import warnings
from pandas.core.common import SettingWithCopyWarning
import config
from transform.overview import transform_dataframe
from transform.deep_analysis_and_anomaly import transform_dataframe_deep_analysis
from transform.daily_analysis import transform_daily_data


#DSW workspace config settings
DMDG_WORKSPACE_BUCKET = config.DMDG_WORKSPACE_BUCKET
WORKSPACE_PREFIX = config.WORKSPACE_PREFIX
TABLE_PATH = config.TABLE_PATH
DATABASE_NAME =config.DATABASE_NAME

#Loading - Append
def append_table(table_name,data):
    # Push 1 table, take name of the table and data to push
    path = f'{TABLE_PATH}{table_name}'

    wr.s3.to_parquet(
        df=data,
        path=path,
        database=DATABASE_NAME,
        dataset=True,
        table=table_name,
        mode='append',
        index=False,
        compression=None,
    )
    
    return True


#Loading - Overwrite
def push_table(table_name,data):
    # Push 1 table, take name of the table and data to push
    path = f'{TABLE_PATH}{table_name}'

    wr.s3.to_parquet(
        df=data,
        path=path,
        database=DATABASE_NAME,
        dataset=True,
        table=table_name,
        mode='overwrite',
        index=False,
        compression=None,
    )
    
    return True

#Extraction

# Obtenir la date d'aujourdhui

date_aujourd_hui = datetime.now().strftime("%Y-%m-%d") + "T23:59:59Z"
#(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d") + "T23:59:59Z"
# Charger les données en utilisant la date d'aujourd'hui
sv_horai_a = load_data(step_size=24, max_days=1, start_date=dateutil.parser.isoparse(date_aujourd_hui))
#sv_horai_a = load_data(step_size=24, max_days=1, start_date=dateutil.parser.isoparse("2023-08-30T23:59:59Z"))

print(f"Loading finished & sv_horai_a table contains {sv_horai_a.shape[0]} rows")
print(f"Nombre de lignes des voyages non realises: {sv_horai_a[sv_horai_a['etat'] == 'NON_REALISE'].shape[0]}")
fusion_df = sv_horai_a.sort_values('hor_theo')
fusion_df.reset_index(drop=True,inplace=True)
print(f"la table horaires chargée : {fusion_df}")

#load arret table
arret = load_sv_arret_p()

#Load lignes table
ligne = load_sv_ligne_a()

#load course table
course = load_sv_cours_a()

#load chemin table
chemin = get_data("sv_chem_l", {})




###########################################################Overview ###########################################################################
overview = transform_dataframe(arret,fusion_df)
print(f"nombre de lignes dans overview table : {overview.shape[0]}")


######################################################### Deep/Anomaly analysis ##########################################################
aggregated,anomalies = transform_dataframe_deep_analysis(arret,ligne,course,fusion_df)
print(f"nombre de lignes dans deep_analysis table : {aggregated.shape[0]}")
print(f"nombre de lignes dans anomalies table : {anomalies.shape[0]}")


######################################################## Daily analysis ####################################################################
non_realise_df,daily_df = transform_daily_data(arret,ligne,course,chemin,fusion_df)
print(f"nombre de lignes dans daily_analysis table : {daily_df.shape[0]}")
print(f"nombre de lignes dans non_realise table : {non_realise_df.shape[0]}")




# #####################################################Push to AWS S3 ########################################################################
#append overview
append_table("dwh_day_agg_sv_horai_a_by_arret",overview)
print("---------------------Appending to DWH finished for dwh_day_agg_sv_horai_a_by_arret-----------------------------------")


#Append anomalies DSW
append_table("dwh_merged_anomalies_detection",anomalies)
print("---------------------------Appending to DWH finished for dwh_merged_anomalies_detection-----------------------------------")


#append day aggregated table in DSW
append_table("dwh_merged_day_agg_sv_horai_a",aggregated)
print("---------------------------Appending to DWH finished for dwh_merged_day_agg_sv_horai_a-------------------------------------")


#Daily table to DSW
push_table("dwh_daily_analysis",daily_df)
print("----------------------Loading to DWH finished for dwh_daily_analysis table--------------------------------------------")

# Vérifier si non_realise_df n'est pas vide
if not non_realise_df.empty:
    # Exécuter push_table uniquement si non_realise_df n'est pas vide
    push_table("dwh_non_realise_daily", non_realise_df)
    print("----------------------Loading to DWH finished for dwh_non_realise_daily table--------------------------------------------")
else:
    print("NonRealise DataFrame is empty. Skipping push_table.")

