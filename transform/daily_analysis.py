import os
import pandas as pd
from tqdm import tqdm
import awswrangler as wr
import math
import numpy as np
from datetime import datetime, timedelta
import dateutil.parser
from tables.sv_horai_a import load_data
from tables.sv_arret_p import load_sv_arret_p
from tables.sv_cours_a import load_sv_cours_a
from tables.sv_ligne_a import load_sv_ligne_a
from api.api import get_data
# from tables.sv_chem_l import load_sv_chem_l



def custom_convert_to_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        # Gérer les cas spécifiques de conversion pour les valeurs non numériques
        if value != None and value.endswith('p7'):
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


def merge_static_tables(ligne,course,chemin) -> pd.DataFrame : #chemin
    #Etape 1 : merge course/ligne
    ligne = ligne.rename(columns={'libelle': 'ligne'})
    ligne['ident'] = ligne['ident'].astype(int)
    course = course.merge(ligne[['ident', 'ligne', 'vehicule']], left_on='rs_sv_ligne_a', right_on='ident', how='left')
    course = course.rename(columns = {'ident':'id_ligne'})

    # Etape 2 : merge course/chemin
    chemin['gid'] = chemin['gid'].astype(int)
    chemin = chemin.rename(columns={'libelle': 'chemin'})
    chemin = chemin.rename(columns={'gid': 'gid_chemin'})
    course = course.merge(chemin[['gid_chemin', 'sens','chemin']], left_on='rs_sv_chem_l', right_on='gid_chemin', how='left')
    print(course.columns)
    course_df = course.rename(columns={'gid': 'gid_course'})
    course_df = course_df[['gid_course','id_ligne','ligne','sens','chemin']] 
    course_df.sort_values(by="gid_course")
    return course_df


def date_format(batch_df):
    batch_df["hor_theo"] = pd.to_datetime(batch_df["hor_theo"], errors='coerce',utc=True)
    batch_df["hor_real"] = pd.to_datetime(batch_df["hor_real"], errors='coerce',utc=True)
    batch_df["mdate"] = pd.to_datetime(batch_df["mdate"], errors='coerce',utc=True)
    return batch_df

def transform_non_realise_daily_data(arret,ligne,course,chemin,batch_df):
    # #Etape 1 : Merge static tables
    #merge static tables
    course_df = merge_static_tables(ligne,course,chemin)
    # merge_static_tables(ligne,course,chemin)
    # Étape 2 : data augmentation
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

        arret.rename(columns={"libelle": "arret"}, inplace=True)
        arret.rename(columns={"gid": "gid_arret"}, inplace=True)
        arret.rename(columns={"type": "type_arret"}, inplace=True)
        # Convertir la colonne 'gid' du DataFrame 'arret' en type 'object'
        arret['gid_arret'] = arret['gid_arret'].astype(int)
        
        
        #Etape 2: merge horaire/arret
        batch_df = batch_df.merge(arret[["gid_arret","arret", "vehicule", "type_arret", "voirie"]], left_on='rs_sv_arret_p', right_on='gid_arret', how='inner')

        #Etape 3 : merge horaire/course_df
        batch_df = batch_df.merge(course_df, left_on='rs_sv_cours_a', right_on='gid_course', how='inner')    
        
    except (AttributeError, ValueError) as e:
        print(e)
    
    return batch_df


def transform_daily_data(arret,ligne,course,chemin,batch_df): #prendre même les non realise

    #considèrer les voyages non réalisés aussi
    non_realise_df = batch_df.query('etat == "NON_REALISE"')
    if not non_realise_df.empty:
        non_realise_df = date_format(non_realise_df)
        non_realise_df = transform_non_realise_daily_data(arret,ligne,course,chemin,non_realise_df)
    #merge static tables
    course_df = merge_static_tables(ligne,course,chemin)
    print(f"la table course fusionnés avec ligne et chemin {course_df}")
    
    # Supprimer les valeurs non-finies dans la colonne "hor_theo" et "hor_real"
    batch_df.dropna(subset=["hor_theo"], inplace=True)
    batch_df.dropna(subset=["hor_real"], inplace=True)
    #Supprimer les valeurs non-finies dans la colonne "mdate"
    batch_df.dropna(subset=["mdate"], inplace=True)
    
    # Étape 2 : Supprimer les colonnes 'gid' et 'hor_app'
#     batch_df.drop(["gid", "hor_app"], axis=1, inplace=True)
    
    # Étape 3 : Convertir les colonnes en datetime
    batch_df = date_format(batch_df)
    
    
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

        arret.rename(columns={"libelle": "arret"}, inplace=True)
        arret.rename(columns={"gid": "gid_arret"}, inplace=True)
        arret.rename(columns={"type": "type_arret"}, inplace=True)
        # Convertir la colonne 'gid' du DataFrame 'arret' en type 'object'
        arret['gid_arret'] = arret['gid_arret'].astype(int)
        
        
        #Etape 8: merge horaire/arret
        batch_df = batch_df.merge(arret[["gid_arret","arret", "vehicule", "type_arret", "voirie"]], left_on='rs_sv_arret_p', right_on='gid_arret', how='inner')

        #Etape 9 : merge horaire/course_df
        batch_df = batch_df.merge(course_df, left_on='rs_sv_cours_a', right_on='gid_course', how='inner')
        print(f"la table daily transformes {batch_df}")
        print(f"la table de trajets non realises transformees {non_realise_df}")
        
    except (AttributeError, ValueError) as e:
        print(e)

        
    return non_realise_df,batch_df


    
    
                
    
    
    
    