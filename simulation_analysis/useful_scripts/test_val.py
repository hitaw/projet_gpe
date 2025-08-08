import os
import pandas as pd
import plotly.express as px
import re
import Levenshtein
import unicodedata

def clean_string(string : str) -> str:
    """
    Cleans a string by removing accents, special characters, and converting it to lowercase.

    :param string: The original string to be cleaned.
    :return: A cleaned string with no accents, special characters, and in lowercase.
    """

    normalized_string = unicodedata.normalize('NFD', string)

    no_accent_string = ''.join(
        c for c in normalized_string if unicodedata.category(c) != "Mn"
    )

    cleaned_string = re.sub('[^0-9a-zA-Z]+', '', no_accent_string).lower()

    return cleaned_string

fact = 100
gtfs_folder = r"C:\Users\lea.movsessian\Documents\projet-gpe\data\IDFM-gtfs" # Il faudrait mettres les données GTFS dans un sous dossier ici.

stops = pd.read_csv(os.path.join(gtfs_folder, "stops.txt"))
routes = pd.read_csv(os.path.join(gtfs_folder, "routes.txt"))
eqasim_pt = pd.read_csv(r"control_output\1_percent\eqasim_pt.csv", sep=";")

validation_folder = "validation_data\csv"

train_files = []
bus_tram_files = []

for file in os.listdir(validation_folder):
    if "ferre" in file:
        train_files.append(file)
    if "surface" in file:
        bus_tram_files.append(file)

# __________ Métro, RER et Transilien __________ #

train_df = pd.DataFrame()

for file in train_files:
    temp_df = pd.read_csv(os.path.join(validation_folder, file),sep=";")
    train_df = pd.concat([train_df, temp_df])

train_df["NB_VALD"] = train_df["NB_VALD"].apply(lambda x: str(x).replace(" ","")).astype(int)

anomalies = train_df[train_df.ID_ZDC.isna()]

train_df = train_df[train_df.ID_ZDC.notna()]
train_df["ID_ZDC"] = train_df["ID_ZDC"].astype(int).astype(str)

train_df["ID_ZDC"] = "IDFM:" + train_df["ID_ZDC"]

nb_dates = len(train_df["JOUR"].unique())

train_df = train_df.groupby(['LIBELLE_ARRET', "ID_ZDC"], as_index=False)['NB_VALD'].sum()
anomalies = anomalies.groupby('LIBELLE_ARRET', as_index=False)['NB_VALD'].sum()

train_df["NB_VALD"]/=nb_dates
anomalies["NB_VALD"]/=nb_dates

eqasim_train = eqasim_pt[(eqasim_pt.transit_mode != "bus") & (eqasim_pt.transit_mode != "tram")]

eqasim_train_count = eqasim_train["access_area_id"].value_counts().reset_index()
eqasim_train_count["count"] *= fact

for _, row in eqasim_train_count.iterrows():

    stop_id = row["access_area_id"]
    stop_name = stops[stops.stop_id == stop_id]["stop_name"].values[0]
    matches = train_df[train_df.ID_ZDC == stop_id].values
    
    if len(matches) == 0:
        continue
    
    for m in matches:
        name = m[0]
        ratio = Levenshtein.ratio(clean_string(m[0]), clean_string(stop_name))

        if ratio < 0.5:
            print(f"Stop_name : {stop_name}")
            print(f"match name : {name}")
            print(f"ratio : {ratio}\n")
