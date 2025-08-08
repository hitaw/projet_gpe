import os
import pandas as pd
import plotly.express as px
import re

fact = 100
gtfs_folder = r"C:\Users\lea.movsessian\Documents\projet-gpe\GTFS_versions\GTFS_completed" # Il faudrait mettres les données GTFS dans un sous dossier ici.

stops = pd.read_csv(os.path.join(gtfs_folder, "stops.txt"))
eqasim_pt = pd.read_csv(r"control_output\1_percent\eqasim_pt.csv", sep=";")

validation_folder = "validation_data\csv"

train_files = []
bus_tram_files = [] #Tram T4 est dans train_files

for file in os.listdir(validation_folder):
    if "ferre" in file:
        train_files.append(file)
    if "surface" in file:
        bus_tram_files.append(file)

# Train

train_df = pd.DataFrame()

for file in train_files:
    temp_df = pd.read_csv(os.path.join(validation_folder, file),sep=";")
    train_df = pd.concat([train_df, temp_df])

train_df["NB_VALD"] = train_df["NB_VALD"].apply(lambda x: str(x).replace(" ","")).astype(int)

anomalies = train_df[train_df.ID_ZDC.isna()]["LIBELLE_ARRET"].unique()

train_df = train_df[train_df.ID_ZDC.notna()]
train_df["ID_ZDC"] = train_df["ID_ZDC"].astype(int)

stop_ids = stops["stop_id"].to_list()

dates = len(train_df["JOUR"].unique())

eqasim_pt = eqasim_pt[eqasim_pt.transit_mode != "bus"]

count_simu = {}
count_valid = {}
stop_ids=stop_ids[30:200]
i = 0

import time
simu = 0
ref = 0
for stop_id in stop_ids:

    parent_station_id = stops[stops.stop_id == stop_id]["parent_station"].item()
    if pd.isna(parent_station_id):
        continue
    parent_station = stops[stops.stop_id == parent_station_id]["stop_name"].item()

    parent_id_numbers = int("".join(re.findall(r"\d", parent_station_id)))

    # Simulation
    deb = time.time()
    simu_stops = eqasim_pt[(eqasim_pt.access_stop_id.str.contains(stop_id))]
    index_to_drop = simu_stops.index
    eqasim_pt = eqasim_pt.drop(index_to_drop)
    simu += time.time() - deb

    if parent_station not in count_simu:
        count_simu[parent_station] = 0

    count_simu[parent_station] += len(simu_stops) * fact

    # Validation

    deb = time.time()
    valid_stops = train_df[train_df["ID_ZDC"] == parent_id_numbers]
    index_to_drop = valid_stops.index
    train_df = train_df.drop(index_to_drop)
    ref += time.time() - deb

    if parent_station not in count_valid:
        count_valid[parent_station] = 0

    count_valid[parent_station] += valid_stops["NB_VALD"].sum()/dates

    i = i+1
    if i % 10 == 0:
        print(i)

print(f"Simu : {simu}")
print(f"Ref : {ref}")

count_simu = {key: value for key, value in count_simu.items() if value != 0}
count_valid = {key: value for key, value in count_valid.items() if value !=0} #On ne considère pas les arrêts trop peu desservis
fig = px.bar(pd.DataFrame({"Simulation" :count_simu, "Référence" : count_valid}), barmode="group")
fig.show()