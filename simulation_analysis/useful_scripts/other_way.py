import os
import pandas as pd
import plotly.express as px
import re
import Levenshtein
import unicodedata

import plotly.io as pio
pio.templates["custom"] = pio.templates["plotly_white"]
pio.templates["custom"]["layout"]["font"] = {"size": 15}
pio.templates.default = "custom"

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

#Référence

train_df = pd.DataFrame()

for file in train_files:
    temp_df = pd.read_csv(os.path.join(validation_folder, file),sep=";")
    train_df = pd.concat([train_df, temp_df])

nb_dates = len(train_df["JOUR"].unique())

train_df["NB_VALD"] = train_df["NB_VALD"].apply(lambda x: str(x).replace(" ","")).astype(int)

anomalies = train_df[train_df.ID_ZDC.isna()]

train_df = train_df[train_df.ID_ZDC.notna()]
train_df["ID_ZDC"] = train_df["ID_ZDC"].astype(int).astype(str)

train_df = train_df.groupby(['LIBELLE_ARRET', "ID_ZDC"], as_index=False)['NB_VALD'].sum()
anomalies = anomalies.groupby('LIBELLE_ARRET', as_index=False)['NB_VALD'].sum()

stations = train_df["LIBELLE_ARRET"].unique()
for name in anomalies["LIBELLE_ARRET"].unique():
    best = None
    val = 0

    for stat in stations:
        rat = Levenshtein.ratio(clean_string(name), clean_string(stat))
        #print(stat)
        if rat > val:
            val = rat
            best = stat

    train_df.loc[train_df.LIBELLE_ARRET == best, "NB_VALD"] += anomalies.loc[anomalies.LIBELLE_ARRET == name, "NB_VALD"].values[0]

train_df["ID_ZDC"] = "IDFM:" + train_df["ID_ZDC"]

train_df = train_df.merge(
    stops[['stop_id', 'stop_name']],
    left_on='ID_ZDC',
    right_on='stop_id',
    how='left'
).rename(columns={'stop_name': 'name'}).drop('stop_id', axis=1)

anomalies = train_df[train_df.name.isna()]

parent_stops = stops[stops.location_type == 1]

for _, arret in anomalies.iterrows():
    name = arret["LIBELLE_ARRET"]

    if arret["ID_ZDC"] == "IDFM:999999":
        print(f"Skipped {name}, with {arret['NB_VALD']} entries")
        continue

    best_match = None
    best_id = None
    best_ratio = 0

    for _, row in parent_stops.iterrows():
        station = row["stop_name"]
        ratio = Levenshtein.ratio(clean_string(name), clean_string(station))
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = station
            best_id = row["stop_id"]

    if best_ratio > 0.65:
        train_df.loc[train_df.LIBELLE_ARRET == name, "name"] = best_match
        train_df.loc[train_df.LIBELLE_ARRET == name, "ID_ZDC"] = best_id

    else:
        print(f"Did not find a good match for {name} [best_match is {best_match}, ratio : {best_ratio}]")

train_df["NB_VALD"] /= nb_dates

# Simulation 

test = eqasim_pt[(eqasim_pt.transit_mode != "bus") & (eqasim_pt.transit_mode != "tram")]

eqasim_train = eqasim_pt.sort_values(by=["person_id", "person_trip_id", "leg_index"]).reset_index(drop=True)

eqasim_train["prev_egress_area_id"] = eqasim_train.groupby(["person_id", "person_trip_id"])["egress_area_id"].shift(1)
eqasim_train["prev_transit_mode"] = eqasim_train.groupby(["person_id", "person_trip_id"])["transit_mode"].shift(1)

mask = (
    (eqasim_train["access_area_id"] == eqasim_train["prev_egress_area_id"]) & 
    (eqasim_train["transit_mode"].isin(["rail", "subway"])) & 
    (eqasim_train["prev_transit_mode"].isin(["rail", "subway"]))
)

eqasim_train = eqasim_train[~mask].drop(columns=["prev_egress_area_id", "prev_transit_mode"])
eqasim_train = eqasim_train[(eqasim_train.transit_mode != "bus") & (eqasim_train.transit_mode != "tram")]

eqasim_train_count = eqasim_train["access_area_id"].value_counts().reset_index()
eqasim_train_count["count"] *= fact

eqasim_train_count = eqasim_train_count.merge(
    stops[['stop_id', 'stop_name']],
    left_on='access_area_id',
    right_on='stop_id',
    how='left'
).rename(columns={'stop_name': 'name'}).drop('stop_id', axis=1)

merge_data = train_df.merge(eqasim_train_count, on = "name")

merge_data = merge_data.rename(columns={"count" : "Simulation", "NB_VALD" : "Référence"})

melted_data = merge_data.melt(id_vars='name', value_vars=['Référence', 'Simulation'], var_name='Metric', value_name='Value')


merge_data_sample = merge_data.sample(10)
melted_data_sample = merge_data_sample.melt(id_vars='name', value_vars=['Référence', 'Simulation'], var_name='Metric', value_name='Value')

fig = px.histogram(melted_data_sample, x='name', y='Value', color='Metric', barmode='group',
                   labels={"name": "Arrêt", "Value" : "Nombre de validations"}, title='Comparaison entre la référence et la simulation')
fig.write_image("plots/validation/comp_ref_simu_ferre.png", width=1000)
fig.show()

merge_data['Percentage_Difference'] = (
    (merge_data['Simulation'] - merge_data['Référence']) / merge_data['Référence']
) * 100

print(merge_data_sample)

filtered_data = merge_data[merge_data['name'].isin(merge_data_sample['name'].to_list())]

fig_diff = px.bar(filtered_data, x='name', y='Percentage_Difference',
                  title='Différence (en %) entre les données de référence et la simulation baseline pour chaque arrêt',
                  labels={'Percentage_Difference': 'Différence (%)', "name": "Arrêt"})

fig_diff.write_image("plots/validation/comp_ref_simu_ferre_pourcent.png", width=1000)
fig_diff.show()

sum_data = melted_data.groupby("Metric")["Value"].sum().reset_index()

reference_value = sum_data.loc[sum_data["Metric"] == "Référence", "Value"].values[0]
simulation_value = sum_data.loc[sum_data["Metric"] == "Simulation", "Value"].values[0]
diff_percentage = ((simulation_value - reference_value) / reference_value) * 100

fig2 = px.bar(sum_data, x="Metric", y="Value", title="Somme totale des valeurs")

# Ajouter une annotation pour afficher la différence en pourcentage
fig2.add_annotation(
    x=0.5, y=max(reference_value, simulation_value),  # Position
    text=f"Différence : {diff_percentage:.2f}%",  # Texte formaté
    showarrow=False,
    font=dict(size=14, color="red"),
    xref="paper", yref="y"
)

fig2.write_image("plots/validation/comp_ref_simu_ferre_total.png", width=1000)
fig2.show()

# __________ Bus et Tram __________ #

eqasim_surface = eqasim_pt[(eqasim_pt.transit_mode == "bus") | (eqasim_pt.transit_mode == "tram")]

surface_simu = pd.DataFrame()

for file in bus_tram_files:
    temp_df = pd.read_csv(os.path.join(validation_folder, file),sep=";")
    surface_simu = pd.concat([surface_simu, temp_df])

nb_dates = len(surface_simu["JOUR"].unique())

referentiel_lines = pd.read_csv(os.path.join("validation_data", "referentiel-des-lignes.csv"), sep=";")

selected_columns = ['ID_Line', 'ID_GroupOfLines']
referentiel_lines = referentiel_lines[selected_columns]

surface_simu = surface_simu.groupby("ID_GROUPOFLINES", as_index=False)["NB_VALD"].sum()

surface_simu = surface_simu.merge(
    referentiel_lines[['ID_GroupOfLines', 'ID_Line']],
    left_on = "ID_GROUPOFLINES",
    right_on = "ID_GroupOfLines",
    how = "left"
)

surface_simu["ID_Line"] = "IDFM:" + surface_simu["ID_Line"]

surface_simu = surface_simu.merge(
    routes[["route_id", "route_short_name", "route_type"]],
    left_on = 'ID_Line',
    right_on = "route_id",
    how = "left"
)

surface_simu = surface_simu.drop(columns=["ID_GroupOfLines", "ID_Line"])

eqasim_surface = eqasim_surface["transit_line_id"].value_counts().reset_index()
eqasim_surface["count"] *= fact

surface_simu = surface_simu.merge(
    eqasim_surface[["transit_line_id", "count"]],
    left_on = "route_id",
    right_on = "transit_line_id",
    how="left"
)

anomalies = surface_simu[surface_simu.route_id.isna()]
surface_simu = surface_simu[surface_simu.route_id.notna()]
surface_simu["route_type"] = surface_simu["route_type"].astype(int)
surface_simu = surface_simu[(surface_simu.route_type == 0) | (surface_simu.route_type == 3)]
surface_simu["count"] = surface_simu["count"].fillna(0)

surface_simu = surface_simu[(surface_simu.route_type == 0)]

surface_simu = surface_simu.groupby(["ID_GROUPOFLINES", "NB_VALD", "route_short_name"], as_index="False")["count"].sum().reset_index()

surface_simu = surface_simu.rename(columns = {"NB_VALD" : "Référence", "count" : "Simulation"})
surface_simu["Référence"] /= nb_dates

melted_data = surface_simu.melt(id_vars='route_short_name', value_vars=['Référence', 'Simulation'], var_name='Metric', value_name='Value')
fig = px.histogram(melted_data, 
                   x='route_short_name', 
                   y='Value', 
                   color='Metric', 
                   barmode='group',
                   labels={
                       "route_short_name" : "Ligne de Tramway",
                       "Value" : "Nombre de trajets"
                   },
                   title='Comparaison entre les données de référence et la simulation baseline')

fig.write_image("plots/validation/comp_ref_simu_surface.png", width=1000)
fig.show()

surface_simu['Différence (%)'] = ((surface_simu['Simulation'] - surface_simu['Référence']) / surface_simu['Référence']) * 100
diff_data = surface_simu[['route_short_name', 'Différence (%)']]

fig_diff = px.bar(diff_data, 
                  x='route_short_name', 
                  y='Différence (%)', 
                  labels={
                      "route_short_name" : "Ligne de Tramway"
                  },
                  title='Différence (en %) entre les données de référence et la simulation baseline')

fig_diff.write_image("plots/validation/comp_ref_simu_surface_pourcentage.png", width=1000)
fig_diff.show()