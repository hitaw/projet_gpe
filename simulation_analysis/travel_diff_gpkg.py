import pandas as pd 
import geopandas as gpd
import plotly.express as px
from shapely.geometry import Point

baseline_legs = pd.read_csv("baseline_output/navette/eqasim_legs.csv", sep=";")
baseline_pt = pd.read_csv("baseline_output/navette/eqasim_pt.csv", sep=";")
baseline_trips = pd.read_csv("baseline_output/navette/eqasim_trips.csv", sep=";")
baseline_trips = baseline_trips[baseline_trips["mode"]=="pt"]

gpe_legs = pd.read_csv("gpe_output/navette/eqasim_legs.csv", sep=";")
gpe_pt = pd.read_csv("gpe_output/navette/eqasim_pt.csv", sep=";")
gpe_trips = pd.read_csv("gpe_output/navette/eqasim_trips.csv", sep=";")
gpe_trips = gpe_trips[gpe_trips["mode"]=="pt"]

routes = pd.read_csv("../implementation_gtfs/GTFS_versions/GTFS_completed/routes.txt")

same_col = ["person_id", "person_trip_id", "origin_x", "origin_y", "destination_x", "destination_y", "departure_time", "travel_time"]

merged_data = baseline_trips[same_col].merge(
    gpe_trips[same_col],
    on=["person_id","person_trip_id"],
    suffixes=("_baseline", "_gpe"),
    how="outer" 
)

for col in ["origin_x", "origin_y", "destination_x", "destination_y", "departure_time"]:
    col_base = f"{col}_baseline"
    col_gpe = f"{col}_gpe"
    merged_data[f"{col}_match"] = merged_data[col_base] == merged_data[col_gpe]

diff_rows = merged_data.loc[~merged_data[[c for c in merged_data.columns if c.endswith("_match")]].all(axis=1)]

merged_data = merged_data.loc[merged_data[[c for c in merged_data.columns if c.endswith("_match")]].all(axis=1)]
merged_data["travel_time_diff"] = merged_data["travel_time_baseline"] - merged_data["travel_time_gpe"]
merged_data.loc[:,'travel_time_diff_percent'] = ((merged_data['travel_time_diff']) / merged_data['travel_time_baseline']) * 100

merged_trips_change = merged_data[merged_data.travel_time_diff != 0]

names = {"origin_x_baseline":"origin_x", 
         "origin_y_baseline":"origin_y", 
         "destination_x_baseline":"destination_x", 
         "destination_y_baseline":"destination_y"}

merged_trips_change = merged_trips_change[names.keys()].rename(columns=names)

rows = []

for ind, row in merged_trips_change.iterrows():

    rows.append({
        "label" : "départ",
        "id" : ind,
        "x" : row["origin_x"],
        "y": row["origin_y"]
    })

    rows.append({
        "label": "arrivée",
        "id": ind,
        "x": row["destination_x"],
        "y": row["destination_y"]
    })

rows = pd.DataFrame(rows)
gdf = gpd.GeoDataFrame(
    rows,
    geometry=gpd.points_from_xy(rows["x"], rows["y"]),
    crs="EPSG:2154"
)

gdf = gdf.to_crs("EPSG:4326")

gdf["lon"] = gdf.geometry.x
gdf["lat"] = gdf.geometry.y

gdf = gdf[["label", "id", "lat", "lon", "geometry"]]

gdf.to_file("outputs/points.gpkg", layer="points_layer", driver="GPKG")