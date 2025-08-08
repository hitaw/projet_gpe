import pandas as pd
import os
import plotly.express as px
import numpy as np
import folium
import branca.colormap as bcm
import matplotlib.pyplot as plt
import matplotlib.colors as colors
from scipy.spatial import ConvexHull

pd.set_option('display.max_columns', None)

stops = pd.read_csv("../implementation_gtfs/data/IDFM-gtfs/stops.txt")

baseline_folder = r"baseline_output\1_percent"

legs = pd.read_csv(os.path.join(baseline_folder, "eqasim_legs.csv"), sep=";")
legs = legs[legs["mode"] == "pt"]
legs = legs.drop(columns=["mode"])

pt = pd.read_csv(os.path.join(baseline_folder, "eqasim_pt.csv"), sep=";")

trips = pd.read_csv(os.path.join(baseline_folder, "eqasim_trips.csv"), sep=";")

pt_legs = pd.merge(legs, pt, on=["person_id", "person_trip_id", "leg_index"], how="inner")
pt_legs = pt_legs.sort_values(by=["person_id", "person_trip_id", "leg_index"])

pt_legs["speed"] = pt_legs["routed_distance"]/pt_legs["travel_time"] * 3.6

pt_legs_bus = pt_legs[pt_legs.transit_mode == "bus"]

pt_legs = pt_legs.groupby(['person_id', 'person_trip_id']) \
    .filter(lambda g: (g['transit_mode'] != "bus").any())

pt_legs = pt_legs.groupby(['person_id', 'person_trip_id']) \
    .filter(lambda g: (g['transit_mode'] == "bus").any())

pt_legs["prev_stop_id"] = pt_legs.groupby(["person_id", "person_trip_id"])["egress_area_id"].shift(1)
pt_legs["prev_transit_mode"] = pt_legs.groupby(["person_id", "person_trip_id"])["transit_mode"].shift(1)

pt_legs["next_stop_id"] = pt_legs.groupby(["person_id", "person_trip_id"])["access_area_id"].shift(-1)
pt_legs["next_transit_mode"] = pt_legs.groupby(["person_id", "person_trip_id"])["transit_mode"].shift(-1)

pt_legs_first = pt_legs.loc[(pt_legs.groupby(['person_id', "person_trip_id"])['leg_index'].idxmin())]
pt_legs_first = pt_legs_first.rename(columns={"next_stop_id":"stop_id"})

pt_legs_last = pt_legs.loc[(pt_legs.groupby(['person_id', "person_trip_id"])['leg_index'].idxmax())]
pt_legs_last = pt_legs_last.rename(columns={"prev_stop_id":"stop_id"})

pt_legs_first = pt_legs_first[pt_legs_first.transit_mode == "bus"]
pt_legs_last = pt_legs_last[pt_legs_last.transit_mode == "bus"]

pt_legs_first = pt_legs_first[pt_legs_first.next_transit_mode != "bus"]
pt_legs_last = pt_legs_last[pt_legs_last.prev_transit_mode != "bus"]

pt_legs_all = pd.concat([pt_legs_first, pt_legs_last])

anomalies = pt_legs_all[pt_legs_all.speed > 130]

pt_legs_all = pt_legs_all[pt_legs_all.speed<=130]
pt_legs_bus = pt_legs_bus[pt_legs_bus.speed<=130]

"""
fig = px.histogram(pt_legs_all, x='speed', nbins=30, title="Histogramme des vitesses (km/h)",
                   labels={'speed': 'Vitesse (km/h)'})
fig.update_layout(bargap=0.1)
fig.write_image("plots/analysis/speed.png")
fig.show()

fig = px.histogram(pt_legs_bus, x='speed', nbins=30, title="Histogramme des vitesses (km/h)",
                   labels={'speed': 'Vitesse (km/h)'})
fig.update_layout(bargap=0.1)
fig.show()
"""

pt_legs_first = pt_legs_first.rename(columns={"access_area_id":"from_to_stop_id"})
pt_legs_last = pt_legs_last.rename(columns={"egress_area_id":"from_to_stop_id"})

pt_legs_all_dist = pd.concat([pt_legs_first, pt_legs_last])

stops_from_to = stops[stops.stop_id.isin(pt_legs_all_dist.from_to_stop_id)]

count_trajets = pt_legs_all_dist['stop_id'].value_counts().reset_index()
count_trajets.columns = ['stop_id', 'weight']

moyennes = pt_legs_all_dist.groupby('stop_id')['euclidean_distance'].mean().reset_index()
moyennes.rename(columns={'euclidean_distance': 'mean_distance_m'}, inplace=True)

moyennes = moyennes.merge(count_trajets, on="stop_id", how="left")
moyennes = moyennes.sort_values(by='weight', ascending=True).reset_index(drop=True)

max_distances = pt_legs_all_dist.groupby('stop_id')['euclidean_distance'].max().reset_index()
max_distances.rename(columns={'euclidean_distance': 'max_distance_m'}, inplace=True)

stops = stops[stops.stop_id.isin(moyennes.stop_id)]

stops = pd.merge(stops, moyennes, on="stop_id", how="inner")

stops['quartile'] = pd.cut(stops["weight"],bins=[0, 20, 100, 200, float("inf")], labels = ["Q1", "Q2", "Q3", "Q4"])

min_weight = stops['weight'].min()
max_weight = stops['weight'].max()

norm = colors.Normalize(vmin=min_weight, vmax=max_weight)
colormap = plt.colormaps['YlOrRd']

branca_colormap = bcm.LinearColormap(
    colors=[colors.to_hex(colormap(norm(v))) for v in [min_weight, max_weight]],
    vmin=min_weight,
    vmax=max_weight,
    caption="Nombre d'occurrences"
)

center_lat = stops['stop_lat'].mean()
center_lon = stops['stop_lon'].mean()

stops_map = folium.Map(location=[center_lat, center_lon], zoom_start=11)

first_quartile = folium.FeatureGroup(name='Très Faible').add_to(stops_map)
second_quartile = folium.FeatureGroup(name='Faible').add_to(stops_map)
third_quartile = folium.FeatureGroup(name="Moyen").add_to(stops_map)
last_quartile = folium.FeatureGroup(name='Fort').add_to(stops_map)

for _, row in stops.iterrows():

    stop_id = row["stop_id"]
    quartile = row["quartile"]

    rgba = colormap(norm(row['weight']))
    color_hex = colors.to_hex(rgba)

    related_rows = pt_legs_all_dist[pt_legs_all_dist.stop_id==stop_id]
    from_ids = related_rows["from_to_stop_id"].unique()

    from_coords = stops_from_to[stops_from_to.stop_id.isin(from_ids)][['stop_lat','stop_lon']].to_numpy()

    if len(from_coords) >= 3:
        hull = ConvexHull(from_coords)
        hull_points = from_coords[hull.vertices]
        polygon_coords = [[lat, lon] for lat, lon in hull_points]
        polygon_coords.append(polygon_coords[0])

        polygon = folium.Polygon(
                locations=polygon_coords,
                color=color_hex,
                weight=2,
                fill=True,
                fill_color=color_hex,
                fill_opacity=0.2,
                popup=folium.Popup(f"{row['stop_name']}<br>Distance moyenne : {row['mean_distance_m']:.1f} m", max_width=250),
            )
    else:
        polygon = folium.Circle(
            location=[row['stop_lat'], row['stop_lon']],
            radius=row['mean_distance_m'],
            popup=folium.Popup(f"{row['stop_name']}<br>Distance moyenne : {row['mean_distance_m']:.1f} m", max_width=250),
            color=color_hex,
            fill=True,
            fill_color = color_hex,
            fill_opacity=0.2
        )

    marker = folium.Marker(
        location=[row['stop_lat'], row['stop_lon']],
        popup=row['stop_name'],
        icon=folium.Icon(color='gray', icon='info-sign')
    )

    if quartile == "Q1":
        polygon.add_to(first_quartile)
        marker.add_to(first_quartile)
    elif quartile == "Q2":
        polygon.add_to(second_quartile)
        marker.add_to(second_quartile)
    elif quartile == "Q3":
        polygon.add_to(third_quartile)
        marker.add_to(third_quartile)
    else:
        polygon.add_to(last_quartile)
        marker.add_to(last_quartile)

branca_colormap.add_to(stops_map)
folium.LayerControl().add_to(stops_map)
stops_map.save("map_stops.html")

fig = px.histogram(stops, x='weight', nbins=50, title="Distribution des occurrences par stop_id")
fig.update_layout(
    xaxis_title="Occurrences",
    yaxis_title="Nombre de stops",
    bargap=0.1
)
# fig.show()

stops = pd.read_csv("../implementation_gtfs/data/IDFM-gtfs/stops.txt")

stops_ = stops[stops.stop_name == "Marne-la-Vallée Chessy"]

print(pt_legs_all_dist[pt_legs_all_dist.stop_id.isin(stops_.stop_id)])

stops = stops[stops.stop_id.isin(pt_legs_all_dist.from_to_stop_id)]