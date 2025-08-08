import pandas as pd
from utils import *
import logging
from sklearn.linear_model import LinearRegression
from scipy.stats import gaussian_kde
import matplotlib.pyplot as plt
import seaborn as sns

NaN = float("nan")

logger = logging.getLogger("__main__")

def find_stop_link(stops : pd.DataFrame, stop_times : pd.DataFrame, trips : pd.DataFrame, journey_time : pd.DataFrame):

    stations_journey = set(journey_time["departure"].to_list() + journey_time["arrival"].to_list())
    cleaned_stations_journey = {clean_string(station) for station in stations_journey}

    stop_times_ids = set(stop_times.stop_id)

    stop_names = set(stops[stops.stop_id.isin(stop_times_ids)]["stop_name"])
    cleaned_stop_names = {clean_string(station) for station in stop_names}
    
    link_cleaned_name = (cleaned_stations_journey & cleaned_stop_names)

    if len(link_cleaned_name) > 1:
        raise ValueError()
    
    link_cleaned_name = link_cleaned_name.pop()

    original_name = None
    stop_id = []
    for _, stop in stops.iterrows():
        if clean_string(stop["stop_name"]) == link_cleaned_name:
            original_name = stop["stop_name"]
            stop_id.append(stop["stop_id"])

    return stop_id, original_name

def analyse_speed(stop_times : pd.DataFrame, stops : pd.DataFrame):
    
    trip_ids = stop_times.trip_id.unique()

    distances = []
    times = []

    for trip_id in trip_ids :
        stop_times_trip = stop_times[stop_times.trip_id == trip_id].sort_values(by="stop_sequence")

        for i in range(len(stop_times_trip) - 1):
            current_row = stop_times_trip.iloc[i]
            next_row = stop_times_trip.iloc[i + 1]

            current_hour = time_to_seconds(current_row["arrival_time"])
            next_hour = time_to_seconds(next_row["arrival_time"])

            time_between_stops = next_hour - current_hour

            depart_id = current_row["stop_id"]
            arrival_id = next_row["stop_id"]

            depart = stops[stops.stop_id == depart_id]
            arrival = stops[stops.stop_id == arrival_id]

            distance = haversine((depart["stop_lat"].values[0], depart["stop_lon"].values[0]), (arrival["stop_lat"].values[0], arrival["stop_lon"].values[0]))

            distances.append(distance)
            times.append(time_between_stops)

    distances = np.array(distances)
    times = np.array(times)

    model = LinearRegression()
    model.fit(distances.reshape(-1, 1), times)

    times_pred = model.predict(distances.reshape(-1, 1))

    a = model.coef_[0]
    b = model.intercept_

    return a, b

    # plt.figure(figsize=(8, 5))
    # plt.scatter(distances, times, alpha=0.3, label='Données brutes')
    # plt.plot(distances, times_pred, color='red', linewidth=2, label='Régression linéaire')
    # plt.xlabel('Distance (km)')
    # plt.ylabel('Temps (s)')
    # plt.legend()
    # plt.title("Graphique brut avec régression")
    # plt.show()

    # plt.figure(figsize=(8, 5))
    # plt.hexbin(distances.flatten(), times, gridsize=50, cmap="Blues", bins="log")
    # plt.plot(distances, times_pred, color='red', linewidth=2, label='Régression linéaire')
    # plt.colorbar(label="Densité de points")
    # plt.xlabel('Distance (km)')
    # plt.ylabel('Temps (s)')
    # plt.title("Densité des points (Hexbin)")
    # plt.show()

        
def add_extension(stop_times : pd.DataFrame, stop_times_ext : pd.DataFrame, stops_ext : pd.DataFrame, journey_time : pd.DataFrame, stations : pd.DataFrame, link_id : str, link_name : str):

    if clean_string(link_name) in journey_time["departure"].apply(clean_string).to_list():
        journey = journey_time

    elif clean_string(link_name) in journey_time["arrival"].apply(clean_string):
        journey =  pd.DataFrame({
            "departure" : journey_time["arrival"].to_list()[::-1],
            "arrival" : journey_time["departure"].to_list()[::-1],
            "time" : journey_time["time"].to_list()[::-1]
        })

    else:
        logging.error("This error is not supposed to be there. Check the code")
        return stop_times

    empty_journey = journey[pd.isna(journey["time"])]

    a = b = None
    if not empty_journey.empty:
        a, b = analyse_speed(stop_times_ext, stops_ext)

    ind_min = 1 if stop_times_ext[stop_times_ext.stop_sequence == 0].empty else 0

    # Extensions : only at the beginning and at the end
    stop_times_ext_outward = stop_times_ext.loc[(stop_times_ext.stop_sequence == ind_min)]
    stop_times_ext_return = stop_times_ext.loc[(stop_times_ext.groupby('trip_id')['stop_sequence'].idxmax())]
    
    stop_times_ext_outward = stop_times_ext_outward.loc[(stop_times_ext_outward.stop_id.isin(link_id))]
    stop_times_ext_return = stop_times_ext_return.loc[(stop_times_ext_return.stop_id.isin(link_id))]

    if stop_times_ext_outward.empty:
        logger.warning(f"Aucun trajet partant de {link_name} trouvé")
        return stop_times
    if stop_times_ext_return.empty:
        logger.warning(f"Aucun trajet arrivant à {link_name} trouvé")
        return stop_times

    new_stop_times = {
        "trip_id" : [],
        "arrival_time" : [],
        "departure_time" : [],
        "start_pickup_drop_off_window" : NaN,
        "end_pickup_drop_off_window" : NaN,
        "stop_id" : [],
        "stop_sequence" : [],
        "pickup_type" : 0,
        "drop_off_type" : 0,
        "local_zone_id" : NaN,
        "stop_headsign" : NaN,
        "timepoint" : 1,
        "pickup_booking_rule_id" : NaN,
        "drop_off_booking_rule_id" : NaN
    }

    for _, stop_times_link in stop_times_ext_return.iterrows():
        arrival_time = time_to_seconds(stop_times_link["arrival_time"])
        stop_sequence = stop_times_link["stop_sequence"]
        trip_id = stop_times_link["trip_id"]

        for ind, row in journey.iterrows():

            time_between_stops = row["time"]

            if pd.isna(time_between_stops):
                if clean_string(row["departure"]) == clean_string(link_name):
                    depart = stops_ext[stops_ext.stop_id == link_id[0]]
                    dep_lat = depart["stop_lat"].values[0]
                    dep_lon = depart["stop_lon"].values[0]
                else:
                    depart = stations[stations.name==row["departure"]]
                    dep_lat = depart["latitude"].values[0]
                    dep_lon = depart["longitude"].values[0]

                arrival = stations[stations.name==row["arrival"]]
                arr_lat = arrival["latitude"].values[0]
                arr_lon = arrival["longitude"].values[0]

                distance = haversine((dep_lat, dep_lon), (arr_lat, arr_lon))

                time_between_stops = int(a*distance+b)
                journey.loc[ind, "time"] = time_between_stops
                row["time"] = time_between_stops

            arrival_time += row["time"]
            stop_sequence += 1
            stop_id = stations[stations.name==row["arrival"]]["stop_id"].values[0]

            new_stop_times["trip_id"].append(stop_times_link["trip_id"])
            new_stop_times["arrival_time"].append(seconds_to_time(arrival_time)[1])
            new_stop_times["departure_time"].append(seconds_to_time(arrival_time)[1])
            new_stop_times["stop_id"].append(stop_id)
            new_stop_times["stop_sequence"].append(stop_sequence)

    for ind, stop_times_link in stop_times_ext_outward.iterrows():
        trip_id = stop_times_link["trip_id"]
        arrival_time = time_to_seconds(stop_times_link["arrival_time"])
        stop_sequence = len(journey) 
        stop_times_ext.loc[stop_times_ext.trip_id == trip_id, "stop_sequence"] += stop_sequence

        stop_sequence += stop_times_link["stop_sequence"]

        for _, row in journey.iterrows():
            arrival_time -= row["time"]
            stop_sequence -= 1
            stop_id = stations[stations.name==row["arrival"]]["stop_id"].values[0]

            new_stop_times["trip_id"].append(trip_id)
            new_stop_times["arrival_time"].append(seconds_to_time(arrival_time)[1])
            new_stop_times["departure_time"].append(seconds_to_time(arrival_time)[1])
            new_stop_times["stop_id"].append(stop_id)
            new_stop_times["stop_sequence"].append(stop_sequence)
    
    stop_times = pd.concat([stop_times, pd.DataFrame(new_stop_times)], ignore_index=True)
    stop_times.loc[stop_times.index.isin(stop_times_ext.index)] = stop_times_ext

    return stop_times