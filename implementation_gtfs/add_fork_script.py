import pandas as pd
import random
from utils import *
import logging
from line_extension import(analyse_speed)

NaN = float("nan")

logger = logging.getLogger("__main__")

## Change this to do a pretreatement and then, give it to add_extension
def add_fork(stop_times, stop_times_ext, stops_ext, journey_time, stations, link_id, link_name, frequency):
    
    with_link = stop_times_ext[stop_times_ext.stop_id.isin(link_id)]
    trips_ids = with_link["trip_id"].unique()

    start_ind = random.randint(0, int(1/frequency-1))

    trips_ids = [trips_ids[i] for i in range(len(trips_ids)) if (i+start_ind)%(1/frequency) == 0]

    if len(trips_ids) == 0:
        raise Exception("No trips found")

    stop_times_ext = stop_times_ext[stop_times_ext.trip_id.isin(trips_ids)]

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
    
    stop_times_ext_outward = stop_times_ext[stop_times_ext.direction_id == 1] # TODO : change the way direction are recognized
    stop_times_ext_return = stop_times_ext[stop_times_ext.direction_id == 0] # TODO

    new_stop_times_ext_outward = pd.DataFrame()
    for trip_id in stop_times_ext_outward.trip_id.unique():
        
        stop_times_trip = stop_times_ext_outward[stop_times_ext_outward.trip_id == trip_id]
        stop_sequence = stop_times_trip[stop_times_trip.stop_id.isin(link_id)]["stop_sequence"].item()
        stop_times_trip = stop_times_trip[stop_times_trip.stop_sequence >= stop_sequence]
        stop_times_trip["stop_sequence"] = stop_times_trip["stop_sequence"] - stop_sequence

        new_stop_times_ext_outward = pd.concat([new_stop_times_ext_outward, stop_times_trip])

    stop_times_ext_outward = new_stop_times_ext_outward

    new_stop_times_ext_return = pd.DataFrame()
    for trip_id in stop_times_ext_return.trip_id.unique():
        
        stop_times_trip = stop_times_ext_return[stop_times_ext_return.trip_id == trip_id]
        stop_sequence = stop_times_trip[stop_times_trip.stop_id.isin(link_id)]["stop_sequence"].item()
        stop_times_trip = stop_times_trip[stop_times_trip.stop_sequence <= stop_sequence]

        new_stop_times_ext_return = pd.concat([new_stop_times_ext_return, stop_times_trip])

    stop_times_ext_return = new_stop_times_ext_return

    filtered_stop_times = pd.concat([new_stop_times_ext_outward, new_stop_times_ext_return])
    filtered_trip_ids = filtered_stop_times["trip_id"].unique()

    stop_times_unchanged = stop_times[~stop_times["trip_id"].isin(filtered_trip_ids)]
    stop_times = pd.concat([stop_times_unchanged, filtered_stop_times], ignore_index=True)

    stop_times_ext_unchanged = stop_times_ext[~stop_times_ext["trip_id"].isin(filtered_trip_ids)]
    stop_times_ext = pd.concat([stop_times_ext_unchanged, filtered_stop_times], ignore_index=True)

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

    ind_min = 1 if stop_times_ext[stop_times_ext.stop_sequence == 0].empty else 0

    stop_times_ext_outward = stop_times_ext.loc[(stop_times_ext.stop_sequence == ind_min)]
    stop_times_ext_return = stop_times_ext.loc[(stop_times_ext.groupby('trip_id')['stop_sequence'].idxmax())]
    
    stop_times_ext_outward = stop_times_ext_outward.loc[(stop_times_ext_outward.stop_id.isin(link_id))]
    stop_times_ext_return = stop_times_ext_return.loc[(stop_times_ext_return.stop_id.isin(link_id))]

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
    
    updated_trips_ids = pd.concat([stop_times_ext, pd.DataFrame(new_stop_times)])["trip_id"].unique()
    stop_times = stop_times[~stop_times["trip_id"].isin(updated_trips_ids)]
    stop_times_updated = pd.concat([stop_times_ext, pd.DataFrame(new_stop_times)], ignore_index=True)
    stop_times = pd.concat([stop_times, stop_times_updated], ignore_index=True)

    new_stop_times = pd.DataFrame(new_stop_times)
    return stop_times







