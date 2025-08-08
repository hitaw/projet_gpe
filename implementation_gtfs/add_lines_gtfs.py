import os
import sys
import json
import shutil
import time
import logging
import polars as pl
from utils import *
from find_frequency import *
from line_extension import *
from add_fork_script import *


logger = logging.getLogger(__name__)


keys_needed = ["stations", "lines", "journey_time", "gtfs_data", "dest_folder"]
files_needed = ["agency.txt", "routes.txt", "trips.txt", "calendar.txt", "calendar_dates.txt", "stop_times.txt", "stops.txt"]
NaN = float("nan")

routes_type = [0, 1, 2, 3, 4, 5, 6, 7, 11, 12]

if __name__ == "__main__":

    #filename = "logs.log"
    logging.basicConfig(filename=None, level=logging.INFO, format='%(asctime)s - %(levelname)s:%(name)s:%(message)s', datefmt='%H:%M:%S', encoding="utf-8")
    logger.info("Started")

    beginning = time.time()

    args = sys.argv[1:]
    n_args = len(args)

    if n_args == 0:
        if not os.path.isfile("config.json"):
            e = "config.json does not exists in the current directory. You can either add it or give the complete path in argument"
            logger.error(e)
            raise Exception(e)
        path = "config.json"
    elif n_args == 1:
        path = args[0]
        if not os.path.isfile(path):
            e = f"{path} is not a valid file."
            logger.error(e)
            raise Exception(e)
        
        if not path.endswith(".json"):
            e = f"{path} is not a json file, please give a .json file"
            logger.error(e)
            raise Exception(e)
    else:
        e = "Too many arguments, expected 0 or 1"
        logger.error(e)
        raise Exception(e)
    
    with open(path, "r") as config_file:
        config_data = json.load(config_file)

    config_keys = config_data.keys()
    for k in keys_needed:
        if not k in config_keys:
            e = f"{k} not in {path}"
            logger.error(e)
            raise Exception(e)
        
    k_files = ["stations", "lines", "journey_time"]
    k_folder = ["gtfs_folder", "dest_folder"]
    for k, val in config_data.items():
        if k in k_files:
            if not os.path.isfile(val):
                e = f"{val} is not a valid file"
                logger.error(e)
                raise Exception(e)
        elif k in k_folder:
            if not os.path.isdir(val):
                e = f"{val} is not a valid directory"
                logger.error(e)
                raise Exception(e)

    # Read data
    stations = read_data(config_data["stations"])

    if "name" not in stations:
        e = f'Missing "name" field in {config_data[stations]}'
        logger.error(e)
        raise Exception(e)

    lines = read_data(config_data["lines"])

    if "name" not in lines:
        e = f'Missing column "name" in {config_data["lines"]}'
        logger.error(e)
        raise Exception(e)
    
    if "type" not in lines:
        e = f'Missing column "type" in {config_data["lines"]}'
        logger.error(e)
        raise Exception(e)
    
    lines["name"] = lines["name"].astype(str)

    journey_time = read_data(config_data["journey_time"])
    journey_columns = ["line", "departure", "arrival", "time"]

    for column in journey_columns:
        if column not in journey_time:
            e = f'Missing {column} field in {config_data["journey_time"]}'
            logger.error(e)
            raise Exception(e)
    
    journey_time["line"] = journey_time["line"].astype(str)

    if "forks" in config_data:
        forks_file = config_data["forks"]
        if os.path.isfile(forks_file):
            forks = pd.read_csv(forks_file)
        else:
            e = f"{forks_file} is not a valid file"
            logger.error(e)
            raise Exception(e)

    dest_folder = config_data["dest_folder"]
    gtfs_folder = config_data["gtfs_data"]

    if "suffix" in config_data:
        suffix = config_data["suffix"]
    else: 
        suffix = ""
    
    gtfs_data = [f for f in os.listdir(gtfs_folder) if os.path.isfile(os.path.join(gtfs_folder, f))]

    for file in files_needed:
        if not file in gtfs_data:
            e = f"Missing {file} in your gtfs folder"
            logger.error(e)
            raise Exception(e)
        
    for file in gtfs_data:
        if not file in files_needed or file == "calendar_dates.txt":
            shutil.copy(os.path.join(gtfs_folder, file), os.path.join(dest_folder, file))

    logger.info("Reading gtfs data...")

    routes = read_data(os.path.join(gtfs_folder, "routes.txt"))
    agency_data = read_data(os.path.join(gtfs_folder, "agency.txt"))
    stops_data = read_data(os.path.join(gtfs_folder, "stops.txt"))
    calendar = read_data(os.path.join(gtfs_folder, "calendar.txt"))
    trips_data = read_data(os.path.join(gtfs_folder, "trips.txt"))
    calendar_dates = read_data(os.path.join(gtfs_folder, "calendar_dates.txt"))
    stop_times_data = read_data(os.path.join(gtfs_folder, "stop_times.txt"))

    logger.info("Reading Done")
        
    # ROUTES AND AGENCY
    
    logger.info("Beginning creation of routes")

    # Check if one of the route already exists in the gtfs data
    
    extensions = lines.reset_index().merge(routes, left_on=['name', 'type'], right_on=['route_long_name', 'route_type']).set_index("index")

    if len(extensions) > 0:
        index_to_drop = extensions.index
        lines = lines.drop(index_to_drop)
        e = f"Detection of {len(extensions)} already existing line(s). They will be considered as line extension or forks if applicable."
        logger.info(e)

    # Get the new ids for the new routes
    new_routes_ids = calculate_new_ids(routes, "route_id", len(lines))
    lines["route_id"] = new_routes_ids

    # Find the associated agency_ids for each route.
    agency_ids = []
    new_agency_name = None
    new_agency_id = None

    for ind, row in lines.iterrows():
        ag = row["agency"] if "agency" in lines else None
        match = agency_data[agency_data.agency_name == row["agency"]]["agency_id"]

        # If the agency does not already exists in the GTFS data, create a new one
        if len(match) == 0:
            # What if row["agency"] is not NaN and config data also contains "agency" ? TODO
            # What if config_data["agency"] is not a valid value ? TODO
            if not new_agency_name:
                if "agency" in config_data:
                    new_agency_name = config_data["agency"] # What if config_data["agency"] in agency_data ? TODO
                else:
                    new_agency_name = calculate_new_ids(agency_data, "agency_name",1)[0]

            new_agency_id = calculate_new_ids(agency_data, "agency_id", 1)[0]

        agency_ids.append(match.item() if len(match) == 1 else new_agency_id)

    # We totally could add multiple agencies TODO
    if new_agency_name:
        # Add the new data for the agencies
        agency_data.loc[len(agency_data)] = {
            "agency_id" : new_agency_id,
            "agency_name" : new_agency_name,
            "agency_url" : "https://www.iledefrance-mobilites.fr", #What if this is not the right link ? TODO
            "agency_timezone" : "Europe/Paris", # What if the timezone is different ? TODO
            "agency_lang" : NaN,
            "agency_phone" : NaN,
            "agency_email" : NaN,
            "agency_fare_url" : NaN,
            "ticketing_deep_link" : NaN,
        }

    names = lines["name"].to_list()

    if len(names) != len(lines):
        e = "Lines names are not unique, please check your data"
        logger.error(e)
        raise Exception(e)

    # Get the line color from the dataframe (or black if "color" does not exist)
    if "color" in lines:
        colors = lines["color"].to_list()
        colors = [c if pd.notna(c) else "000000" for c in colors]
    else:
        colors = ["000000"]*len(lines)

    # Find the text color using the function find_text_color
    text_colors = [find_text_color(c) for c in colors]

    # Create the dataframe for the new routes
    new_routes_data = pd.DataFrame({
        "route_id" : new_routes_ids,
        "agency_id" : agency_ids,
        "route_short_name" : [name[:8] for name in names],
        "route_long_name" : names,
        "route_desc" : NaN,
        "route_type" : lines["type"],
        "route_url" : NaN,
        "route_color" : colors,
        "route_text_color" : text_colors,
        "route_sort_order" : NaN
    })

    # Merge the GTFS routes with the new ones
    all_routes_data = pd.concat([routes, new_routes_data], ignore_index=True)

    # Save routes.txt and agency.txt in the destination folder
    all_routes_data.to_csv(os.path.join(dest_folder, "routes.txt"), index=False)
    agency_data.to_csv(os.path.join(dest_folder, "agency.txt"), index=False)

    logger.info("Routes created")

    # STOPS

    logger.info("Beggining creation of stops")

    # By default, create one parent_station (type 1) and one stop (type 0) by station
    number_of_new_stops = len(stations)*2

    debut = time.time()
    for ind, row in stations.iterrows():

        # Clean the string in case the names are the same but different because of an accent or a special character
        clean_station_name = clean_string(row["name"])
        clean_stop_names = stops_data["stop_name"].apply(clean_string)

        # Find the parent station in the stops_data dataframe
        parent_station = stops_data[(clean_stop_names == clean_station_name) & (stops_data.location_type == 1)] #If two stops do not have the same name (without accent and special characters), it does not work TODO
        
        # If a match is found, retrieve the parent_station id 
        parent_station_name = parent_station["stop_name"].item() if len(parent_station) == 1 else None # Assume one match or 0 will be found TODO
        parent_station_id = parent_station["stop_id"].item() if len(parent_station) == 1 else None

        stations.loc[ind, "parent_id"] = parent_station_id
        
        # If a parent_station is found, substract one to the number of stops to create
        if parent_station_name:
            number_of_new_stops -= 1

    logger.debug(f"Time spend to find equivalent stations : {time.time()-debut:.2f} s")

    # Get the new ids for the new stops
    new_stops_ids = calculate_new_ids(stops_data, "stop_id", number_of_new_stops)

    # Create a list with all of the parent station ids
    new_parent_ids = iter(new_stops_ids[len(stations):])
    all_parent_ids = stations["parent_id"].to_list()
    all_parent_ids = [next(new_parent_ids) if x is None else x for x in all_parent_ids]
    all_parent_ids += [NaN for _ in range(number_of_new_stops - len(stations))]

    # Create the list with all of the stop_names
    new_parents = stations[stations.parent_id.isna()]
    
    new_stop_names = stations["name"].to_list()
    new_parent_stop_names = new_parents["name"].to_list()
    stop_names = new_stop_names + new_parent_stop_names

    # Retrieve the coordinates of all new stops

    if "latitude" not in stations:
        e = f'Missing "latitude" field in {config_data[stations]}'
        logger.error(e)
        raise Exception(e)
    
    if "longitude" not in stations:
        e = f'Missing "longitude" field in {config_data[stations]}'
        logger.error(e)
        raise Exception(e)
    
    stop_lon = stations["longitude"].to_list()+new_parents["longitude"].to_list()
    stop_lat = stations["latitude"].to_list()+new_parents["latitude"].to_list()

    logger.debug("Searching for zone_id")

    debut = time.time()

    # Retrieve the zone_id from the nearest station, using a KDTree
    filtered_stops = stops_data.dropna(subset = ["zone_id"])
    zone_ids = []
    for ind, row in stations.iterrows():
        nearest_station = find_nearest_kdtree(row, filtered_stops)
        if nearest_station is None:
            zone_id = 0.0
        else:
            zone_id = nearest_station["zone_id"]
        zone_ids.append(zone_id)

    # For the parent stations, zone_id is NaN
    zone_ids += [NaN]*len(new_parent_stop_names)

    logger.debug(f"Time spend to find zone_id : {time.time()-debut:.2f} s")

    # Create the dataframe containing the new stops
    new_stops_data = pd.DataFrame({
        "stop_id" : new_stops_ids,
        "stop_code" : NaN,
        "stop_name" : stop_names,
        "stop_desc" : NaN,
        "stop_lon" : stop_lon,
        "stop_lat" : stop_lat,
        "zone_id" : zone_ids,
        "stop_url" : NaN,
        "location_type" : [0 if i < len(stations) else 1 for i in range(number_of_new_stops)],
        "parent_station" : all_parent_ids,
        "stop_timezone" : NaN,
        "level_id" : NaN,
        "wheelchair_boarding" : 0,
        "platform_code" : NaN
    })

    # Adding the stop_ids for later use
    stations["stop_id"] = new_stops_ids[:len(stations)]

    # Save stops.txt in the destination folder
    all_stops = pd.concat([stops_data, new_stops_data], ignore_index=True)
    all_stops.to_csv(os.path.join(dest_folder, "stops.txt"), index=False)

    logger.info("Stops created")

    # CALENDAR

    logger.info("Beginning creation of calendar")

    calendar_data = calendar.copy()

    # Get the new id for the unique service to create
    new_service_id = calculate_new_ids(calendar_data, "service_id", 1)[0]

    # Get the starting date and the ending date from the gtfs data
    starting_date, ending_date = define_dates(calendar_data)

    # Add the new service, enabled all days from the starting date to the ending date
    calendar_data.loc[len(calendar_data)] = {
        "service_id" : new_service_id,
        "monday" : 1,
        "tuesday" : 1,
        "wednesday" : 1,
        "thursday" : 1,
        "friday" : 1,
        "saturday" : 1,
        "sunday" : 1,
        "start_date" : starting_date,
        "end_date" : ending_date
    }

    # Save calendar.txt in the destination_folder
    calendar_data.to_csv(os.path.join(dest_folder, "calendar.txt"), index = False)

    logger.info("Calendar created")

    # STOP_TIMES

    logger.info("Beginning creation of stop times and trips")

    # Get the new ids for each route (one for the outward journey and one for the return journey
    new_trips_id = calculate_new_ids(trips_data, "trip_id", len(new_routes_ids)*2)
    new_trips_iter = iter(new_trips_id)
    
    cleared_data_type = {}

    for route_type in set(lines["type"].to_list()): # Do not acknowledge the difference between RER, TER, Transilien,... TODO

        routes_one_type = routes[routes.route_type == route_type]
        trips_one_type = trips_data[trips_data.route_id.isin(routes_one_type.route_id)]
        calendar_one_type = calendar[calendar.service_id.isin(trips_one_type.service_id)]
        calendar_dates_one_type = calendar_dates[calendar_dates.service_id.isin(calendar_one_type.service_id)]
            
        best_date = find_best_date(calendar_dates, weekend=False) # Could have the date in config.json ? TODO
        calendar_one_type = clear_dates(calendar_one_type.copy(), calendar_dates_one_type.copy(), best_date)

        trips_one_type = trips_one_type[trips_one_type.service_id.isin(calendar_one_type.service_id)]
        
        all_trains_count, trains_count_by_line = analyse_freq(routes_one_type.copy(), trips_one_type.copy(), stop_times_data.copy())

        all_trains_count /= len(trains_count_by_line)
        freq_max = all_trains_count.max()
        all_trains_count /= freq_max

        # plot_freq_data(all_trains_count, title=f"Ratio Moyen des Départs Horaires (date : {best_date})", xlabel = "Heure", ylabel = "Ratio", file_path=r"plots\fr\ratio_moyen.png")
        # plot_freq_data(all_trains_count, title=f"Average Hourly Departure Ratio (date: {best_date})", xlabel="Hour", ylabel="Ratio", file_path=r"plots\en\mean_ratio.png")
        # plot_freq_data(trains_count_by_line, title = f"Nombre de Départs Horaires par ligne (date : {best_date})", xlabel="Heure", ylabel="Nombre de Départs", file_path=r"plots\fr\nb_departs_par_ligne")
        # plot_freq_data(trains_count_by_line, title=f"Number of Hourly Departures per Line (date: {best_date})", xlabel="Hour", ylabel="Number of Departures", file_path=r"plots\en\nb_departure_per_line")

        cleared_data_type[route_type] = {
            "mean" : all_trains_count,
            "freq_max" : freq_max,
            "by_line" : trains_count_by_line
            }

    all_data = {
        "arrival_stations" : [],
        "arrival_times" : [],
        "arrival_order" : [],
        "new_trips_ids" : [],
        "trips_ids" : [],
        "routes_ids" : [],
        "direction" : [],
    }

    debut = time.time()

    # Create data for each line for trips.txt
    for ind, row in lines.iterrows():

        line = row["name"]
        route_type = row["type"]
        # Retrieve the journey for the line
        journey_line = journey_time[journey_time["line"] == line]

        freq_max = cleared_data_type[route_type]["freq_max"].item() # By default, the frequency for a line is the maximum frequency of all lines of the same type
        
        #Get the frequency 
        if "frequency" in lines :
            if not pd.isna(row["frequency"]):
                freq_max = lines[lines.name == line]["frequency"].item()

        other_lines_ratio = cleared_data_type[route_type]["mean"]

        freq_by_hour = other_lines_ratio * freq_max

        # Calculate the number of seconds between the departure of two trains
        nb_sec_between_trains = (60/freq_by_hour)*60

        id = lines[lines.name == line]["route_id"].item()
        outward_trip_id = next(new_trips_iter)
        return_trip_id = next(new_trips_iter)

        # Does not take into account monodirectional journeys TODO
        # What if the journey is not given in order ? TODO
        
        arrival_stations_outward, arrival_times_outward, arrival_order_outward, n_trips_outward = calculate_arrival_times(journey_line, nb_sec_between_trains)

        # Create the dataframe for the return trip
        return_df = pd.DataFrame({
            "line" : line,
            "departure" : journey_line["arrival"].to_list()[::-1],
            "arrival" : journey_line["departure"].to_list()[::-1],
            "time" : journey_line["time"].to_list()[::-1]
        })

        arrival_stations_return, arrival_times_return, arrival_order_return, n_trips_return = calculate_arrival_times(return_df, nb_sec_between_trains) #Ne prends pas en considération la différence de fréquence entre l'aller et le retour TODO

        # Find the corresponding ids
        stop_name_to_id = dict(zip(stations["name"], stations["stop_id"]))

        arrival_stations_outward = [stop_name_to_id[station] for station in arrival_stations_outward]
        arrival_stations_return = [stop_name_to_id[station] for station in arrival_stations_return]

        all_data["arrival_stations"] += arrival_stations_outward
        all_data["arrival_stations"] += arrival_stations_return

        all_data["arrival_times"] += arrival_times_outward
        all_data["arrival_times"] += arrival_times_return

        all_data["arrival_order"] += arrival_order_outward
        all_data["arrival_order"] += arrival_order_return

        all_data["trips_ids"] += [outward_trip_id + "_" + str(i) for i in range(n_trips_outward) for _ in range(len(arrival_stations_outward)//n_trips_outward)]
        all_data["trips_ids"] += [return_trip_id + "_" + str(i) for i in range(n_trips_return) for _ in range(len(arrival_stations_return)//n_trips_return)]

        all_data["new_trips_ids"] += [outward_trip_id + "_" + str(i) for i in range(n_trips_outward)]
        all_data["new_trips_ids"] += [return_trip_id + "_" + str(i) for i in range(n_trips_return)]
        all_data["routes_ids"] += [new_routes_data[new_routes_data.route_long_name == str(line)]["route_id"].item() for _ in range(n_trips_outward+n_trips_return)]
        all_data["direction"] += [0 for i in range(n_trips_outward)]
        all_data["direction"] += [1 for i in range(n_trips_return)]

    logger.debug(f"Time spend to create stop times : {time.time() - debut:.2f} s")

    for ind, row in extensions.iterrows():
        trips_ext = trips_data[trips_data.route_id == row["route_id"]]
        stop_times_ext = stop_times_data[stop_times_data.trip_id.isin(trips_ext.trip_id)]
        stops_ext = stops_data[stops_data.stop_id.isin(stop_times_ext.stop_id)]
        journey_time_ext = journey_time[journey_time.line == row["name"]]

        itineraries = journey_time_ext["itinerary"].unique()

        for itinerary in itineraries:

            fork = forks[(forks["line"] == row["name"]) & (forks["itinerary"] == itinerary)]

            journey_time_ext_itinerary = journey_time_ext if pd.isna(itinerary) else journey_time_ext[journey_time_ext["itinerary"] == itinerary]

            try:
                link_id, link_name = find_stop_link(stops_ext, stop_times_ext, trips_ext, journey_time_ext_itinerary)
            except KeyError as e:
                logger.warning(f"No stop match found for line {row['name']}, please consider checking your data.")
                logger.info(f"Line {row['name']} is discarded")
                continue
            except ValueError as e:
                logger.warning(f"Too many matches found for line {row['name']}. This 'extension' (or a part of it) already exists in your dataset.")
                logger.info(f"Line {row['name']} is discarded")
                continue

            # We add the extension before and after the existing trips, we're not changing the existing stop_times (only the stop sequence)
            if fork.empty : 
                stop_times_data = add_extension(stop_times_data, stop_times_ext, stops_ext, journey_time_ext_itinerary, stations, link_id, link_name)
            else :
                stop_times_ext = stop_times_ext.merge(trips_ext[["trip_id", "direction_id"]], on = "trip_id") ### TO DELETE
                frequency = forks["frequency"].item()
                stop_times_data = add_fork(stop_times_data, stop_times_ext, stops_ext, journey_time_ext_itinerary, stations, link_id, link_name, frequency)
    # Create the dataframe containing the new stop_times
    new_stop_times = pd.DataFrame({
        "trip_id" : all_data["trips_ids"],
        "arrival_time" : all_data["arrival_times"],
        "departure_time" : all_data["arrival_times"],
        "start_pickup_drop_off_window" : NaN,
        "end_pickup_drop_off_window" : NaN,
        "stop_id" : all_data["arrival_stations"],
        "stop_sequence" : all_data["arrival_order"],
        "pickup_type" : [0 for _ in range(len(all_data["trips_ids"]))],
        "drop_off_type" : [0 for _ in range(len(all_data["trips_ids"]))],
        "local_zone_id" : NaN,
        "stop_headsign" : NaN,
        "timepoint" : [1 for _ in range(len(all_data["trips_ids"]))],
        "pickup_booking_rule_id" : NaN,
        "drop_off_booking_rule_id" : NaN
    })

    debut = time.time()
    all_stop_times = pd.concat([stop_times_data, new_stop_times],ignore_index=True)
    logger.debug(f"Time spend to concatenate stop_times : {time.time()-debut:.2f} s")

    debut = time.time()

    #Save stop_times.txt, we use polars to gain 20 seconds
    all_stop_times = pl.from_pandas(all_stop_times)
    all_stop_times.write_csv(os.path.join(dest_folder, "stop_times.txt"))

    logger.debug(f"Time spend to save stop_times.txt : {time.time() - debut:.2f} s")

    logger.info("Stop times created")

    # TRIPS
    
    n_trips = len(all_data["new_trips_ids"])

    new_trips_data = pd.DataFrame({
        "route_id" :    all_data["routes_ids"],
        "service_id" : [new_service_id for i in range(n_trips)],
        "trip_id" : all_data["new_trips_ids"],
        "trip_headsign" : NaN,
        "trip_short_name" : NaN,
        "direction_id" : all_data["direction"],
        "block_id" : NaN,
        "shape_id" : NaN,
        "wheelchair_accessible" : 0,
        "bikes_allowed" : 0
    })

    all_trips_data = pd.concat([trips_data, new_trips_data], ignore_index=True)
    all_trips_data.to_csv(os.path.join(dest_folder, "trips.txt"), index=False)

    logger.info("Trips created")
    logger.info("Done")

    logger.debug(f"Time spend to execute the program : {time.time()-beginning:.2f} s")
