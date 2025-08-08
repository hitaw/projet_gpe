import pandas as pd
import geopandas as gpd
import time
import re
import unicodedata
from geopy.distance import geodesic
from scipy.spatial import KDTree
import numpy as np
import warnings
import logging
import inspect
import math

logger = logging.getLogger("__main__")

def read_data(file_path : str, sep=",") -> pd.DataFrame:
    """
    Reads data from a file and returns a DataFrame.

    :param file_path: The path to the file to be read. If the file is a shapefile (.shp),
                     it will be read as a GeoDataFrame and converted to a DataFrame with
                     latitude and longitude columns. Otherwise, it will be read as a CSV file.
    :param sep: The delimiter used to separate values in the CSV file. Default is a comma (,).
    :return: A DataFrame containing the data from the file.
    """

    if file_path.endswith(".shp"):
        gdf = gpd.read_file(file_path)
        gdf = gdf.to_crs(epsg=4326)
        gdf['latitude'] = gdf['geometry'].apply(lambda geom: geom.y)
        gdf['longitude'] = gdf['geometry'].apply(lambda geom: geom.x)
        df = pd.DataFrame(gdf.drop(columns='geometry'))
    else:
        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            df = pd.read_csv(file_path, sep=sep)

            for w in warning_list:

                frame = inspect.currentframe()
                func_name = inspect.getframeinfo(frame).function
                logger.warning(f"{__name__}:{func_name}:{w.message}")
    
    return df

def convert_int(string : str) -> (int | None):
    """
    Extracts and converts the final digits from a string to an integer.

    :param string: The input string from which to extract trailing digits.
    :return: An integer formed by the trailing digits of the input string.
             If no digits are found, returns None.
    """
    num = ""

    for c in reversed(string):
        if c.isdigit():
            num = c+num
        else:
            break

    return int(num) if num else None

def remove_last_substring(string : str, substring : str) -> str:
    """
    Removes the last occurrence of a specified substring from a string.

    :param string: The original string from which to remove the substring.
    :param substring: The substring to be removed from the original string.
    :return: A new string with the last occurrence of the substring removed.
             If the substring is not found, returns the original string unchanged.
    """
    index = string.rfind(substring)
    if index != -1:
        return string[:index]
    return string

def calculate_new_ids(df : pd.DataFrame, col : str, nb : int) -> list[str]:
    """
    Generates a list of new IDs based on the highest existing ID in a DataFrame column.

    :param df: The DataFrame containing the IDs.
    :param col: The name of the column containing the IDs.
    :param nb: The number of new IDs to generate.
    :return: A list of new IDs, incremented from the highest existing ID.
    """

    if col not in df:
        e = f'{col} does not exist in the current dataframe'
        logger.error(e)
        raise Exception(e)
    
    df_ids = df[col].to_list()
    df_ids.sort()

    last_id = df_ids[-1]

    new_id = convert_int(last_id)
    last_id = remove_last_substring(last_id, str(new_id))

    if not new_id:
        new_id = 0
    
    new_ids = [last_id + str(new_id + i) for i in range(1, nb+1)]

    return new_ids

def define_dates(calendar_df : pd.DataFrame) -> tuple[int]:
    """
    Determines the earliest and latest dates from a DataFrame containing start and end dates.

    :param calendar_df: The DataFrame containing 'start_date' and 'end_date' columns.
    :return: A tuple containing the earliest date and the latest date from the DataFrame.
    """

    dates = calendar_df["start_date"].to_list() + calendar_df["end_date"].to_list()
    dates.sort()

    return dates[0], dates[len(dates)-1]    
    
def find_text_color(hex_color : str) -> str:
    """
    Determines the optimal text color (black or white) for a given background color in hexadecimal format.

    :param hex_color: A string representing the background color in hexadecimal format (e.g., "RRGGBB").
    :return: A string representing the optimal text color in hexadecimal format ("000000" for black, "FFFFFF" for white).
    """

    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)

    lum = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    #print(f"\033[48;2;{r};{g};{b}m{' ' * 10}\033[0m")

    return "000000" if lum > 0.5 else "FFFFFF"

def time_to_seconds(time_str : str) -> int:
    """
    Converts a time string in the format "HH:MM:SS" to the total number of seconds.

    :param time_str: A string representing time in the format "HH:MM:SS".
    :return: An integer representing the total number of seconds.
    """

    h, m, s = map(int, time_str.split(":"))
    return h * 3600 + m * 60 + s

def seconds_to_time(seconds : int) -> str:
    """
    Converts a total number of seconds into a time string in the format "HH:MM:SS".

    :param seconds: An integer representing the total number of seconds.
    :return: A string representing the time in the format "HH:MM:SS".
    """
    h = seconds//3600
    m = (seconds % 3600)//60
    s = seconds % 60
    return h, f"{int(h):02}:{int(m):02}:{int(s):02}"

def calculate_arrival_times(df : pd.DataFrame, freq : pd.DataFrame) -> tuple[list[str], list[str], list[int], int]:
    """
    Calculate arrival times for all trips between start_time and end_time.

    :params df: DataFrame containing the stops and time in seconds between them.
    :params freq: Number of seconds between two departures from both terminus.
    :return: Lists of arrival stations, times, and order for all trips.
    """

    arrival_stations = []
    arrival_times = []
    arrival_order = []

    start_hour = freq.index[0]
    end_hour = freq.index[-1]

    start_seconds = start_hour * 3600
    end_seconds = (end_hour + 1) * 3600

    # Calculate the base arrival times for the first trip
    base_arrival_times = []
    current_seconds = start_seconds
    for index, row in df.iterrows():
        current_seconds += row["time"]
        base_arrival_times.append(current_seconds)

    # Generate all trips
    current_seconds = start_seconds
    while current_seconds <= end_seconds:
        # Add the first stop with the current start time
        current_hour, initial_arrival_time = seconds_to_time(current_seconds)
        freq_hour = freq.loc[current_hour, "count"] 
        if freq_hour == float("inf"):
            current_seconds += 3600
            continue
        arrival_times.append(initial_arrival_time)
        arrival_stations.append(df.iloc[0]["departure"])
        arrival_order.append(0)

        # Add the rest of the stops
        for i in range(len(base_arrival_times)):
            trip_time = current_seconds + (base_arrival_times[i] - start_seconds)
            arrival_times.append(seconds_to_time(trip_time)[1])
            arrival_stations.append(df.iloc[i]["arrival"])
            arrival_order.append(i + 1)

        # Move to the next departure time
        current_seconds += min(freq_hour, 3600) #We do not want to skip an hour if the freq is < 1 train per hour

    return arrival_stations, arrival_times, arrival_order, sum(1 for elem in arrival_order if elem == 0)

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

def find_nearest(stop : pd.Series, stops_df : pd.DataFrame) -> (pd.Series | None) :
    """
    NOT USED
    
    Finds the nearest stop from a DataFrame of stops based on geographical coordinates.

    :param stop: A Series containing the 'latitude' and 'longitude' of the stop to compare.
    :param stops_df: A DataFrame containing stops with 'stop_lat' and 'stop_lon' columns representing their coordinates.
    :return: A Series representing the nearest stop from the DataFrame.
    """

    stop_coords = (stop["latitude"], stop["longitude"])
    nearest_stop = None
    min_dist = float("inf")

    for ind, row in stops_df.iterrows():
        
        station_coords = (row["stop_lat"], row["stop_lon"])
        dist = geodesic(stop_coords, station_coords).meters

        if dist < min_dist:
            min_dist = dist
            nearest_stop = row
    
    return nearest_stop

def find_nearest_kdtree(stop : pd.Series, stops_df : pd.DataFrame) -> pd.Series:
    """
    Finds the nearest stop from a DataFrame of stops using a KDTree for efficient spatial searching.

    :param stop: A Series containing the 'latitude' and 'longitude' of the stop to compare.
    :param stops_df: A DataFrame containing stops with 'stop_lat' and 'stop_lon' columns representing their coordinates.
    :return: A Series representing the nearest stop from the DataFrame.
    """

    stops_df_coords = stops_df[["stop_lat", "stop_lon"]].values
    stops_tree = KDTree(stops_df_coords)

    stop_coords = np.array([stop["latitude"], stop["longitude"]])
    if pd.isna(stop_coords[0]) or pd.isna(stop_coords[1]):
        e = "Latitude et longitude manquantes"
        logger.debug(e)
        return None
    
    dist, ind = stops_tree.query(stop_coords)

    nearest_station = stops_df.iloc[ind]

    return nearest_station

def haversine(p1, p2):
    R = 6371.0

    lat1 = math.radians(p1[0])
    lon1 = math.radians(p1[1])
    lat2 = math.radians(p2[0])
    lon2 = math.radians(p2[1])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c