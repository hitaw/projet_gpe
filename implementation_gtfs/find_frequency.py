from datetime import date
from collections import Counter
import pandas as pd
import logging
import matplotlib.pyplot as plt

logger = logging.getLogger("__main__")

WEEK = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

# DOES NOT ACKOLEDWE DATES WHERE THERE IS NO PERTURBATION #TODO
def find_best_date(calendar_dates : pd.DataFrame, weekend : bool = False) -> date:

    dates = calendar_dates.date.to_list()
    dates_datetime = [date.fromisoformat(str(d)) for d in dates] 

    best_date = None
    best_value = float("inf")

    for day in range(7 if weekend else 5):

        dates_day = [d for d in dates_datetime if d.weekday() == day]
        dates_counter = Counter(dates_day) 

        if not dates_counter:
            continue

        min_date = min(dates_counter, key = dates_counter.get)
        min_value = dates_counter[min_date]

        if min_value < best_value:
            best_value = min_value
            best_date = min_date

    return best_date

def clear_dates(calendar : pd.DataFrame, calendar_dates : pd.DataFrame, best_date : date) -> tuple[pd.DataFrame]:

    week_day = best_date.weekday()
    best_date = int(best_date.strftime("%Y%m%d"))

    calendar_dates = calendar_dates[calendar_dates.date == best_date]

    calendar = calendar[calendar[WEEK[week_day]] == 1]
    indexes = []

    for ind, row in calendar.iterrows():
        if not (best_date in range(row["start_date"], row["end_date"]+1)):
            indexes.append(ind)

    calendar.drop(indexes, inplace = True)

    calendar = pd.merge(calendar, calendar_dates[["service_id", "exception_type"]], on="service_id", how="outer")
    calendar = calendar[calendar.exception_type != 2]
    calendar.drop(columns="exception_type", inplace=True)

    return calendar

def analyse_freq(routes : pd.DataFrame, trips : pd.DataFrame, stop_times : pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    
    lines = routes.route_long_name.to_list()

    all_trains_count = pd.DataFrame(index=range(26), columns=['count']).astype(float).fillna(0)
    trains_count_by_line = {}

    for line in lines:

        route_line = routes[routes.route_long_name == line]
        trips_line = trips[trips.route_id.isin(route_line.route_id)]

        stop_times_line = stop_times[stop_times.trip_id.isin(trips_line.trip_id)]
        stop_times_line = stop_times_line[stop_times_line.stop_sequence == 1]
        
        stop_ids = stop_times_line.stop_id.to_list()

        if len(stop_ids) == 0:
            logger.warning(f"Line {line} does not have any stops")
            continue

        most_frequent_stop = None
        max_count = 0

        for stop_id in set(stop_ids):
            count = stop_ids.count(stop_id)
            if count > max_count:
                max_count = count
                most_frequent_stop = stop_id
        
        stop_times_line = stop_times_line[stop_times_line.stop_id == most_frequent_stop]

        stop_times_line = stop_times_line.sort_values(by="departure_time")
        stop_times_line["hour"] = stop_times_line["departure_time"].apply(lambda x: int(str(x).split(":")[0]))

        train_count = stop_times_line["hour"].value_counts().sort_index()
        train_count = train_count.reindex(range(train_count.index[-1]+1), fill_value=0)

        all_trains_count["count"] = all_trains_count["count"].add(train_count, fill_value=0)
        trains_count_by_line[line] = train_count

    return all_trains_count, trains_count_by_line

def plot_freq_data(data : pd.DataFrame | dict, title : str = "", xlabel : str = "", ylabel : str = "", file_path : str = None):
    
    plt.figure(figsize=(12,6))
    if isinstance(data, dict):
        xticks = 0
        for line, counts in data.items():
            plt.plot(counts.index, counts.values, marker = "o", label=f"Line {line}")
            xticks = max(len(counts), xticks)
        plt.legend()
    else:
        plt.plot(data.index, data["count"], marker = "o")
        xticks = len(data)

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(range(xticks))
    plt.grid(True)

    if file_path:
        plt.savefig(file_path)
        logger.info(f"Plot saved in {file_path}")
    plt.show()

