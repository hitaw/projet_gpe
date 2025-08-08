import pandas as pd
import os

stop_times = pd.read_csv(".\\GTFS_completed\\stop_times.txt")
trips = pd.read_csv(".\\GTFS_completed\\trips.txt")

print(len(stop_times))
stop_times = stop_times[stop_times.trip_id.isin(trips.trip_id)]
print(len(stop_times))
#stop_times.to_csv(".\\GTFS_completed\\stop_times.txt", index=False)