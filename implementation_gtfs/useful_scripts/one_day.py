import os
import shutil
import pandas as pd

date = 20250310

gtfs_folder = ".\\GTFS_completed\\"
dest_folder = ".\\GTFS_one_day_only\\"

calendar = pd.read_csv(os.path.join(gtfs_folder, "calendar.txt"))

indices = []

iterr = calendar.iterrows()
for ind, row in iterr:
    if date in range(row["start_date"], row["end_date"]+1):
        calendar.loc[ind, "start_date"] = date
        calendar.loc[ind, "end_date"] = date
    else:
        indices.append(ind)

calendar.drop(indices, inplace=True)

calendar_dates = pd.read_csv(os.path.join(gtfs_folder, "calendar_dates.txt"))

calendar_dates = calendar_dates[calendar_dates["date"] == date]
calendar_dates = calendar_dates[calendar_dates["service_id"].isin(calendar["service_id"])]

trips = pd.read_csv(os.path.join(gtfs_folder, "trips.txt"))

trips = trips[trips.service_id.isin(calendar.service_id)]

stop_times = pd.read_csv(os.path.join(gtfs_folder, "stop_times.txt"))
stop_times = stop_times[stop_times.trip_id.isin(trips.trip_id)]

calendar.to_csv(os.path.join(dest_folder, "calendar.txt"), index=False)
calendar_dates.to_csv(os.path.join(dest_folder, "calendar_dates.txt"), index=False)
trips.to_csv(os.path.join(dest_folder, "trips.txt"), index=False)
stop_times.to_csv(os.path.join(dest_folder, "stop_times.txt"), index=False)

files = ["calendar.txt", "calendar_dates.txt", "trips.txt", "stop_times.txt"]

list_files = os.listdir(gtfs_folder)

for f in list_files:
    if f not in files:
        shutil.copy(os.path.join(gtfs_folder, f), os.path.join(dest_folder, f))
