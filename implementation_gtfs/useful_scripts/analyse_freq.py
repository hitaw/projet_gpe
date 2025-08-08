import os
import pandas as pd
from collections import Counter
from datetime import date
import matplotlib.pyplot as plt
import time

gtfs_folder = r"data\IDFM-gtfs"
gtfs_folder = r"GTFS_versions\GTFS_completed"
files = ["routes.txt", "trips.txt", "stop_times.txt", "calendar.txt", "calendar_dates.txt", "agency.txt"]

files = [os.path.join(gtfs_folder, f) for f in files]

routes = pd.read_csv(files[0])
trips = pd.read_csv(files[1])
stop_times = pd.read_csv(files[2])
calendar = pd.read_csv(files[3])
calendar_dates = pd.read_csv(files[4])
agency = pd.read_csv(files[5])

deb = time.time()
routes = routes[routes.route_type == 1] #On ne sélectionne que les métros
# agency = agency[agency.agency_name == "TER"]
routes = routes[routes.agency_id.isin(agency.agency_id)]
lines = routes.route_long_name.to_list()

trips = trips[trips.route_id.isin(routes.route_id)]

calendar = calendar[calendar.service_id.isin(trips.service_id)]
calendar_dates = calendar_dates[calendar_dates.service_id.isin(trips.service_id)]

calendar_dates_test = calendar_dates.sort_values(by="date")

dates = calendar_dates_test.date.to_list()
dates = calendar_dates.date.to_list()
dates = [str(d) for d in dates]
dates_datatime = [date.fromisoformat(d) for d in dates]

weekdays = [d.weekday() for d in dates_datatime]
week = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
all_min_date = None
all_min_value = float("inf")
weekday = None
for jour in range(5):
    dates_j = [dates_datatime[i] for i in range(len(dates_datatime)) if weekdays[i] == jour]
    date_counts = Counter(dates_j)

    min_date = min(date_counts, key=date_counts.get)
    min_date_count = date_counts[min_date]
    
    if min_date_count<all_min_value:
        all_min_value = min_date_count
        all_min_date = min_date
        weekday = week[jour]

min_date = int(all_min_date.strftime('%Y%m%d'))
min_date = 20250310
calendar_dates = calendar_dates[calendar_dates.date == min_date]

calendar = calendar[calendar[weekday] == 1]

indices = []
iterr = calendar.iterrows()
for ind, row in iterr:
    if not (min_date in range(row["start_date"], row["end_date"]+1)):
        indices.append(ind)

calendar.drop(indices, inplace=True)
trips = trips[(trips.service_id.isin(calendar.service_id)) | (trips.service_id.isin(calendar_dates.service_id))]
# print(calendar_dates)

col = ["start_pickup_drop_off_window", "end_pickup_drop_off_window", "stop_headsign", "timepoint", "pickup_booking_rule_id", "drop_off_booking_rule_id", "pickup_type", "drop_off_type", "local_zone_id"]

stop_times = stop_times.drop(columns=col)

all_train_counts = pd.DataFrame(index=range(26), columns=['count']).astype(float).fillna(0)
train_counts_by_line = {}

ratio_by_line = {}
all_ratio = pd.DataFrame(index=range(26), columns=['count']).astype(float).fillna(0)

for line in lines:
    if line != "3B" and line != "7B" and int(line) >= 15:
        route_line = routes[routes.route_long_name == line]
        trips_line = trips[trips.route_id.isin(route_line.route_id)]

        stop_times_line = stop_times[stop_times.trip_id.isin(trips_line.trip_id)]
        stop_times_line = stop_times_line[stop_times_line.stop_sequence == 1]
        
        stop_ids = stop_times_line.stop_id.to_list()
        if len(stop_ids) == 0:
            print(line)
            continue

        more_freq = None
        max_count = 0

        for stop_id in set(stop_ids):
            count = stop_ids.count(stop_id)
            if count > max_count:
                max_count = count
                more_freq = stop_id

        stop_times_line = stop_times_line[stop_times_line.stop_id == more_freq]
        stop_times_line = stop_times_line.sort_values(by="arrival_time")

        stop_times_line['hour'] = stop_times_line['arrival_time'].apply(lambda x: int(x.split(":")[0]))
                                                                        
        train_counts = stop_times_line['hour'].value_counts().sort_index()
        train_counts = train_counts.reindex(range(train_counts.index[-1]+1), fill_value=0)

        max_trains = train_counts.max()
        hour_max_trains = train_counts.idxmax()

        all_train_counts['count'] = all_train_counts['count'].add(train_counts, fill_value=0)
        train_counts_by_line[line] = train_counts
        ratio_by_line[line] = train_counts/max_trains
        all_ratio["count"] = all_ratio["count"].add(ratio_by_line[line], fill_value=0)

r = True
if r :
    plt.figure(figsize=(12, 6))
    for line, counts in ratio_by_line.items():
        plt.plot(counts.index, counts.values, marker='o', label=f'Ligne {line}')

    plt.title(f'Ratio des Départs Horaires par ligne (date : {min_date})')
    plt.xlabel('Heure')
    plt.ylabel('Ratio')
    plt.xticks(range(26))  # Afficher toutes les heures de la journée
    plt.legend()
    plt.grid(True)
    plt.savefig(r".\plots\fr\gpe\ratio_ligne_gpe.png")
    plt.show()

    plt.figure(figsize=(12, 6))
    plt.plot(all_ratio.index, all_ratio['count']/len(ratio_by_line), marker='o')
    plt.title(f'Ratio Moyen des Départs Horaires pour les lignes du GPE (date : {min_date})')
    plt.xlabel('Heure')
    plt.ylabel('Nombre de trains')
    plt.xticks(range(26))  # Afficher toutes les heures de la journée
    plt.grid(True)
    plt.savefig(r".\plots\fr\gpe\ratio_moyen_gpe.png")
    plt.show()

a = True
if a:
    plt.figure(figsize=(12, 6))
    for line, counts in train_counts_by_line.items():
        plt.plot(counts.index, counts.values, marker='o', label=f'Ligne {line}')

    plt.title(f"Nombre de Départs Horaires par ligne (date : {min_date})")
    plt.xlabel('Heure')
    plt.ylabel('Nombre de trains')
    plt.xticks(range(26))  # Afficher toutes les heures de la journée
    plt.legend()
    plt.grid(True)
    plt.savefig(r".\plots\fr\gpe\nb_ligne_gpe.png")
    plt.show()

    plt.figure(figsize=(12, 6))
    plt.plot(all_train_counts.index, all_train_counts['count']/len(train_counts_by_line), marker='o')
    plt.title(f'Moyenne des Départs Horaires pour les lignes du GPE (date : {min_date})')
    plt.xlabel('Heure')
    plt.ylabel('Nombre de trains')
    plt.xticks(range(26))  # Afficher toutes les heures de la journée
    plt.grid(True)
    plt.savefig(r".\plots\fr\gpe\nb_gpe.png")
    plt.show()

print(time.time()-deb)