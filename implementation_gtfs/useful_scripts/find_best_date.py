import os
import pandas as pd
from collections import Counter
from datetime import date

calendar = pd.read_csv(os.path.join("IDFM-gtfs", "calendar.txt"))
calendar_dates = pd.read_csv(os.path.join("IDFM-gtfs", "calendar_dates.txt"))
trips = pd.read_csv(os.path.join("IDFM-gtfs", "trips.txt"))
routes = pd.read_csv(os.path.join("IDFM-gtfs", "routes.txt"))

routes = routes[routes.route_type == 1]
trips = trips[trips.route_id.isin(routes.route_id)]
calendar_dates = calendar_dates[calendar_dates.service_id.isin(trips.service_id)]

calendar_dates = calendar_dates.sort_values(by="date")

dates = calendar_dates.date.to_list()
dates = [str(d) for d in dates]
dates_datatime = [date.fromisoformat(d) for d in dates]

weekdays = [d.weekday() for d in dates_datatime]
week = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
min_week = float("inf")
min_end = float("inf")
weekday = 0
date_weekday = None
weekend = 0
date_weekend = None
for jour in range(7):
    dates_j = [dates_datatime[i] for i in range(len(dates_datatime)) if weekdays[i] == jour]
    date_counts = Counter(dates_j)

    min_date = min(date_counts, key=date_counts.get)
    min_date_count = date_counts[min_date]

    if jour <5:
        if min_date_count < min_week:
            min_week = min_date_count
            weekday = jour
            date_weekday = min_date
    else:
        if min_date_count < min_end:
            min_end = min_date_count
            weekend = jour
            date_weekend = min_date

print(f"Semaine, meilleur jour : {week[weekday]} {date_weekday}")
print(f"Weekend meilleur jour : {week[weekend]} {date_weekend}")