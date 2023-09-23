import requests
import datetime
import pprint
import sqlite3

link = "https://api.nasa.gov/neo/rest/v1/feed?start_date=LAST_WEEK&end_date=&api_key=Gt87ibmZefPpnhl8gfz5gWWiTuftebq6IgJBFNdQ"

#now = datetime.date.today()
#today = now.strftime("%Y-%m-%d")
last_week = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")

#link = link.replace("TODAY", today)
link = link.replace("LAST_WEEK", last_week)

response = requests.get(link)
data = response.json()
neo_data = data["near_earth_objects"]

#pprint.pprint(data)
#pprint.pprint(neo_data)

conn = sqlite3.connect("test2.db")
cursor = conn.cursor()

cursor.executescript('''

    DROP TABLE IF EXISTS neo;

    CREATE TABLE neo (
                     id TEXT PRIMARY KEY,
                     name TEXT,
                     potentially_hazardous BOOLEAN,
                     sentry_object BOOLEAN,
                     close_approach_date TEXT,
                     miss_distance_km TEXT,
                     orbiting_body TEXT,
                     relative_velocity_kps TEXT,
                     estimated_diameter_max_m TEXT,
                     estimated_diameter_min_m TEXT
    )
''')

for date, neo_list in neo_data.items():
    for neo in neo_list:
        cursor.execute('''
                    INSERT INTO neo VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        neo["id"],
                        neo["name"],
                        neo["is_potentially_hazardous_asteroid"],
                        neo["is_sentry_object"],
                        date,
                        neo["miss_distance"],
                        neo["orbiting_body"],
                        neo["relative_velocity"["kilometres_per_second"]],
                        neo["estimated_diameter"["metres"["estimated_diameter_max"]]],
                        neo["estimated_diameter"["metres"["estimated_diameter_min"]]]
                    ))
conn.commit()