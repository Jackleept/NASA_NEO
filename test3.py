import requests
import datetime
import pprint
import sqlite3
import json

link = "https://api.nasa.gov/neo/rest/v1/feed?start_date=LAST_WEEK&end_date=&api_key=Gt87ibmZefPpnhl8gfz5gWWiTuftebq6IgJBFNdQ"

#now = datetime.date.today()
#today = now.strftime("%Y-%m-%d")
last_week = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")

#link = link.replace("TODAY", today)
link = link.replace("LAST_WEEK", last_week)

response = requests.get(link)
data = response.json()
neo_data = data["near_earth_objects"]

#pprint.pprint(neo_data)

conn = sqlite3.connect("test3.db")
cursor = conn.cursor()

cursor.executescript('''
                     
            DROP TABLE IF EXISTS neo;
                     
            CREATE TABLE IF NOT EXISTS neo (
            id TEXT PRIMARY KEY,
            name TEXT,
            date DATE,
            is_potentially_hazardous BOOLEAN
        )
    ''')

# Iterate through the NEO data and insert it into the database
for date, neo_list in neo_data.items():
    for neo in neo_list:
        cursor.execute('''
            INSERT OR REPLACE INTO neo (id, name, date, is_potentially_hazardous)
            VALUES (?, ?, ?, ?)
        ''', (
            neo['id'],
            neo['name'],
            date,
            neo['is_potentially_hazardous_asteroid']
        ))

# Commit the changes and close the database connection
conn.commit()