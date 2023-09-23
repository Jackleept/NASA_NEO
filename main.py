import requests
import datetime
import pprint
import sqlite3
import json
import pandas as pd

link = "https://api.nasa.gov/neo/rest/v1/feed?start_date=LAST_WEEK&end_date=&api_key=Gt87ibmZefPpnhl8gfz5gWWiTuftebq6IgJBFNdQ"

#now = datetime.date.today()
#today = now.strftime("%Y-%m-%d")
last_week = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")

#link = link.replace("TODAY", today)
link = link.replace("LAST_WEEK", last_week)

response = requests.get(link)
data = response.json()
neo_data = data["near_earth_objects"]

pprint.pprint(neo_data)

conn = sqlite3.connect("NASA_NEO.db")
cursor = conn.cursor()

cursor.executescript('''

    DROP TABLE IF EXISTS neo;

    CREATE TABLE neo (
                     id text,
                     data json
    )
''')

for date, neo_list in neo_data.items():
    for neo in neo_list:
        cursor.execute("INSERT INTO neo VALUES (?, ?)",
                    (neo["id"], json.dumps(neo)))
conn.commit()

df = pd.read_sql_query('''
                       select json_extract(data, "$.name") as name,
                       json_extract(data, "$.id") as ID,
                       json_extract(data, "$.is_potentially_hazardous_asteroid") as potentially_hazardous,
                       json_extract(data, "$.close_approach_date_full") as close_approach_date
                       from neo
                       order by close_approach_date
                       limit 3;''', conn)

df.to_clipboard()