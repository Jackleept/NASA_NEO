import requests
import datetime
import pprint
import sqlite3
import pandas

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
                     close_approach_data JSON,
                     estimated_diameter JSON
    )
''')

for date, neo_list in neo_data.items():
    for neo in neo_list:
        cursor.execute('''
                    INSERT INTO neo VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        neo['id'],
                        neo['name'],
                        neo['is_potentially_hazardous_asteroid'],
                        neo['is_sentry_object'],
                        neo['close_approach_data'],
                        neo['estimated_diameter']
                    ))
conn.commit()

df = pd.read_sql_query('''SELECT * FROM neo''')

print(df.head(3))