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

pprint.pprint(data)

conn = sqlite3.connect("NASA_NEO.db")
cursor = conn.cursor()

cursor.executescript('''

    DROP TABLE IF EXISTS neo;

    CREATE TABLE neo (
                     id varchar(3)
                     data json
    )
''')

