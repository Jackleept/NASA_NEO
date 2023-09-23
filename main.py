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
"""
Look into f-strings for the above instead of the .replace
link = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={last_week}&end_date=&api_key={api_key}"
"""

"""Move everything between here to the commit loop into a function. And then call it at the end of the file
Utilise:
if __name__ = '__main__':
    run_x()

This is done to stop code running if you ever try to import from this file to others. Code then only runs
when you have a function call
"""
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


"""
I'd look at reading the whole table in as is and then transforming the JSON data col with pandas
You can look at lambda functions for that with a pd.assign
Here's an example with pandas .apply: 
https://saturncloud.io/blog/how-to-access-a-json-column-with-pandas/#:~:text=To%20extract%20data%20from%20a,along%20with%20a%20lambda%20function.&text=In%20this%20example%2C%20we%20are%20extracting%20data%20from%20a%20JSON,key'%20in%20the%20JSON%20data.
I'd go for the assign route, because it allows for method chaining. Which you did in the Previse interview.

"""
df = pd.read_sql_query('''
                       select json_extract(data, "$.name") as name,
                       json_extract(data, "$.id") as ID,
                       json_extract(data, "$.is_potentially_hazardous_asteroid") as potentially_hazardous,
                       json_extract((json_extract(data, "$.close_approach_data"), "$.close_approach_date_full") as close_approach_date
                       from neo
                       order by close_approach_date;
                       ''', conn)

df["potentially_hazardous"] = df["potentially_hazardous"].astype(bool)

df_hazardous = df[df["potentially_hazardous"]==True]

df.to_clipboard()