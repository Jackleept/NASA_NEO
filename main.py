import requests
import datetime
import pprint
import sqlite3
import json
import pandas as pd

last_week = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")

link = f'https://api.nasa.gov/neo/rest/v1/feed?start_date={last_week}&end_date=&api_key=Gt87ibmZefPpnhl8gfz5gWWiTuftebq6IgJBFNdQ'
conn = sqlite3.connect('NASA_NEO.db')

def load_data():
    response = requests.get(link)
    data = response.json()
    neo_data = data['near_earth_objects']
  
    pprint.pprint(neo_data)

    # out_file = open('neo_data.json', 'w')
    # json.dump(neo_data, out_file)
    # out_file.close()

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
            cursor.execute('INSERT INTO neo VALUES (?, ?)',
                        (neo['id'], json.dumps(neo)))
    conn.commit()

    # cursor.executescript('''

    #     DROP TABLE IF EXISTS neo;
                         
    #     CREATE TABLE neo (
    #                      data json
    #     )
    # ''')

    # cursor.execute('INSERT INTO neo VALUES (?)',
    #                json.dumps(data))
    # conn.commit()

if __name__ == '__main__':
    load_data()

df = pd.read_sql_query('''SELECT data FROM neo''', conn)

print(df.head(3))

data0 = pd.json_normalize(df['data'])

print(data0)

# df = pd.read_sql_query('''
#                        select json_extract(data, "$.name") as name,
#                        json_extract(data, "$.id") as ID,
#                        json_extract(data, "$.is_potentially_hazardous_asteroid") as potentially_hazardous
#                        from neo;
#                        ''', conn)

# df["potentially_hazardous"] = df["potentially_hazardous"].astype(bool)

# df_hazardous = df[df["potentially_hazardous"]==True]

# df.to_clipboard()