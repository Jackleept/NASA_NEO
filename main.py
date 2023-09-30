import requests
import datetime
import sqlite3
import json
import pandas as pd

conn = sqlite3.connect('NASA_NEO.db')

def extract():
    try:
        with open('last_executed.txt') as f:
            last_execution_date = datetime.datetime.strptime(f.read(), '%Y-%m-%d').date()
    except FileNotFoundError:
            last_execution_date = (datetime.date.today() - datetime.timedelta(days=365))

    delta = (datetime.date.today() - last_execution_date).days

    links_a = []
    links_b = []

    if delta % 7 != 0:
        end_date = datetime.date.today()
        start_date = (end_date - datetime.timedelta(days=delta%7)).strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')
        link = f'https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key=Gt87ibmZefPpnhl8gfz5gWWiTuftebq6IgJBFNdQ'
        links_a.append(link)

    for x in range(delta%7, delta+7, 8):
        end_date = (datetime.date.today() - datetime.timedelta(days=x+1))
        start_date = (end_date - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')
        link = f'https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key=Gt87ibmZefPpnhl8gfz5gWWiTuftebq6IgJBFNdQ'
        links_b.append(link)

    global links 
    links = links_a + links_b

def load_data():
    
    global links

    for link in links:
    
        response = requests.get(link)
        data = response.json()
        neo_data = data['near_earth_objects']

        cursor = conn.cursor()

        cursor.executescript('''

             CREATE TABLE IF NOT EXISTS neo (
                            id text,
                            data json
            )
        ''')

        for date, neo_list in neo_data.items():
            for neo in neo_list:
                cursor.execute('INSERT INTO neo VALUES (?, ?)',
                            (neo['id'], json.dumps(neo)))
        conn.commit()

        with open('last_executed.txt', 'w') as f:
            f.write(str(datetime.date.today()))

def transform():
    df = pd.read_sql_query('''SELECT data FROM neo''', conn)

    df['data'] = df['data'].apply(json.loads)

    json_df = pd.json_normalize(df['data'])

    for n in json_df.index.values:
        json_df['close_approach_data'][n] = {k:v for element in json_df['close_approach_data'][n] for k,v in element.items()}

    cad_df = pd.json_normalize(json_df['close_approach_data'])

    df = pd.concat([json_df, cad_df, df], axis=1)

    df = df.drop(['neo_reference_id', 'close_approach_data', 'links.self',
        'estimated_diameter.kilometers.estimated_diameter_min',
        'estimated_diameter.kilometers.estimated_diameter_max',
        'estimated_diameter.miles.estimated_diameter_min',
        'estimated_diameter.miles.estimated_diameter_max',
        'estimated_diameter.feet.estimated_diameter_min',
        'estimated_diameter.feet.estimated_diameter_max',
        'close_approach_date', 'epoch_date_close_approach',
        'relative_velocity.miles_per_hour',
        'miss_distance.miles'], axis=1
        )
    
    df = df.rename(columns={
    'estimated_diameter.meters.estimated_diameter_min': 'estimated_diameter.meters_min',
    'estimated_diameter.meters.estimated_diameter_max': 'estimated_diameter.meters_max'
    })

    print(df)
    df.info()

    print(df.tail(5))

if __name__ == '__main__':
    extract(), load_data(), transform()
"""
Improvements
- Productionisation considerations
    - Update table rather than drop each run
    - add created_at field (useful for upserting)
    - generate loop of weeks to collect
    - assuming a weekly run, make your data collection date aware
        - So if I ran this on Sept 24th, and then on 25th. I would only want to collect new data for the 25th.
        - Without this, you risk scraping duplicate data when inserting.
- Explore some visualisations

Summary of a project:
- Data ingestion from an API
- Transform that data and save it
- Visualise the data. Couple of graphs
- Have the ability to be updating the saved data as if it was being run daily
- Have the ability to refresh the data. Maybe load the last year worth of data?
"""