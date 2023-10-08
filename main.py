import requests
import datetime
import sqlite3
import json
import pandas as pd
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource

conn = sqlite3.connect('NASA_NEO.db')
cursor = conn.cursor()


def delta():
    try:
        with open('last_executed.txt') as f:
            last_execution_date = datetime.datetime.strptime(f.read(), '%Y-%m-%d').date()
    except FileNotFoundError:
        last_execution_date = (datetime.date.today() - datetime.timedelta(days=5))

    delta = (datetime.date.today() - last_execution_date).days

    return delta


def get_links(delta):
    if delta == 0:
        return []

    links = []

    def build_link(x, y):
        end_date = x.strftime('%Y-%m-%d')
        start_date = (datetime.date.today() - datetime.timedelta(days=y)).strftime('%Y-%m-%d')
        link = f'https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key=Gt87ibmZefPpnhl8gfz5gWWiTuftebq6IgJBFNdQ'
        links.append(link)

    if delta % 7 != 0:
        build_link(datetime.date.today(), delta%7-1)

    for x in range(delta%7, delta, 7):
        build_link((datetime.date.today() - datetime.timedelta(days=x)), x+6)

    return links


def extract_load(links):

    for link in links:

        response = requests.get(link)
        data = response.json()
        neo_data = data['near_earth_objects']

        cursor.executescript('''

             CREATE TABLE IF NOT EXISTS neo (
                            date_added datetime,
                            data json
            )
        ''')

        for date, neo_list in neo_data.items():
            for neo in neo_list:
                cursor.execute('INSERT INTO neo VALUES (?, ?)',
                               (datetime.datetime.today(), json.dumps(neo)))
        conn.commit()

        with open('last_executed.txt', 'w') as f:
            f.write(str(datetime.date.today()))


def transform():
    df = pd.read_sql_query('''SELECT * FROM neo''', conn)

    df['data'] = df['data'].apply(json.loads)

    json_df = pd.json_normalize(df['data'])

    for n in json_df.index.values:
        json_df['close_approach_data'][n] = {k: v for element in json_df['close_approach_data'][n]
                                             for k, v in element.items()}

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
        'estimated_diameter.meters.estimated_diameter_min': 'estimated_diameter_meters_min',
        'estimated_diameter.meters.estimated_diameter_max': 'estimated_diameter_meters_max',
        'relative_velocity.kilometers_per_second': 'relative_velocity_kilometers_per_second',
        'relative_velocity.kilometers_per_hour': 'relative_velocity_kilometers_per_hour',
        'miss_distance.astronomical': 'miss_distance_astronomical',
        'miss_distance.lunar': 'miss_distance_lunar',
        'miss_distance.kilometers': 'miss_distance_kilometers',
        })

    cols = ['relative_velocity_kilometers_per_second',
        'relative_velocity_kilometers_per_hour',
        'miss_distance_astronomical',
        'miss_distance_lunar',
        'miss_distance_kilometers'
        ]

    df[cols] = df[cols].astype('float')
    
    df['close_approach_date_full'] = df['close_approach_date_full'].astype('datetime64[ns]')

    df['estimated_diameter_meters_mean'] = df[['estimated_diameter_meters_max',
                                               'estimated_diameter_meters_min']].mean(axis=1)
    
    return df

def plot1(df):
    data = df
    source = ColumnDataSource(data)

    p = figure(x_axis_label=r'\[\text{ relative velocity }kms^{-1}\]', y_axis_label=r'\[\text{ estimated diameter min/max }(m)\]')

    p.circle(x='relative_velocity_kilometers_per_second', y='estimated_diameter_meters_max', source=source, fill_color='red')
    p.circle(x='relative_velocity_kilometers_per_second', y='estimated_diameter_meters_min', source=source, fill_color='green')

    show(p)

def plot2(df):
    data = df
    source = ColumnDataSource(data)

    p = figure(x_axis_label=r'\[\text{ mean estimated diameter }(m)\]', y_axis_label=r'\[\text{ absolute magnitude }\]')

    p.circle(x='estimated_diameter_meters_mean', y='absolute_magnitude_h', source=source)

    show(p)
    

delta = delta()
links = get_links(delta)

if __name__ == '__main__':
    extract_load(links)
    df = transform()
    plot1(df)
    plot2(df)
