import pandas as pd
import numpy as np
import dateutil.parser
import os
import os.path as path
import datetime
import cx_Oracle as oracledb
import concurrent.futures
from typing import Set

DATA_PATH = path.join(
    path.dirname(path.realpath(__file__)),
    '../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports'
)

data_files_list = filter(lambda file: file.endswith('.csv'), os.listdir(DATA_PATH))
data_files = list(map(
    lambda filename: path.normpath(path.join(DATA_PATH, filename)),
    data_files_list
))

COLUMN_MAPPING = {
    'Last Update': 'timestamp',
    'Last_Update': 'timestamp',
    'Country/Region': 'country',
    'Country_Region': 'country',
    'Province/State': 'state',
    'Province_State': 'state',
    'Admin2': 'admin2', # county
    'Latitude': 'latitude',
    'Longitude': 'longitude',
    'Lat': 'latitude',
    'Long_': 'longitude',
    'Confirmed': 'confirmed',
    'Deaths': 'deaths',
    'Recovered': 'recovered',
    'Active': 'active',
    'Incidence_Rate': 'incidence',
    'Incident_Rate': 'incidence',
    'Case-Fatality_Ratio': 'case_fatality_ratio',
    'Case_Fatality_Ratio': 'case_fatality_ratio'
}
RESULT_COLUMNS = set([
    'timestamp', 'country', 'state', 'admin2', 'latitude', 'longitude', 'confirmed',
    'deaths', 'recovered', 'active', 'incidence', 'case_fatality_ratio'
])

COUNTRY_MAPPING = {
    ' Azerbaijan': 'Azerbaijan',
    'Bahamas, The': 'Bahamas',
    'Mainland China': 'China',
    'Hong Kong': 'China',
    'Hong Kong SAR': 'China',
    'Macau': 'China',
    'Macau SAR': 'China',
    'Iran (Islamic Republic of)': 'Iran',
    'Taiwan*': 'Taiwan',
    'Taipei and environs': 'Tawian',
    'occupied Palestinian territory': 'State of Palestine', # not too sure about this one
    'West Bank and Gaza': 'State of Palestine',
    'Palestine': 'State of Palestine',
    'Republic of Ireland': 'Ireland',
    'Republic of Korea': 'South Korea',
    'Korea, South': 'South Korea',
    'Republic of Moldova': 'Moldova',
    'Republic of the Congo': 'Congo (Brazzaville)', # yes, there are in fact two Congos
    'Russian Federation': 'Russia',
    'Viet Nam': 'Vietnam',
    'Curacao': 'Netherlands', # counted as part of netherlands
    'Aruba': 'Netherlands',
    'US': 'United States',
    'Puerto Rico': 'United States',
    'Guam': 'United States',
    'Czechia': 'Czech Republic',
    'North Ireland': 'United Kingdom',
    'UK': 'United Kingdom',
    'Faroe Islands': 'Denmark',
    'The Gambia': 'Gambia',
    'Gambia, The': 'Gambia',
    'Greenland': 'Denmark',
    'Channel Islands': 'United Kingdom', # channel islands were moved into UK
    'Jersey': 'United Kingdom',
    'Guernsey': 'United Kingdom',
    'Cayman Islands': 'United Kingdom',
    'Gibraltar': 'United Kingdom',
    'Martinique': 'France',
    'French Guiana': 'France',
    'Mayotte': 'France',
    'Reunion': 'France',
    'Saint Barthelemy': 'France',
    'Guadeloupe': 'France',
    'St. Martin': 'France', # also netherlands apparently maybe perhaps?
    'Saint Martin': 'France', # of course there are two names
    'Vatican City': 'Italy', # this disappeared and idk where it went
    'East Timor': 'Timor-Leste',
    'Ivory Coast': "Cote d'Ivoire",
    'Cape Verde': 'Cabo Verde',
    'Cruise Ship': 'Others' # it was reclassified later
}

# figure out columns
def enum_cols():
    col_schema: Set[str] = set()
    for file in data_files:
        data = pd.read_csv(file,
            header=0,
            sep=','
        )
        col_schema.add(','.join(data.columns))

    for v in col_schema:
        print(v.split(','))

def read_file(file: str) -> pd.DataFrame:
    raw_data: pd.DataFrame = pd.read_csv(file,
        header=0,
        sep=',',
        converters={
            'Last Update': dateutil.parser.parse,
            'Last_Update': dateutil.parser.parse
        },
        na_values=[
            'Unknown', # sometimes Unknown is used instead of empty
            '#DIV/0!' # simply WHY, 01-14-2021.csv
        ]
    )

    # rename wanted rows
    data: pd.DataFrame = raw_data.rename(mapper=COLUMN_MAPPING, axis='columns')
    # drop unneeded rows (isn't python nice)
    drop_cols = set(data.columns) - RESULT_COLUMNS
    data.drop(columns=drop_cols, inplace=True)
    # map np.nan to None
    data = data.where(data.notnull(), None)
    # add missing columns
    for col in RESULT_COLUMNS - set(data.columns):
        data.insert(len(data.columns), col, [None] * len(data))
    # generate timestamp_id column
    file_date = datetime.datetime.strptime(path.basename(file), '%m-%d-%Y.csv')
    data['timestamp_id'] = [file_date.timestamp()] * len(data)
    # map countries
    data['country'] = data['country'].map(lambda x: COUNTRY_MAPPING[x] if x in COUNTRY_MAPPING else x)

    return data

#[print(read_file(x)) for x in data_files]

def enum_countries():
    # this may take a long time
    countries = np.array([], dtype=str)
    for file in data_files:
        df = read_file(file)
        countries = np.union1d(countries, df.get('country').values)
    
    countries_list = list(countries)
    print(countries_list)
    return countries_list

def upload_database_all():
    connection = oracledb.connect(
        user=os.environ.get('DBUSER'),
        password=os.environ.get('DBPASSWORD'),
        dsn='oracle.cise.ufl.edu/orcl',
        threaded=True
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        for file in data_files:
            cursor = connection.cursor()
            executor.submit(upload_database, cursor, file)
        
        
def upload_database(cursor, file: str):
    file_basename = path.basename(file)
    print(file_basename, 'read data')
    data = read_file(file)
    print(file_basename, 'iter tuples')
    tuples = []
    for row in data.itertuples():
        tuples.append((
            row.timestamp,
            row.timestamp_id,
            row.admin2,
            row.state,
            row.country,
            row.latitude,
            row.longitude,
            row.confirmed,
            row.deaths,
            row.recovered,
            row.active,
            row.incidence,
            row.case_fatality_ratio
        ))

    print(file_basename, 'insert')
    try:
        cursor.executemany("""
            insert into covid_data
                (timestamp, timestamp_id, admin2, state, country, latitude,
                longitude, confirmed, deaths, recovered, active, incidence,
                case_fatality_ratio)
            values
                (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)
        """, tuples)
        print(file_basename, 'rows inserted', cursor.rowcount)
    except Exception as ex:
        # WHY
        print(ex)
        print('offending file:', file)
        exit(1)

    print(file_basename, 'commit')
    cursor.execute('commit')

upload_database_all()

""" figure out which countries are bad
all_countries = set(enum_countries())
df = read_file('/mnt/datapool/prj/dbproject/data/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/01-14-2021.csv')
final_countries = set(df['country'])
print(all_countries - final_countries)
"""

""" display entire file
pd.set_option(
    'display.max_rows', None,
    'display.max_columns', None,
    'display.max_colwidth', None,
    'display.width', 1000000000000
)
print(read_file('/mnt/datapool/prj/dbproject/data/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/01-14-2021.csv'))
"""
