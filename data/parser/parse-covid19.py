import pandas as pd
import numpy as np
import dateutil.parser
import os
import os.path as path
import datetime
import cx_Oracle as oracledb
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
    data['timestamp_id'] = data['timestamp'].map(
        lambda d: int(d.replace(
            # take day only
            hour=0, minute=0, second=0, microsecond=0,
            tzinfo=datetime.timezone.utc
        ).timestamp())
    )

    return data

#[print(read_file(x)) for x in data_files]

def enum_countries():
    # this may take a long time
    countries = np.array([], dtype=str)
    for file in data_files:
        df = read_file(file)
        countries = np.union1d(countries, df.get('country').values)
    
    print(countries)

def upload_database():
    connection = oracledb.connect(
        user=os.environ.get('DBUSER'),
        password=os.environ.get('DBPASSWORD'),
        dsn='oracle.cise.ufl.edu/orcl'
    )
    cursor = connection.cursor()
    cursor.execute("""
        create table covid_data (
            timestamp date,
            timestamp_id number,
            admin2 varchar2(64),
            state varchar2(64),
            country varchar2(64),
            latitude number,
            longitude number,
            confirmed number,
            deaths number,
            recovered number,
            active number,
            incidence number,
            case_fatality_ratio number
        )
    """)
    cursor.execute("""
        create table covid_data_staging (
            timestamp date,
            timestamp_id number,
            admin2 varchar2(64),
            state varchar2(64),
            country varchar2(64),
            latitude number,
            longitude number,
            confirmed number,
            deaths number,
            recovered number,
            active number,
            incidence number,
            case_fatality_ratio number
        )
    """)
    for file in data_files:
        print('read data', path.basename(file))
        data = read_file(file)
        print('iter tuples')
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

        print('insert')
        try:
            cursor.executemany("""
                insert into covid_data_staging
                    (timestamp, timestamp_id, admin2, state, country, latitude,
                    longitude, confirmed, deaths, recovered, active, incidence,
                    case_fatality_ratio)
                values
                    (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)
            """, tuples)
            print('rows inserted', cursor.rowcount)
        except Exception as ex:
            # WHY
            print(ex)
            print('offending file:', file)
            exit(1)

        print('merge')
        # pain
        cursor.execute("""
            merge into covid_data data
            using covid_data_staging staging
            on (
                data.country = staging.country and
                data.state = staging.state and
                data.admin2 = staging.admin2 and
                data.timestamp = staging.timestamp
            )
            when not matched then
                insert
                    (timestamp, timestamp_id, admin2, state, country, latitude,
                    longitude, confirmed, deaths, recovered, active, incidence,
                    case_fatality_ratio)
                values
                    (staging.timestamp, staging.timestamp_id, staging.admin2,
                    staging.state, staging.country, staging.latitude,
                    staging.longitude, staging.confirmed, staging.deaths,
                    staging.recovered, staging.active, staging.incidence,
                    staging.case_fatality_ratio)
        """)
        print('merged')
        cursor.execute('delete from covid_data_staging')
        cursor.execute('commit')
        print('commit')

upload_database()

"""
pd.set_option(
    'display.max_rows', None,
    'display.max_columns', None,
    'display.max_colwidth', None,
    'display.width', 1000000000000
)
print(read_file('/mnt/datapool/prj/dbproject/data/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/01-14-2021.csv'))
"""
