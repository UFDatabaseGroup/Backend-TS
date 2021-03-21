import pandas as pd
import dateutil.parser
import os
import os.path as path
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

def try_read_file(file):
    data = pd.read_csv(file,
        header=0,
        sep=',',
        converters={
            'Last Update': dateutil.parser.parse,
            'Last_Update': dateutil.parser.parse
        }
    )
    print(data)

try_read_file(data_files[0])
