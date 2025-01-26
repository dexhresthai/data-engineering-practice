import argparse
from sqlalchemy import create_engine
import pandas as pd
import os

def ingest(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    chunksize = params.chunksize
    csv_name = 'data/output.csv.gz'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=chunksize)

    # Initialize table schema
    df = next(df_iter)
    df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
    df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Insert first chunk
    df.to_sql(name=table_name, con=engine, if_exists='append')
    nrows = df.shape[0]
    print(f'Inserted {nrows} rows')

    # Insert remaining chunks
    while True:
        try:
            df = next(df_iter)
            df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
            df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            nrows += df.shape[0]
            print(f'Inserted {df.shape[0]} rows')
        except StopIteration:
            print(f'Successfully ingested {nrows} rows to {db}/{table_name}.')
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', required=True, help='User name for Postgres')
    parser.add_argument('--password', required=True, help='Password for Postgres')
    parser.add_argument('--host', required=True, help='Host for Postgres')
    parser.add_argument('--port', required=True, help='Port for Postgres')
    parser.add_argument('--db', required=True, help='Database name for Postgres')
    parser.add_argument('--table_name', required=True, help='Name of the table for the results')
    parser.add_argument('--url', required=True, help='URL of the CSV file')
    parser.add_argument('--chunksize', type=int, default=50000, help='Number of rows per chunk')

    args = parser.parse_args()
    ingest(args)


'''
URL=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz && \
 python scripts/ingest_data.py \
 --user=root \
 --password=root \
 --host=localhost \
 --port=5432 \
 --db=ny_taxi \
 --table_name=yellow_taxi_trips \
 --url=${URL}
'''

## Homework Code 2025-01-26
'''
URL=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz && \
 python scripts/ingest_data.py \
 --user=root \
 --password=root \
 --host=localhost \
 --port=5432 \
 --db=ny_taxi \
 --table_name=yellow_taxi_trips_hw \
 --url=${URL} \
 --chunksize=100000
'''
