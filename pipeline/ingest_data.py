#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from tqdm.auto import tqdm
from sqlalchemy import create_engine

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# df = pd.read_csv(
#     url,
#     dtype=dtype,
#     parse_dates=parse_dates
# )

# print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

# df.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='yellow_taxi_data', help='Target table name')
@click.option("--year", default=2021, type=int, help="Data year")
@click.option("--month", default=1, type=int, help="Data month")
@click.option("--chunksize", default=100000, type=int, help="CSV chunksize")
def run(user, password, host, port, db, table, year, month, chunksize):
    # pg_user = 'root'
    # pg_password = 'root'
    # pg_host = 'localhost'
    # pg_port = 5432
    # pg_tb = 'ny_taxi'

    # year = 2021
    # month = 1

    # target_table = 'yellow_taxi_data'
    # chunksize = 100000

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
                # Create table schema (no data)
                df_chunk.head(0).to_sql(
                    name=table,
                    con=engine,
                    if_exists="replace"
                )
                first = False
                print("Table created")

            # Insert chunk
        df_chunk.to_sql(
            name=table,
            con=engine,
            if_exists="append"
        )

        print("Inserted:", len(df_chunk))

if __name__ == '__main__':
     run()