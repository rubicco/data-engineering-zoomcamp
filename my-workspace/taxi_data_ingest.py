import chunk
import os
import argparse
from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = "taxi_trips.parquet"
    os.system(f"wget {url} -O {parquet_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df = pd.read_parquet(parquet_name)
    # fix the date fields from TEXT to a TIMESTAMP type
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    chunk_size = 100000
    i = 0
    while i * chunk_size < len(df):
        t_start = time()
        df_chunk = df.loc[i * chunk_size : (i + 1) * chunk_size - 1, :]
        if_exists_action = "append" if i != 0 else "replace"
        df_chunk.to_sql(name=table_name, con=engine, if_exists=if_exists_action)
        t_end = time()
        print(
            f"Inserted chunk [{i}] (size: {len(df_chunk)}). It took {round(t_end - t_start, 2)} seconds."
        )
        i += 1

    print(
        "\nYou can debug the progress on pgcli with the following command:\nSELECT count(1) FROM yellow_taxi_data\n"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", required=True, help="user name for postgres")
    parser.add_argument("--password", required=True, help="password for postgres")
    parser.add_argument("--host", required=True, help="host for postgres")
    parser.add_argument("--port", required=True, help="port for postgres")
    parser.add_argument("--db", required=True, help="database name for postgres")
    parser.add_argument(
        "--table_name",
        required=True,
        help="name of the table where we will write the results to",
    )
    parser.add_argument("--url", required=True, help="url of the csv file")

    args = parser.parse_args()

    main(args)
