import pandas as pd
import click
import pyarrow as pa
from sqlalchemy import create_engine
from tqdm import tqdm

dtype = {
    'VendorID': 'int32',
    'store_and_fwd_flag': 'str',
    'RatecodeID': 'float64',
    'PULocationID': 'int32',
    'DOLocationID': 'int32',
    'passenger_count': 'float64',
    'trip_distance': 'float64',
    'fare_amount': 'float64',
    'extra': 'float64', 
    'mta_tax': 'float64',
    'tip_amount': 'float64',
    'tolls_amount': 'float64',
    'ehail_fee': 'float64',
    'improvement_surcharge': 'float64',
    'total_amount': 'float64',
    'payment_type': 'float64',
    'trip_type': 'float64',
    'congestion_surcharge': 'float64',
    'cbd_congestion_fee': 'float64',
}

parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]

df = pd.read_parquet()
df = df.astype(dtype)
df[parse_dates] = df[parse_dates].apply(pd.to_datetime)


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='green_taxi_data', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, target_table):
    """Ingest NYC taxi data into PostgreSQL database."""
    
    path = './green_tripdata_2025-11.parquet'
    
    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )


    pf = pa.ParquetFile(path)



    first = True

    for pf_iter in tqdm(pf.iter_batches(batch_size=chunksize)):
        df_chunk = pf_iter.to_pandas()
        df_chunk = df_chunk.astype(dtype)
        df_chunk[parse_dates] = df_chunk[parse_dates].apply(pd.to_datetime)
        if first:
            df_chunk.head(n=0).to_sql(
                name=target_table, 
                con=engine, 
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(
            name=target_table, 
            con=engine, 
            if_exists='append'
        )

if __name__ == '__main__':
    run()
