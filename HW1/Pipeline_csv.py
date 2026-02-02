from sqlalchemy import create_engine
import pandas as pd
from tqdm.auto import tqdm
import click



path = './taxi_zone_lookup.csv'



dtype = {
    'LocationID': 'int64',
    'Borough': 'str',
    'Zone': 'str',
    'service_zone': 'str',
}


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='green_taxi_data', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, target_table):





    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )



    df_iter = pd.read_csv(
        path,
        dtype=dtype,
        iterator=True, 
        chunksize = chunksize
    )

    first = True

    for df_chunk in tqdm(df_iter):
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
