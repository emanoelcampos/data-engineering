import os
import pandas as pd
import argparse
from time import time
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    # Downloading the .gz file
    try:
        os.system(f"wget -O {csv_name} {url}")
    except Exception as e:
        print(f"Error downloading the file: {e}")
        return

    # Checking if the .gz file has been downloaded
    if not os.path.exists(csv_name):
        print(f"File {csv_name} not found.")
        return

    # Creating the database connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Reading the .gz file directly with pandas
    try:
        df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, compression='gzip')
        df = next(df_iter)
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Creating the table in the database and inserting the first chunk
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # Inserting the subsequent chunks
    while True:
        try:
            t_start = time()
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print('Inserted another chunk... took %.03f seconds' % (t_end - t_start))

        except StopIteration:
            print("All chunks have been processed.")
            break
        except Exception as e:
            print(f"Error processing the chunk: {e}")
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user, password, host, port, database name, table name, url of the csv
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the gzipped csv file')

    args = parser.parse_args()

    main(args)
