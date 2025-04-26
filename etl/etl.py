import datetime
import glob
import os

import gdown
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

load_dotenv()

db_user = os.getenv("DB_USER", "airflow")
db_password = os.getenv("DB_PASSWORD", "airflow")
db_host = os.getenv("DB_HOST", "postgres")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME", "airflow")

# db_user = os.getenv("DB_USER")
# db_password = os.getenv("DB_PASSWORD")
# db_host = os.getenv("DB_HOST")
# db_port = os.getenv("DB_PORT")
# db_name = os.getenv("DB_NAME")


postgres_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

engine = create_engine(postgres_url)

with engine.connect() as conn:
    result = conn.execute(text("SELECT version();"))
    print("Connected to DB:", result.fetchone())
    inspector = inspect(engine)
    print("Tables in the database:", inspector.get_table_names())


url = 'https://drive.google.com/drive/folders/1_8tzTD1BHaAa1joaCd5mAKvxQxDwiF6k'
output = '../ml-100k-data'
gdown.download_folder(url, output=output, quiet=False, use_cookies=False)
list_csv = glob.glob('../*data*/*.csv')

# Set incremental table and column
incremental_table = 'item_movie_lens'
incremental_column = 'release_date'


# Get latest release_date from DB
def get_latest_release_date(table_name, column_name):
    try:
        with engine.connect() as conn:
            query = text(f"SELECT MAX({column_name}) FROM {table_name}")
            result = conn.execute(query)
            latest = result.scalar()
            return pd.to_datetime(latest) if latest else pd.Timestamp.min
    except Exception as e:
        print(f"Error fetching latest {column_name} from {table_name}: {e}")
        return pd.Timestamp.min


def get_run_date(df):
    df[incremental_column] = pd.to_datetime(df[incremental_column], errors='coerce')
    if df[incremental_column].isna().all():
        raise ValueError("No valid release_date values found.")
    start_date = df[incremental_column].min()
    end_date = df[incremental_column].max()
    print(f"Generating dates from {start_date.date()} to {end_date.date()}")
    return [
        (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end_date - start_date).days + 1)
    ]


def extract(list_csv=list_csv):
    for file in list_csv:
        df = pd.read_csv(file)
        table_name = os.path.splitext(os.path.basename(file))[0]
        yield table_name, df


def transform():
    for table_name, df in extract(list_csv):
        try:
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)

            # Remove year from movie_title 
            if 'movie_title' in df.columns:
                df['movie_title'] = df['movie_title'].str.replace(r"\s*\(\d{2,4}\)$", "", regex=True).str.strip()

            if table_name == incremental_table and incremental_column in df.columns:
                df[incremental_column] = pd.to_datetime(df[incremental_column], errors='coerce')
                latest_date = get_latest_release_date(table_name, incremental_column)
                
                # Filter for new records
                df = df[df[incremental_column] > latest_date]

                # Drop duplicates based on unique key columns (customize as needed)
                df = df.drop_duplicates(subset=['movie_id'])

                print(f"{table_name}: Filtered to {len(df)} new unique rows after {latest_date}")

        except Exception as e:
            print(f"Error transforming {table_name}: {e}")
        finally:
            yield table_name, df


# Load step
def load():
    for table_name, df in transform():
        if df.empty:
            print(f"No new data to load for {table_name}")
            continue

        try:
            if table_name == incremental_table:
                df.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='append',
                    index=False
                    )
                print(f"{table_name} incrementally loaded with {len(df)} rows")
            else:
                df.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='replace',
                    index=False
                    )
                print(f"{table_name} fully reloaded with {len(df)} rows")
        except Exception as e:
            print(f"Error loading {table_name}: {e}")


load()
