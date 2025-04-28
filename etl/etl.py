import datetime
import glob
import logging
import os

import gdown
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s", 
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("etl.log"),  
    ],
)

# Load environment variables
load_dotenv()

# Database configuration
db_user = os.getenv("DB_USER", "airflow")
db_password = os.getenv("DB_PASSWORD", "airflow")
db_host = os.getenv("DB_HOST", "postgres")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME", "airflow")

# Create the PostgreSQL connection URL
postgres_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Create the SQLAlchemy engine
engine = create_engine(postgres_url)

# Test the database connection
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        logging.info(f"Connected to DB: {result.fetchone()}")
        inspector = inspect(engine)
        logging.info(f"Tables in the database: {inspector.get_table_names()}")
except Exception as e:
    logging.error(f"Failed to connect to the database: {e}")
    raise

# Set incremental table and column
incremental_table = "item_movie_lens"
incremental_column = "release_date"


def download_data():
    """Download data from Google Drive."""
    url = "https://drive.google.com/drive/folders/1_8tzTD1BHaAa1joaCd5mAKvxQxDwiF6k"
    output = '../ml-100k-data'
    try:
        gdown.download_folder(url, output=output, quiet=False, use_cookies=False)
        logging.info("Data downloaded successfully.")
    except Exception as e:
        logging.error(f"Error downloading data: {e}")
        raise


list_csv = glob.glob("../*data/*.csv")  # Updated path for Docker container


def extract(list_csv):
    """Extract data from CSV files."""
    for file in list_csv:
        try:
            df = pd.read_csv(file)
            table_name = os.path.splitext(os.path.basename(file))[0]
            logging.info(f"Extracted data from file: {file}")
            yield table_name, df
        except Exception as e:
            logging.error(f"Error extracting data from file {file}: {e}")


def get_latest_release_date(table_name, column_name):
    """Get the latest release date from the database."""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT MAX({column_name}) FROM {table_name}")
            result = conn.execute(query)
            latest = result.scalar()
            logging.info(f"Latest {column_name} in {table_name}: {latest}")
            return pd.to_datetime(latest) if latest else pd.Timestamp.min
    except Exception as e:
        logging.error(f"Error fetching latest {column_name} from {table_name}: {e}")
        return pd.Timestamp.min


def transform():
    """Transform the extracted data."""
    for table_name, df in extract(list_csv):
        try:
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
                logging.info(f"Converted 'timestamp' column in {table_name} to datetime.")

            # Remove year from movie_title
            if "movie_title" in df.columns:
                df["movie_title"] = df["movie_title"].str.replace(
                    r"\s*\(\d{2,4}\)$", "", regex=True
                ).str.strip()
                logging.info(f"Cleaned 'movie_title' column in {table_name}.")

            if table_name == incremental_table and incremental_column in df.columns:
                df[incremental_column] = pd.to_datetime(
                    df[incremental_column], errors="coerce"
                )
                latest_date = get_latest_release_date(table_name, incremental_column)

                # Filter for new records
                df = df[df[incremental_column] > latest_date]

                # Drop duplicates based on unique key columns
                df = df.drop_duplicates(subset=["movie_id"])
                logging.info(
                    f"{table_name}: Filtered to {len(df)} new unique rows after {latest_date}"
                )

        except Exception as e:
            logging.error(f"Error transforming {table_name}: {e}")
        finally:
            yield table_name, df


def load():
    """Load the transformed data into the database."""
    for table_name, df in transform():
        if df.empty:
            logging.warning(f"No new data to load for {table_name}. Skipping...")
            continue

        try:
            if table_name == incremental_table:
                df.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists="append",
                    index=False,
                )
                logging.info(f"{table_name} incrementally loaded with {len(df)} rows.")
            else:
                df.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists="replace",
                    index=False,
                )
                logging.info(f"{table_name} fully reloaded with {len(df)} rows.")
        except Exception as e:
            logging.error(f"Error loading {table_name}: {e}")


if __name__ == "__main__":
    logging.info("Starting ETL process...")
    try:
        download_data()
        load()
        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")