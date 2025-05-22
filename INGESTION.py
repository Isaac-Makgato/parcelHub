"""
Author: Isaac Makgato
Purpose: This script ingests parcel, event, route, and hub data from CSV and JSON files
         into Google BigQuery staging tables for the ParcelHub project.
         It authenticates using a service account and supports specifying the processing date.
"""
from dotenv import load_dotenv
import argparse
import pandas as pd
import json
import os
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account


# Load environment variables from .env file (if it exists)
load_dotenv("servacc.env")

# Load the service account path from environment variable
SERVICE_ACCOUNT_PATH = os.getenv("SERVICE_ACCOUNT_PATH")

if not SERVICE_ACCOUNT_PATH or not os.path.exists(SERVICE_ACCOUNT_PATH):
    raise FileNotFoundError("Missing or invalid SERVICE_ACCOUNT_PATH. Set it in a .env file or environment variable.")
PROJECT_ID = "parcelhubproject2"
DATASET = "Staging_Parcelhub"
#GOOGLE_APPLICATION_CREDENTIALS="parcelhubproject2-003b97702bef.json"
# Load credentials and initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

def run_ingestion(processing_date: str):
    print(f"Ingesting data for {processing_date}")

# Debug: confirm project being used
print(f"[DEBUG] Using project: {client.project}")
#Referenced from  Stack-Over flow (line 23 -45) 
#These functions load CSV or JSON data into a BigQuery table and check if data for a given date already exists.
def load_csv(path, table):
    print(f"Loading CSV from {path} into {table}")
    df = pd.read_csv(path)
    client.load_table_from_dataframe(df, table).result()
    print(f"Loaded {len(df)} rows into {table}")
# Load JSON file into BigQuery
def load_json(path, table):
    print(f"Loading JSON from {path} into {table}")
    with open(path, 'r') as f:
        lines = f.readlines()
        records = [json.loads(line.strip()) for line in lines if line.strip()]
    df = pd.DataFrame(records)
    client.load_table_from_dataframe(df, table).result()
    print(f"Loaded {len(df)} rows into {table}")

# Check if data for a specific date already exists
def already_loaded(table, column, date):
    query = f"""
        SELECT COUNT(*) AS count FROM `{table}`
        WHERE DATE({column}) = DATE("{date}")
    """
    result = client.query(query).result()
    return list(result)[0].count > 0

def main(processing_date):
    # File paths
    paths = {
        "parcels_20250101": "C:/Users/taelo/parcelhub_project/sample_data/parcels_20250101.csv",
        "parcels_20250102": "C:/Users/taelo/parcelhub_project/sample_data/parcels_20250102.csv",
        "events_20250101": "C:/Users/taelo/parcelhub_project/sample_data/events_20250101.json",
        "events_20250102": "C:/Users/taelo/parcelhub_project/sample_data/events_20250102.json",
        "routes": "C:/Users/taelo/parcelhub_project/sample_data/routes.csv",
        "hubs":   "C:/Users/taelo/parcelhub_project/sample_data/hubs.csv",
    }

    tables = {
        "parcels_20250101": f"{PROJECT_ID}.{DATASET}.stg_parcels_20250101",
        "parcels_20250102": f"{PROJECT_ID}.{DATASET}.stg_parcels_20250102",
        "events_20250101":  f"{PROJECT_ID}.{DATASET}.stg_events_20250101",
        "events_20250102":  f"{PROJECT_ID}.{DATASET}.stg_events_20250102",
        "routes": f"{PROJECT_ID}.{DATASET}.stg_routes",
        "hubs":   f"{PROJECT_ID}.{DATASET}.stg_hubs",
    }

    # Load parcels — skip "already_loaded()" to avoid query errors
    load_csv(paths["parcels_20250101"], tables["parcels_20250101"])
    load_csv(paths["parcels_20250102"], tables["parcels_20250102"])

    # Load events
    load_json(paths["events_20250101"], tables["events_20250101"])
    load_json(paths["events_20250102"], tables["events_20250102"])

    # Load reference data
    load_csv(paths["routes"], tables["routes"])
    load_csv(paths["hubs"], tables["hubs"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--processing_date", type=str, default=datetime.now().strftime("%Y-%m-%d"))
    args = parser.parse_args()
    main(args.processing_date)
