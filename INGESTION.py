"""
Author: Isaac Makgato
Purpose: This script ingests parcel, event, route, and hub data from CSV and JSON files
         into Google BigQuery staging tables for the ParcelHub project.
         It authenticates using a service account and supports specifying the processing date.
      NB: To gain a better understanding of the approach, I conducted research 
          using resources such as Google and Stack Overflow.
"""

from dotenv import load_dotenv
import argparse
import pandas as pd
import json
import os
from datetime import datetime
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

# Load environment variables
load_dotenv("servacc.env")

SERVICE_ACCOUNT_PATH = os.getenv("SERVICE_ACCOUNT_PATH")
BASE_DATA_DIR = os.getenv("BASE_DATA_DIR")

if not SERVICE_ACCOUNT_PATH or not os.path.exists(SERVICE_ACCOUNT_PATH):
    raise FileNotFoundError("Missing or invalid SERVICE_ACCOUNT_PATH in .env file.")

if not BASE_DATA_DIR or not os.path.exists(BASE_DATA_DIR):
    raise FileNotFoundError("Missing or invalid BASE_DATA_DIR in .env file.")

PROJECT_ID = "parcelhubproject2"
DATASET = "Staging_Parcelhub"

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

# === File loading functions ===
def load_csv(path, table):
    print(f"Loading CSV from {path} into {table}")
    df = pd.read_csv(path)
    client.load_table_from_dataframe(df, table).result()
    print(f"Loaded {len(df)} rows into {table}")

def load_json(path, table):
    print(f"Loading JSON from {path} into {table}")
    with open(path, 'r') as f:
        records = [json.loads(line.strip()) for line in f if line.strip()]
    df = pd.DataFrame(records)
    client.load_table_from_dataframe(df, table).result()
    print(f"Loaded {len(df)} rows into {table}")

# === Path helper ===
def get_file_paths(processing_date):
    base = Path(BASE_DATA_DIR)
    return {
        "parcels": base / f"parcels_{processing_date.replace('-', '')}.csv",
        "events":  base / f"events_{processing_date.replace('-', '')}.json",
        "routes":  base / "routes.csv",
        "hubs":    base / "hubs.csv"
    }

def get_table_names(processing_date):
    date_tag = processing_date.replace("-", "")
    return {
        "parcels": f"{PROJECT_ID}.{DATASET}.stg_parcels_{date_tag}",
        "events":  f"{PROJECT_ID}.{DATASET}.stg_events_{date_tag}",
        "routes":  f"{PROJECT_ID}.{DATASET}.stg_routes",
        "hubs":    f"{PROJECT_ID}.{DATASET}.stg_hubs"
    }

# === Main ingestion ===
def main(processing_date):
    print(f"Processing date: {processing_date}")
    paths = get_file_paths(processing_date)
    tables = get_table_names(processing_date)

    load_csv(paths["parcels"], tables["parcels"])
    load_json(paths["events"], tables["events"])
    load_csv(paths["routes"], tables["routes"])
    load_csv(paths["hubs"], tables["hubs"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--processing_date", type=str, default=datetime.now().strftime("%Y-%m-%d"))
    args = parser.parse_args()
    main(args.processing_date)
