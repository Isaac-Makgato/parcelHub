# Transformations.py

"""
Author: Isaac Makgato
Purpose:
    Executes SQL scripts to create star schema tables and analytical views
    in BigQuery for the ParcelHub data warehouse.
"""

import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

# Load environment variables from servacc.env
load_dotenv("servacc.env")


PROJECT_ID = "parcelhubproject2"
DATASET = "dw_Parcelhub"

# Read environment variables
SERVICE_ACCOUNT_PATH = os.getenv("SERVICE_ACCOUNT_PATH")
SQL_SCRIPTS_DIR = os.getenv("SQL_SCRIPTS_DIR")


# Validate required environment variables
if not SERVICE_ACCOUNT_PATH or not os.path.exists(SERVICE_ACCOUNT_PATH):
    raise FileNotFoundError("Missing or invalid SERVICE_ACCOUNT_PATH in servacc.env")

if not SQL_SCRIPTS_DIR or not os.path.isdir(SQL_SCRIPTS_DIR):
    raise FileNotFoundError("Missing or invalid SQL_SCRIPTS_DIR in servacc.env")

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

# Ordered list of SQL files
sql_files = [
    "dim_hub.sql",
    "dim_parcel.sql",
    "dim_route.sql",
    "fact_parcel_events.sql",
    "vw_daily_route_performance.sql",
    "vw_parcel_event_sequence.sql",
]
def run_ingestion(processing_date: str):
    print(f"Ingesting data for {processing_date}")

# Execute a SQL file
def execute_sql_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            query = f.read()
            print(f"Executing: {file_path}...")
            client.query(query).result()
            print(f"Successfully executed: {file_path}\n")
    except Exception as e:
        print(f"Failed to execute {file_path}: {e}")

# Main function
def main():
    print("Starting transformations in BigQuery...\n")
    for file_name in sql_files:
        full_path = os.path.join(SQL_SCRIPTS_DIR, file_name)
        if os.path.exists(full_path):
            execute_sql_file(full_path)
        else:
            print(f"File not found: {full_path}")
    print("All scripts processed.")

# Run script
if __name__ == "__main__":
    main()
