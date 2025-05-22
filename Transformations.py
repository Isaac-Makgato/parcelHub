# Transformations.py

"""
Author: Isaac Makgato
Purpose:
    Executes SQL scripts to create star schema tables and analytical views
    in BigQuery for the ParcelHub data warehouse.

Usage:
    Run after ingestion to structure the data warehouse layer.
"""

import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

# Hardcoded values (as per your request)
PROJECT_ID = "parcelhubproject2"
DATASET = "dw_Parcelhub"

# Load environment variables from .env file
load_dotenv("servacc.env")

# Get other config values from environment
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

# Helper: get all SQL script files in the directory
def get_sql_files(directory):
    return sorted(
        [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".sql")]
    )

# Main transformation runner
def run_transformations(processing_date: str = None):
    print(f"Running transformations for: {processing_date or 'default date'}\n")

    sql_files = get_sql_files(SQL_SCRIPTS_DIR)

    for sql_file in sql_files:
        try:
            with open(sql_file, "r", encoding="utf-8") as f:
                query = f.read()
                print(f"Executing: {sql_file}...")
                client.query(query).result()
                print(f"Successfully executed: {sql_file}\n")
        except Exception as e:
            print(f"Error executing {sql_file}: {e}")

    print("All transformations completed.")
