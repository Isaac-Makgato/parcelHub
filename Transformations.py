# Transformations.py

"""
Author: Isaac Makgato
Purpose:
    This script executes a series of SQL transformation scripts to build
    the star schema and analytical views in BigQuery for the ParcelHub
    data warehouse project.

    It performs the following tasks:
    - Authenticates using a Google Cloud service account.
    - Connects to a specified BigQuery project and dataset.
    - Executes SQL files in a defined order to create dimension tables,
      fact tables, and reporting views.

Usage:
    Run this script after data ingestion is complete to transform and
    structure the data warehouse layer.
"""

from google.oauth2 import service_account
from google.cloud import bigquery
import os

# Function to log the processing date (can be extended to apply dynamic logic in future)
def run_transformations(processing_date: str):
    print(f"Transforming data for {processing_date}")

# Path to the service account key file for authenticating with Google Cloud
SERVICE_ACCOUNT_PATH = "c:\\Users\\taelo\\OneDrive\\Documents\\GCP_PROJECT\\parcelhubproject2-695e7a467bc2.json"

# GCP project and BigQuery dataset identifiers
PROJECT_ID = "parcelhubproject2"
DATASET = "dw_Parcelhub"

# Load service account credentials and initialize the BigQuery client
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

# List of SQL script files to execute in order
sql_files = [
    r"C:\Users\taelo\OneDrive\Desktop\Transformations\dim_hub.sql",                     # Dimension: Hub
    r"C:\Users\taelo\OneDrive\Desktop\Transformations\dim_parcel.sql",                  # Dimension: Parcel
    r"C:\Users\taelo\OneDrive\Desktop\Transformations\dim_route.sql",                   # Dimension: Route
    r"C:\Users\taelo\OneDrive\Desktop\Transformations\fact_parcel_events.sql",          # Fact table: Parcel Events
    r"C:\Users\taelo\OneDrive\Desktop\Transformations\vw_daily_route_performance.sql",  # View: Daily Route Performance
    r"C:\Users\taelo\OneDrive\Desktop\Transformations\vw_parcel_event_sequence.sql",    # View: Parcel Event Sequence
]
# Refecenced line 51 to 69 from Stack over flow and chat gtp AI
# Function to execute a SQL file using the BigQuery client
def execute_sql_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            query = f.read()
            print(f"Executing: {file_path}...")
            client.query(query).result()  # Execute and wait for completion
            print(f"Successfully executed: {file_path}\n")
    except Exception as e:
        print(f"Failed to execute {file_path}: {e}")

# Main function to iterate through and run all SQL scripts
def main():
    print("Starting table creation in BigQuery...\n")
    for sql_file in sql_files:
        if os.path.exists(sql_file):
            execute_sql_file(sql_file)
        else:
            print(f"File not found: {sql_file}")
    print("All scripts processed.")

# Entry point of the script
if __name__ == "__main__":
    main()
