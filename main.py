"""
ParcelHub Data Pipeline Script

Author: Isaac Makgato
NB:The scripts were developed with some reference to resources from Google, Stack Overflow, and ChatGPT AI.
Purpose:
--------
This script is the main entry point for running the ParcelHub data pipeline. It supports two main operations:
1. Data Ingestion: Loads raw data (from CSV/JSON files) into BigQuery staging tables.
2. Data Transformation: Transforms staging data into a star schema with dimension and fact tables.

You can run either step individually or run both together by using command-line arguments.

Usage:
------
Run both ingestion and transformation (default):
    python main.py

Run only ingestion:
    python main.py --run ingest

Run only transformation:
    python main.py --run transform
"""

# Import required modules
import argparse
import logging
from datetime import datetime, timedelta, timezone
import os
import re

# Import the custom ingestion and transformation functions
from INGESTION import run_ingestion
from Transformations import run_transformations


def parse_args():
    parser = argparse.ArgumentParser(description="ParcelHub Data Pipeline")
    parser.add_argument(
        "--run",
        choices=["ingest", "transform", "all"],
        default="all",
        help="Which part of the pipeline to run"
    )
    parser.add_argument(
        "--processing_date",
        type=str,
        default=None,
        help="Date to process (format: YYYY-MM-DD). Defaults to yesterday."
    )
    return parser.parse_args()


def get_all_dates_from_data_folder(data_folder_path):
    """

    Scans the specified folder for parcel CSV files and extracts all available dates
    from filenames matching the pattern parcels_YYYYMMDD.csv.

    Returns:
        A sorted list of dates in 'YYYY-MM-DD' format.
        NB:I used ChatGPT to write this function
    """
    pattern = re.compile(r"parcels_(\d{8})\.csv")
    dates = []
    for filename in os.listdir(data_folder_path):
        match = pattern.match(filename)
        if match:
            date_str = match.group(1)
            date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            dates.append(date_formatted)
    return sorted(dates)


def main():
     """
    Entry point of the data pipeline.
    Determines dates to process based on CLI arguments or available files,
    and runs ingestion and/or transformation steps for each date.
    """
    args = parse_args()
    logging.basicConfig(level=logging.INFO)

    data_folder = "C:\\Users\\taelo\\parcelhub_project\\sample_data"  # Update if needed
    # Determine processing dates
    if args.processing_date:
        processing_dates = [args.processing_date]
    else:
        processing_dates = get_all_dates_from_data_folder(data_folder)
        if not processing_dates:
            logging.error(f"No data files found in {data_folder}. Exiting.")
            return

    logging.info(f"Running pipeline with step: {args.run} for dates: {processing_dates}")
    # Run the pipeline for each date
    for processing_date in processing_dates:
        logging.info(f"Processing date: {processing_date}")

        if args.run in ["ingest", "all"]:
            logging.info("Running ingestion step...")
            run_ingestion(processing_date)

        if args.run in ["transform", "all"]:
            logging.info("Running transformation step...")
            run_transformations(processing_date)

    logging.info("Pipeline finished.")


if __name__ == "__main__":
    main()
