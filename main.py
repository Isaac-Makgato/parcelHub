
"""
ParcelHub Data Pipeline Script

Author: Isaac Makgato


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

import argparse  # For parsing command-line arguments
import logging   # For logging informational messages
from datetime import datetime, timedelta  # For handling date and time

# Import the custom ingestion and transformation functions from respective modules
from INGESTION import run_ingestion
from Transformations import run_transformations  

def parse_args():
    """
    Parse command-line arguments for controlling pipeline execution.
    Returns:
        argparse.Namespace: Parsed arguments with 'run' and 'processing_date'
    """
    parser = argparse.ArgumentParser(description="ParcelHub Data Pipeline")
    
    # Argument to choose which part of the pipeline to run: ingestion, transformation, or both
    parser.add_argument(
        "--run",
        choices=["ingest", "transform", "all"],
        default="all",
        help="Which part of the pipeline to run"
    )
    
    # Argument to specify the date to process; defaults to None, which is handled later
    parser.add_argument(
        "--processing_date",
        type=str,
        default=None,
        help="Date to process (format: YYYY-MM-DD). Defaults to yesterday."
    )
    
    return parser.parse_args()

def main():
    """
    Main execution function for the data pipeline.
    It runs ingestion and/or transformation based on command-line arguments.
    """
    args = parse_args()  # Parse the command-line arguments

    # Set the processing date to the provided value or default to yesterday's date
    processing_date = args.processing_date or (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Configure the logging system
    logging.basicConfig(level=logging.INFO)
    logging.info(f"Running pipeline with step: {args.run} and date: {processing_date}")

    # Run the ingestion step if requested
    if args.run in ["ingest", "all"]:
        logging.info("Running ingestion step...")
        run_ingestion(processing_date)

    # Run the transformation step if requested
    if args.run in ["transform", "all"]:
        logging.info("Running transformation step...")
        run_transformations(processing_date)

    # Log completion of the pipeline
    logging.info("Pipeline finished.")

# Entry point of the script
if __name__ == "__main__":
    main()
