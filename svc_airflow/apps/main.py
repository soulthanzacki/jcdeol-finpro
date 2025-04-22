import logging
import argparse
from scripts.extract_load import Extract
from scripts.transform import Raw, DWH, Mart

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--load_date", required=True, help="Current execution date")
    # parser.add_argument("--data_date", required=True, help="Previous execution date")
    args = parser.parse_args()
    
    load_date = args.load_date
    # data_date = args.data_date

    logging.info(f"Starting ELT pipeline for load_date: {load_date}")
    # logging.info(f"Starting ELT pipeline for data_date: {data_date}")

    Extract().main()
    Raw(load_date).execute()
    DWH().execute()
    # Mart().execute()

    
    logging.info("ELT pipeline completed.")