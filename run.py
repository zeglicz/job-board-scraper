import logging
import argparse

from modules.config import OFFERS_URL, DB_PATH
from modules.logging_config import setup_logging

from modules.fetcher import JobOffersFetcher
from modules.db import JobOfferDatabase


def parse_args():
    parser = argparse.ArgumentParser(description="Download job offers from -_-")
    parser.add_argument(
        "--pages",
        type=int,
        default=None,
        help="Number of offer pages to fetch (default: all)",
    )
    return parser.parse_args()


def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting ETL process...")

    args = parse_args()
    try:
        if not DB_PATH:
            logger.error("DB_PATH is not set. Please set it in your .env or config.py.")
            raise ValueError("DB_PATH is not set.")
        fetcher = JobOffersFetcher(OFFERS_URL)
        offer_pages = fetcher.fetch_pages(args.pages)["data"]
        logger.info(f"Fetched {len(offer_pages)} offer pages")

        db = JobOfferDatabase(DB_PATH)
        db.insert_offers(offer_pages)
        logger.info("Offers inserted into database")
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise
    finally:
        logger.info("ETL process completed")


if __name__ == "__main__":
    main()
