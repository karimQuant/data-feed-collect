"""
Script to initialize the ClickHouse database by creating tables
for all defined models.

Includes an option to drop existing tables before creation.
"""

import logging
import sys
import argparse # Import argparse for command-line arguments

# Add the project root to the sys.path if running from the root
# This helps in importing modules like data_feed_collect
# Assuming this script is run from the project root directory
# If run from elsewhere, sys.path might need adjustment or
# the script should be installed as part of a package.
# For simplicity, assuming run from root or package installed.

from data_feed_collect.storage.database import DataBase
from data_feed_collect.models import (
    Instrument,
    Stock,
    Option,
    OHLCV,
    OptionChain,
)
from data_feed_collect.utils.logging_config import setup_logging

# Configure logging
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define table configurations: model, table name, and ORDER BY columns
# ORDER BY is required for MergeTree engine and is crucial for data merging/deduplication
TABLE_CONFIGS = [
    {
        "model": Instrument,
        "table_name": "instruments",
        # Using 'symbol' as the primary identifier for base instruments (like stocks)
        "order_by": ["symbol"],
        "engine": "MergeTree()"
    },
    {
        "model": Stock,
        "table_name": "stocks",
        # Using 'symbol' as the primary identifier for stocks
        "order_by": ["symbol"],
        "engine": "MergeTree()"
    },
    {
        "model": Option,
        "table_name": "options",
        # Using 'contract_symbol' as the primary identifier for option contracts
        "order_by": ["contract_symbol"],
        "engine": "MergeTree()"
    },
    {
        "model": OHLCV,
        "table_name": "ohlcv",
        # Assuming instrument_id in OHLCV refers to the stock symbol
        "order_by": ["instrument_id", "timestamp"], # OHLCV data is ordered by instrument (stock symbol) and time
        "engine": "MergeTree()"
    },
    {
        "model": OptionChain,
        "table_name": "option_chains",
        # Using 'contract_symbol' and 'timestamp' as the primary key for option chain snapshots
        "order_by": ["contract_symbol", "timestamp"], # Option chains ordered by option contract symbol and collection time
        "engine": "MergeTree()"
    },
]

def initialize_database(drop_existing: bool = False):
    """
    Initializes the database by creating all necessary tables.

    Args:
        drop_existing: If True, drops tables if they exist before creating them.
    """
    logger.info("Starting database initialization...")
    db = None
    try:
        # Use context manager for database connection
        with DataBase() as db:
            if drop_existing:
                logger.warning("Drop existing tables requested. Proceeding to drop tables...")
                # Drop tables in reverse order of potential dependencies if needed,
                # but for simple tables like these, order doesn't strictly matter.
                # Dropping in the order of config list is fine.
                for config in TABLE_CONFIGS:
                    table_name = config["table_name"]
                    logger.info(f"Dropping table '{table_name}' if it exists...")
                    try:
                        db.execute_command(f"DROP TABLE IF EXISTS {table_name}")
                        logger.info(f"Table '{table_name}' dropped (or did not exist).")
                    except Exception as drop_e:
                        logger.error(f"Failed to drop table '{table_name}': {drop_e}", exc_info=True)
                        # Decide if you want to continue or stop on drop failure
                        # For now, we log and continue to attempt creation
                        pass # Continue to the next table or creation step

            for config in TABLE_CONFIGS:
                model = config["model"]
                table_name = config["table_name"]
                order_by = config["order_by"]
                engine = config.get("engine", "MergeTree()") # Default to MergeTree

                logger.info(f"Creating table '{table_name}' from model '{model.__name__}'...")
                db.create_table(
                    dataclass_type=model,
                    table_name=table_name,
                    order_by=order_by,
                    engine=engine,
                    if_not_exists=True # Use IF NOT EXISTS to avoid errors if table exists (redundant if dropping, but safe)
                )
                logger.info(f"Table '{table_name}' creation process completed.")

        logger.info("Database initialization finished successfully.")

    except Exception as e:
        logger.error(f"Database initialization failed: {e}", exc_info=True)
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Initialize the ClickHouse database by creating tables."
    )
    parser.add_argument(
        "--drop",
        action="store_true", # This makes it a boolean flag
        help="Drop existing tables before creating them."
    )

    args = parser.parse_args()

    initialize_database(drop_existing=args.drop)
