"""
Script to initialize the ClickHouse database by creating tables
for all defined models.
"""

import logging
import sys

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
        "order_by": ["id"], # Assuming 'id' is the primary identifier for instruments
        "engine": "MergeTree()"
    },
    {
        "model": Stock,
        "table_name": "stocks",
        "order_by": ["id"], # Assuming 'id' is the primary identifier for stocks
        "engine": "MergeTree()"
    },
    {
        "model": Option,
        "table_name": "options",
        "order_by": ["id"], # Assuming 'id' is the primary identifier for options
        "engine": "MergeTree()"
    },
    {
        "model": OHLCV,
        "table_name": "ohlcv",
        "order_by": ["instrument_id", "timestamp"], # OHLCV data is ordered by instrument and time
        "engine": "MergeTree()"
    },
    {
        "model": OptionChain,
        "table_name": "option_chains",
        "order_by": ["instrument_id", "collection_timestamp"], # Option chains ordered by instrument and collection time
        "engine": "MergeTree()"
    },
]

def initialize_database():
    """Initializes the database by creating all necessary tables."""
    logger.info("Starting database initialization...")
    db = None
    try:
        # Use context manager for database connection
        with DataBase() as db:
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
                    if_not_exists=True # Use IF NOT EXISTS to avoid errors if table exists
                )
                logger.info(f"Table '{table_name}' creation process completed.")

        logger.info("Database initialization finished successfully.")

    except Exception as e:
        logger.error(f"Database initialization failed: {e}", exc_info=True)
        sys.exit(1) # Exit with a non-zero code on failure

if __name__ == "__main__":
    initialize_database()
