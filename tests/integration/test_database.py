import pytest
import os
from data_feed_collect.storage import DataBase
from data_feed_collect.models import Instrument, Stock, Option, OHLCV
from typing import List

# Define test table names
TEST_INSTRUMENT_TABLE = "test_instruments"
TEST_STOCK_TABLE = "test_stocks"
TEST_OPTION_TABLE = "test_options"
TEST_OHLCV_TABLE = "test_ohlcv"
TEST_DATABASE_NAME = "test_data_feed_collect" # Use a dedicated test database

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

@pytest.fixture(scope="module")
def db_connection():
    """
    Pytest fixture to provide a DataBase connection for integration tests.

    Requires CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DATABASE, CLICKHOUSE_USER
    environment variables to be set. Skips tests if not available.
    Creates and drops a dedicated test database and tables.
    """
    # Check if required environment variables are set
    required_env_vars = ["CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_DATABASE", "CLICKHOUSE_USER"]
    if not all(os.getenv(var) for var in required_env_vars):
        pytest.skip(
            "ClickHouse connection environment variables not set. "
            f"Required: {', '.join(required_env_vars)}"
        )

    # Use a temporary client to create the test database if it doesn't exist
    try:
        temp_client = DataBase(
             host=os.getenv("CLICKHOUSE_HOST"),
             port=int(os.getenv("CLICKHOUSE_PORT")),
             database=os.getenv("CLICKHOUSE_DATABASE"), # Connect to default DB first
             user=os.getenv("CLICKHOUSE_USER"),
             password=os.getenv("CLICKHOUSE_PASSWORD")
        )
        # Use execute_command for CREATE DATABASE
        temp_client.execute_command(f"CREATE DATABASE IF NOT EXISTS {TEST_DATABASE_NAME}")
        temp_client.close()
    except Exception as e:
        pytest.fail(f"Failed to connect to ClickHouse or create test database: {e}")


    # Now connect to the specific test database
    db = None
    try:
        db = DataBase(
            host=os.getenv("CLICKHOUSE_HOST"),
            port=int(os.getenv("CLICKHOUSE_PORT")),
            database=TEST_DATABASE_NAME, # Connect to the test database
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD")
        )
        # Ensure connection is working using execute_command
        db.execute_command("SELECT 1")
        print(f"\nConnected to ClickHouse database: {TEST_DATABASE_NAME}")

        yield db # Provide the database connection to the tests

    except Exception as e:
        pytest.fail(f"Failed to connect to test ClickHouse database: {e}")

    finally:
        # Teardown: Drop test tables and the test database
        if db:
            print(f"\nDropping test tables in {TEST_DATABASE_NAME}...")
            tables_to_drop = [TEST_INSTRUMENT_TABLE, TEST_STOCK_TABLE, TEST_OPTION_TABLE, TEST_OHLCV_TABLE]
            for table in tables_to_drop:
                try:
                    # Use execute_command for DROP TABLE
                    db.execute_command(f"DROP TABLE IF EXISTS `{table}`")
                    print(f"Dropped table: `{table}`")
                except Exception as e:
                    print(f"Error dropping table `{table}`: {e}")

            # Drop the test database itself
            try:
                 # Reconnect to default DB to drop the test DB
                 temp_client_drop = DataBase(
                     host=os.getenv("CLICKHOUSE_HOST"),
                     port=int(os.getenv("CLICKHOUSE_PORT")),
                     database=os.getenv("CLICKHOUSE_DATABASE"),
                     user=os.getenv("CLICKHOUSE_USER"),
                     password=os.getenv("CLICKHOUSE_PASSWORD")
                 )
                 # Use execute_command for DROP DATABASE
                 temp_client_drop.execute_command(f"DROP DATABASE IF EXISTS {TEST_DATABASE_NAME}")
                 print(f"Dropped test database: {TEST_DATABASE_NAME}")
                 temp_client_drop.close()
            except Exception as e:
                 print(f"Error dropping test database {TEST_DATABASE_NAME}: {e}")

            db.close()
            print("ClickHouse connection closed.")


def test_create_instrument_tables(db_connection: DataBase):
    """Test creating tables for Instrument, Stock, and Option dataclasses."""
    db = db_connection

    # Test creating Instrument table
    db.create_table(
        dataclass_type=Instrument,
        table_name=TEST_INSTRUMENT_TABLE,
        engine='MergeTree()',
        order_by=['symbol'],
        primary_key=['symbol']
    )
    # Verify table exists using execute_command
    assert db.execute_command(f"EXISTS `{TEST_INSTRUMENT_TABLE}`") == 1

    # Test creating Stock table (inherits from Instrument)
    db.create_table(
        dataclass_type=Stock,
        table_name=TEST_STOCK_TABLE,
        engine='MergeTree()',
        order_by=['symbol'],
        primary_key=['symbol']
    )
    # Verify table exists using execute_command
    assert db.execute_command(f"EXISTS `{TEST_STOCK_TABLE}`") == 1


    # Test creating Option table (inherits from Instrument, adds fields)
    db.create_table(
        dataclass_type=Option,
        table_name=TEST_OPTION_TABLE,
        engine='MergeTree()',
        order_by=['symbol', 'expiration_date', 'strike_price', 'option_type'], # More specific order for options
        primary_key=['symbol', 'expiration_date', 'strike_price', 'option_type']
    )
    # Verify table exists using execute_command
    assert db.execute_command(f"EXISTS `{TEST_OPTION_TABLE}`") == 1


def test_create_ohlcv_table(db_connection: DataBase):
    """Test creating table for OHLCV dataclass."""
    db = db_connection

    db.create_table(
        dataclass_type=OHLCV,
        table_name=TEST_OHLCV_TABLE,
        engine='MergeTree()',
        order_by=['instrument_symbol', 'timestamp'],
        primary_key=['instrument_symbol', 'timestamp']
    )
    # Verify table exists using execute_command
    assert db.execute_command(f"EXISTS `{TEST_OHLCV_TABLE}`") == 1

# Example of how to run these tests:
# 1. Ensure ClickHouse is running and accessible via the environment variables.
# 2. Run pytest with the integration marker:
#    pytest -m integration tests/integration/test_database.py
# 3. To run all tests including integration tests:
#    pytest tests/integration/test_database.py
# 4. To skip integration tests:
#    pytest -m "not integration"
