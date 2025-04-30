import pytest
import pandas as pd
from datetime import datetime, date
from data_feed_collect.storage.database import DataBase
from data_feed_collect.models import Instrument, Stock, OHLCV, OptionChain # Import updated models, remove Option
from data_feed_collect.utils.logging_config import setup_logging
import logging
import os
from dotenv import load_dotenv

# Setup logging for tests
setup_logging(logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables for DB connection
load_dotenv()

TEST_INSTRUMENT_TABLE = "test_instruments"
TEST_STOCK_TABLE = "test_stocks"
# TEST_OPTION_TABLE = "test_options" # Removed
TEST_OHLCV_TABLE = "test_ohlcv"
TEST_OPTION_CHAINS_TABLE = "test_option_chains" # Use the new combined table name
TEST_DATABASE_NAME = "test_data_feed_collect" # Use a dedicated test database

# Ensure test database name is used in connection fixture
@pytest.fixture(scope="module")
def db_connection():
    """Provides a database connection for tests."""
    # Use environment variables, but override database name for testing
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", 8123))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")

    logger.info(f"Connecting to test database: {TEST_DATABASE_NAME} at {host}:{port}")

    # Attempt to create the test database if it doesn't exist
    # This requires a connection to the default database first
    try:
        temp_db_conn = DataBase(host=host, port=port, user=user, password=password, database="default")
        temp_db_conn.execute_command(f"CREATE DATABASE IF NOT EXISTS {TEST_DATABASE_NAME}")
        temp_db_conn.close()
        logger.info(f"Ensured test database '{TEST_DATABASE_NAME}' exists.")
    except Exception as e:
        logger.error(f"Failed to ensure test database exists: {e}")
        pytest.fail(f"Could not connect to ClickHouse or create test database: {e}")


    db = None
    try:
        # Now connect to the specific test database
        db = DataBase(
            host=host,
            port=port,
            database=TEST_DATABASE_NAME, # Connect to the test database
            user=user,
            password=password
        )
        # Ensure tables are clean before tests run
        db.execute_command(f"DROP TABLE IF EXISTS {TEST_INSTRUMENT_TABLE}")
        db.execute_command(f"DROP TABLE IF EXISTS {TEST_STOCK_TABLE}")
        # db.execute_command(f"DROP TABLE IF EXISTS {TEST_OPTION_TABLE}") # Removed
        db.execute_command(f"DROP TABLE IF EXISTS {TEST_OHLCV_TABLE}")
        db.execute_command(f"DROP TABLE IF EXISTS {TEST_OPTION_CHAINS_TABLE}") # Drop the new table

        yield db
    except Exception as e:
        logger.error(f"Database connection or setup failed: {e}")
        pytest.fail(f"Database connection or setup failed: {e}")
    finally:
        if db:
            # Clean up tables after tests
            try:
                db.execute_command(f"DROP TABLE IF EXISTS {TEST_INSTRUMENT_TABLE}")
                db.execute_command(f"DROP TABLE IF EXISTS {TEST_STOCK_TABLE}")
                # db.execute_command(f"DROP TABLE IF EXISTS {TEST_OPTION_TABLE}") # Removed
                db.execute_command(f"DROP TABLE IF EXISTS {TEST_OHLCV_TABLE}")
                db.execute_command(f"DROP TABLE IF EXISTS {TEST_OPTION_CHAINS_TABLE}") # Drop the new table
                logger.info("Cleaned up test tables.")
            except Exception as e:
                 logger.error(f"Failed to drop test tables during cleanup: {e}")
            db.close()
            logger.info("Database connection closed.")


pytestmark = pytest.mark.integration

def test_create_instrument_tables(db_connection: DataBase):
    """Test creating the Instrument and Stock tables."""
    logger.info("Running test_create_instrument_tables")
    db_connection.create_table(Instrument, TEST_INSTRUMENT_TABLE, order_by=['symbol'], if_not_exists=True)
    db_connection.create_table(Stock, TEST_STOCK_TABLE, order_by=['symbol'], if_not_exists=True)

    # Verify tables exist (basic check)
    tables_df = db_connection.execute_query("SHOW TABLES")
    table_names = tables_df['name'].tolist()
    assert TEST_INSTRUMENT_TABLE in table_names
    assert TEST_STOCK_TABLE in table_names
    logger.info("test_create_instrument_tables passed.")

# Removed test_create_option_table

def test_create_ohlcv_table(db_connection: DataBase):
    """Test creating the OHLCV table."""
    logger.info("Running test_create_ohlcv_table")
    # Note: OHLCV model uses 'instrument_id', but table config uses 'symbol'.
    # Assuming 'instrument_id' in the model maps to 'symbol' in the table.
    # The create_table method uses the dataclass field names.
    # Let's update the test to match the model field name 'instrument_id'
    # and the init_db config to match the model field name 'instrument_id'.
    # For now, keeping the test as is, assuming the mapping works or needs correction elsewhere.
    # Correction: Looking at init_db.py, it uses 'instrument_id' in the comment but 'symbol' in order_by.
    # Let's align init_db.py and this test to use 'instrument_id' as per the OHLCV model.
    # This requires a change to init_db.py as well. I will include that change here.

    # --- Correction to align with OHLCV model field 'instrument_id' ---
    # This change is also needed in init_db.py TABLE_CONFIGS for OHLCV
    db_connection.create_table(OHLCV, TEST_OHLCV_TABLE, order_by=['instrument_id', 'timestamp'], if_not_exists=True)
    # --- End Correction ---


    # Verify table exists
    tables_df = db_connection.execute_query("SHOW TABLES")
    table_names = tables_df['name'].tolist()
    assert TEST_OHLCV_TABLE in table_names
    logger.info("test_create_ohlcv_table passed.")

def test_create_option_chains_table(db_connection: DataBase):
    """Test creating the combined OptionChain table."""
    logger.info("Running test_create_option_chains_table")
    # Use ReplacingMergeTree with timestamp as version column
    db_connection.create_table(
        OptionChain,
        TEST_OPTION_CHAINS_TABLE,
        engine='ReplacingMergeTree(timestamp)',
        order_by=['contract_symbol', 'timestamp'],
        primary_key=['contract_symbol'], # Primary key is prefix of order_by
        if_not_exists=True
    )

    # Verify table exists
    tables_df = db_connection.execute_query("SHOW TABLES")
    table_names = tables_df['name'].tolist()
    assert TEST_OPTION_CHAINS_TABLE in table_names
    logger.info("test_create_option_chains_table passed.")


def test_upsert_instrument_dataframe(db_connection: DataBase):
    """Test upserting data into the Instrument table."""
    logger.info("Running test_upsert_instrument_dataframe")
    db_connection.create_table(Instrument, TEST_INSTRUMENT_TABLE, order_by=['symbol'], if_not_exists=True)

    data = [
        {"symbol": "AAPL", "currency": "USD", "exchange": "NASDAQ", "name": "Apple Inc."},
        {"symbol": "MSFT", "currency": "USD", "exchange": "NASDAQ", "name": "Microsoft Corp."},
    ]
    df = pd.DataFrame(data)
    db_connection.upsert_dataframe(df, TEST_INSTRUMENT_TABLE, unique_cols=['symbol'])

    # Verify data was inserted
    result_df = db_connection.execute_query(f"SELECT * FROM {TEST_INSTRUMENT_TABLE} ORDER BY symbol")
    assert len(result_df) == 2
    assert result_df['symbol'].tolist() == ["AAPL", "MSFT"]

    # Test upserting with update and new row
    data_update = [
        {"symbol": "AAPL", "currency": "USD", "exchange": "NASDAQ", "name": "Apple Inc. (Updated)"}, # Update
        {"symbol": "GOOG", "currency": "USD", "exchange": "NASDAQ", "name": "Alphabet Inc."}, # New
    ]
    df_update = pd.DataFrame(data_update)
    db_connection.upsert_dataframe(df_update, TEST_INSTRUMENT_TABLE, unique_cols=['symbol'])

    # Verify data after upsert
    result_df_after_update = db_connection.execute_query(f"SELECT * FROM {TEST_INSTRUMENT_TABLE} ORDER BY symbol")
    assert len(result_df_after_update) == 3
    symbols = result_df_after_update['symbol'].tolist()
    names = result_df_after_update['name'].tolist()
    assert "AAPL" in symbols
    assert "MSFT" in symbols
    assert "GOOG" in symbols
    assert "Apple Inc. (Updated)" in names # Check for updated name
    logger.info("test_upsert_instrument_dataframe passed.")

# Removed test_upsert_option_dataframe

def test_upsert_option_chains_dataframe(db_connection: DataBase):
    """Test upserting data into the combined OptionChain table."""
    logger.info("Running test_upsert_option_chains_dataframe")
    # Create the table first
    db_connection.create_table(
        OptionChain,
        TEST_OPTION_CHAINS_TABLE,
        engine='ReplacingMergeTree(timestamp)',
        order_by=['contract_symbol', 'timestamp'],
        primary_key=['contract_symbol'],
        if_not_exists=True
    )

    # Sample data for OptionChain
    now1 = datetime.utcnow().replace(microsecond=0) # Use second precision for simplicity in test
    now2 = now1 + pd.Timedelta(seconds=1)
    exp_date = date(2024, 12, 31)

    data = [
        {
            "contract_symbol": "AAPL241231C00150000",
            "timestamp": now1,
            "symbol": "AAPL",
            "expiration_date": exp_date,
            "option_type": "Call",
            "strike_price": 150.0,
            "currency": "USD",
            "exchange": "NASDAQ",
            "name": None,
            "lastTradeDate": now1,
            "lastPrice": 10.0,
            "bid": 9.9,
            "ask": 10.1,
            "change": 0.5,
            "percentChange": 5.0,
            "volume": 100,
            "openInterest": 500,
            "impliedVolatility": 0.2,
            "inTheMoney": False,
            "contractSize": "REGULAR",
        },
         {
            "contract_symbol": "AAPL241231P00140000",
            "timestamp": now1,
            "symbol": "AAPL",
            "expiration_date": exp_date,
            "option_type": "Put",
            "strike_price": 140.0,
            "currency": "USD",
            "exchange": "NASDAQ",
            "name": None,
            "lastTradeDate": now1,
            "lastPrice": 5.0,
            "bid": 4.9,
            "ask": 5.1,
            "change": -0.2,
            "percentChange": -3.85,
            "volume": 50,
            "openInterest": 300,
            "impliedVolatility": 0.25,
            "inTheMoney": True,
            "contractSize": "REGULAR",
        },
    ]
    df = pd.DataFrame(data)
    db_connection.upsert_dataframe(df, TEST_OPTION_CHAINS_TABLE, unique_cols=['contract_symbol', 'timestamp'])

    # Verify data was inserted
    result_df = db_connection.execute_query(f"SELECT * FROM {TEST_OPTION_CHAINS_TABLE} ORDER BY contract_symbol, timestamp")
    assert len(result_df) == 2
    assert result_df['contract_symbol'].tolist() == ["AAPL241231C00150000", "AAPL241231P00140000"]
    assert result_df['timestamp'].tolist() == [now1, now1]
    assert result_df['strike_price'].tolist() == [150.0, 140.0]
    assert result_df['lastPrice'].tolist() == [10.0, 5.0]


    # Test upserting with update (same contract, same timestamp - should replace)
    # And a new snapshot for an existing contract (same contract, new timestamp)
    now3 = now1 + pd.Timedelta(seconds=2)

    data_update = [
        { # Update the first record at the same timestamp
            "contract_symbol": "AAPL241231C00150000",
            "timestamp": now1,
            "symbol": "AAPL",
            "expiration_date": exp_date,
            "option_type": "Call",
            "strike_price": 150.0,
            "currency": "USD",
            "exchange": "NASDAQ",
            "name": None,
            "lastTradeDate": now1,
            "lastPrice": 10.5, # Updated price
            "bid": 10.4,
            "ask": 10.6,
            "change": 1.0,
            "percentChange": 10.0,
            "volume": 150, # Updated volume
            "openInterest": 500,
            "impliedVolatility": 0.21,
            "inTheMoney": False,
            "contractSize": "REGULAR",
        },
        { # New snapshot for the second record at a new timestamp
            "contract_symbol": "AAPL241231P00140000",
            "timestamp": now3, # New timestamp
            "symbol": "AAPL",
            "expiration_date": exp_date,
            "option_type": "Put",
            "strike_price": 140.0,
            "currency": "USD",
            "exchange": "NASDAQ",
            "name": None,
            "lastTradeDate": now3,
            "lastPrice": 5.5, # New price
            "bid": 5.4,
            "ask": 5.6,
            "change": 0.3,
            "percentChange": 6.0,
            "volume": 60,
            "openInterest": 310,
            "impliedVolatility": 0.24,
            "inTheMoney": True,
            "contractSize": "REGULAR",
        },
         { # New contract at the latest timestamp
            "contract_symbol": "AAPL241231C00160000",
            "timestamp": now3, # New timestamp
            "symbol": "AAPL",
            "expiration_date": exp_date,
            "option_type": "Call",
            "strike_price": 160.0,
            "currency": "USD",
            "exchange": "NASDAQ",
            "name": None,
            "lastTradeDate": now3,
            "lastPrice": 8.0,
            "bid": 7.9,
            "ask": 8.1,
            "change": 0.0,
            "percentChange": 0.0,
            "volume": 20,
            "openInterest": 100,
            "impliedVolatility": 0.18,
            "inTheMoney": False,
            "contractSize": "REGULAR",
        },
    ]
    df_update = pd.DataFrame(data_update)
    db_connection.upsert_dataframe(df_update, TEST_OPTION_CHAINS_TABLE, unique_cols=['contract_symbol', 'timestamp'])

    # Verify data after upsert
    # We expect 4 rows:
    # AAPL241231C00150000 at now1 (updated)
    # AAPL241231P00140000 at now1 (original)
    # AAPL241231P00140000 at now3 (new snapshot)
    # AAPL241231C00160000 at now3 (new contract)
    result_df_after_update = db_connection.execute_query(f"SELECT * FROM {TEST_OPTION_CHAINS_TABLE} ORDER BY contract_symbol, timestamp")

    # Note: ReplacingMergeTree only guarantees that rows with the same ORDER BY key
    # and the latest version (timestamp in this case) are kept *eventually* after merges.
    # Immediately after insert, you might see duplicates. A simple SELECT might show
    # more rows than the final merged state. However, for testing upsert logic,
    # we can check if the latest version is present and has the correct data.
    # A more robust test might force a merge or query using FINAL.
    # For this test, we'll check the count and verify the latest data for a key.

    # Check total rows - should be 4 (original P@now1, updated C@now1, new P@now3, new C@now3)
    # The first C@now1 was replaced by the updated C@now1 because timestamp is the version column.
    # The P@now1 and P@now3 are distinct because timestamps differ.
    # The new C@now3 is distinct.
    assert len(result_df_after_update) == 4

    # Verify the updated record (AAPL241231C00150000 at now1)
    aapl_c_now1 = result_df_after_update[
        (result_df_after_update['contract_symbol'] == "AAPL241231C00150000") &
        (result_df_after_update['timestamp'] == now1)
    ]
    assert len(aapl_c_now1) == 1
    assert aapl_c_now1['lastPrice'].iloc[0] == 10.5
    assert aapl_c_now1['volume'].iloc[0] == 150

    # Verify the original record (AAPL241231P00140000 at now1)
    aapl_p_now1 = result_df_after_update[
        (result_df_after_update['contract_symbol'] == "AAPL241231P00140000") &
        (result_df_after_update['timestamp'] == now1)
    ]
    assert len(aapl_p_now1) == 1
    assert aapl_p_now1['lastPrice'].iloc[0] == 5.0 # Should be original price

    # Verify the new snapshot record (AAPL241231P00140000 at now3)
    aapl_p_now3 = result_df_after_update[
        (result_df_after_update['contract_symbol'] == "AAPL241231P00140000") &
        (result_df_after_update['timestamp'] == now3)
    ]
    assert len(aapl_p_now3) == 1
    assert aapl_p_now3['lastPrice'].iloc[0] == 5.5 # Should be new price

     # Verify the new contract record (AAPL241231C00160000 at now3)
    aapl_c_now3 = result_df_after_update[
        (result_df_after_update['contract_symbol'] == "AAPL241231C00160000") &
        (result_df_after_update['timestamp'] == now3)
    ]
    assert len(aapl_c_now3) == 1
    assert aapl_c_now3['lastPrice'].iloc[0] == 8.0 # Should be new price


    logger.info("test_upsert_option_chains_dataframe passed.")

