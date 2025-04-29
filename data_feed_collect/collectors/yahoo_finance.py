"""Collector for Yahoo Finance data, including options chains."""

import yfinance as yf
import pandas as pd
import logging
from typing import Dict, Any, List, Optional, Tuple # Import Tuple
from datetime import datetime, date
import time
import concurrent.futures # Import concurrent.futures for threading

# Assume these models and database class exist based on summaries
# from data_feed_collect.models.instrument import Instrument, Stock, Option
# from data_feed_collect.models.ohlcv import OHLCV
# from data_feed_collect.storage.database import DataBase
from data_feed_collect.utils import limiter # Import the centralized limiter

# Define the key used for rate limiting Yahoo Finance requests
YAHOO_FINANCE_LIMIT_KEY = 'yahoo_finance'

# Assume table names based on test file summary
INSTRUMENTS_TABLE_NAME = "instruments" # Assuming a general instruments table
OHLCV_TABLE_NAME = "ohlcv" # Assuming a general ohlcv table

# --- Helper function to generate a consistent option instrument ID ---
def generate_option_instrument_id(underlying_ticker: str, expiration_date: date, strike: float, contract_type: str) -> str:
    """Generates a unique ID for an option instrument."""
    # Format: UNDERLYING_YYYYMMDD_TYPE_STRIKE
    # Ensure strike is formatted consistently, e.g., remove trailing .0 if integer
    strike_str = f"{strike:.2f}".rstrip('0').rstrip('.') if isinstance(strike, (int, float)) else str(strike)
    return f"{underlying_ticker.upper()}_{expiration_date.strftime('%Y%m%d')}_{contract_type.upper()[0]}_{strike_str}"

# --- Helper function to map yfinance option data to OHLCV format ---
def map_option_snapshot_to_ohlcv(
    instrument_id: str,
    snapshot_data: Dict[str, Any],
    collection_timestamp: datetime
) -> Dict[str, Any]:
    """Maps a single option contract snapshot to an OHLCV-like dictionary."""
    # yfinance snapshot provides lastPrice, volume, openInterest.
    # We map lastPrice to 'close' and volume to 'volume'.
    # Open, High, Low are not available in the snapshot, so we'll use 'close' value
    # or None/0 depending on desired schema interpretation for snapshots.
    # Let's use 'close' for O, H, L for simplicity in this snapshot context.
    last_price = snapshot_data.get('lastPrice')
    volume = snapshot_data.get('volume', 0) # Default volume to 0 if not present
    # open_interest = snapshot_data.get('openInterest', 0) # Could add if OHLCV schema supports

    return {
        "instrument_id": instrument_id,
        "timestamp": collection_timestamp,
        "open": last_price, # Using lastPrice for O, H, L in snapshot
        "high": last_price,
        "low": last_price,
        "close": last_price,
        "volume": volume,
        # "open_interest": open_interest # Add if schema supports
    }

# --- Helper function to map yfinance option data to Instrument format ---
def map_option_snapshot_to_instrument(
    instrument_id: str,
    underlying_ticker: str,
    expiration_date: date,
    snapshot_data: Dict[str, Any]
) -> Dict[str, Any]:
    """Maps a single option contract snapshot to an Instrument-like dictionary."""
    # Assuming Instrument table has columns like:
    # instrument_id (PK), symbol (e.g., option symbol like AAPL250117C00150000),
    # instrument_type ('option'), underlying_symbol, strike, expiration_date, contract_type
    return {
        "instrument_id": instrument_id,
        "symbol": snapshot_data.get('contractSymbol'), # yfinance contract symbol
        "instrument_type": "option",
        "underlying_symbol": underlying_ticker.upper(),
        "strike": snapshot_data.get('strike'),
        "expiration_date": expiration_date,
        "contract_type": snapshot_data.get('contractType'), # 'call' or 'put'
        # Add other relevant fields from snapshot if needed, e.g., currency
    }


class YahooFinanceOptionsChainCollector:
    """
    Collects options chain data for a list of stock tickers from Yahoo Finance.
    Saves option instruments and OHLCV snapshot data to the database.
    """

    def __init__(self):
        """
        Initializes the YahooFinanceOptionsChainCollector.
        """
        self.logger = logging.getLogger(__name__)
        # No specific initialization needed for yfinance itself
        pass

    def _collect_single_ticker(self, ticker_symbol: str, collection_timestamp: datetime) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Collects options chain data for a single ticker.
        This method is intended to be run in a separate thread.

        Args:
            ticker_symbol: The stock ticker symbol.
            collection_timestamp: The timestamp for this collection run.

        Returns:
            A tuple containing two lists:
            - List of dictionaries for option instruments.
            - List of dictionaries for OHLCV snapshots.
        """
        option_instruments_data: List[Dict[str, Any]] = []
        ohlcv_data: List[Dict[str, Any]] = []

        self.logger.info(f"Collecting options chain for ticker: {ticker_symbol}")

        try:
            # Use the centralized rate limiter before making the request
            with limiter.ratelimit(YAHOO_FINANCE_LIMIT_KEY, delay=True):
                ticker = yf.Ticker(ticker_symbol)

            # Get available expiration dates
            with limiter.ratelimit(YAHOO_FINANCE_LIMIT_KEY, delay=True):
                 expiration_dates = ticker.options

            if not expiration_dates:
                self.logger.info(f"No options found for {ticker_symbol}.")
                return [], [] # Return empty lists

            # Collect data for each expiration date
            for date_str in expiration_dates:
                self.logger.debug(f"  Fetching options for expiration: {date_str}")
                try:
                    # Parse the date string into a date object
                    expiration_date = datetime.strptime(date_str, '%Y-%m-%d').date()

                    with limiter.ratelimit(YAHOO_FINANCE_LIMIT_KEY, delay=True):
                        # Get option chain for the specific date
                        option_chain = ticker.option_chain(date_str)

                    # Process calls and puts
                    calls_df = option_chain.calls
                    puts_df = option_chain.puts

                    # Combine and process
                    combined_df = pd.concat([calls_df, puts_df], ignore_index=True)

                    if combined_df.empty:
                        self.logger.debug(f"No option contracts found for {ticker_symbol} on {date_str}.")
                        continue

                    # Process each option contract in the chain
                    for index, row in combined_df.iterrows():
                        try:
                            # Generate unique instrument ID
                            instrument_id = generate_option_instrument_id(
                                underlying_ticker=ticker_symbol,
                                expiration_date=expiration_date,
                                strike=row['strike'],
                                contract_type=row['contractType']
                            )

                            # Map data for Instrument table
                            option_instrument_data = map_option_snapshot_to_instrument(
                                instrument_id=instrument_id,
                                underlying_ticker=ticker_symbol,
                                expiration_date=expiration_date,
                                snapshot_data=row.to_dict() # Pass row as dict
                            )
                            option_instruments_data.append(option_instrument_data)

                            # Map data for OHLCV table (snapshot)
                            # Only save OHLCV if lastPrice is available
                            if pd.notna(row.get('lastPrice')):
                                ohlcv_snapshot_data = map_option_snapshot_to_ohlcv(
                                    instrument_id=instrument_id,
                                    snapshot_data=row.to_dict(), # Pass row as dict
                                    collection_timestamp=collection_timestamp
                                )
                                ohlcv_data.append(ohlcv_snapshot_data)
                            else:
                                 self.logger.debug(f"Skipping OHLCV for {instrument_id} due to missing lastPrice.")


                        except Exception as e:
                            self.logger.error(f"Error processing option contract for {ticker_symbol} on {date_str}: {e}")
                            # Continue processing other contracts

                except Exception as e:
                    self.logger.error(f"Error fetching options for {ticker_symbol} on {date_str}: {e}")
                    # Continue with the next expiration date

        except Exception as e:
            self.logger.error(f"An error occurred during Yahoo Options chain collection for {ticker_symbol}: {e}")
            # Return whatever data was collected so far, or empty lists if error was early
            return option_instruments_data, ohlcv_data

        self.logger.info(f"Finished collecting data for {ticker_symbol}.")
        return option_instruments_data, ohlcv_data


    def collect(self, config: Dict[str, Any], db: Any) -> None: # Use Any for db type hint to avoid circular dependency if DataBase not imported
        """
        Collects options chain data for specified tickers in parallel and saves to DB.

        Args:
            config: A dictionary containing the source configuration.
                    Expected to have:
                    - 'tickers' key (list of strings) OR 'fetch_tickers_from_db' (boolean).
                    - 'max_workers' (optional int, number of threads for parallel collection, default 5).
            db: An instance of the DataBase class.
        """
        tickers_to_collect: List[str] = []
        fetch_from_db = config.get("fetch_tickers_from_db", False)
        configured_tickers = config.get("tickers", [])
        max_workers = config.get("max_workers", 5) # Get max_workers from config, default to 5

        if fetch_from_db:
            self.logger.info("Fetching stock tickers from the database...")
            try:
                # Assuming a table named 'instruments' with 'instrument_type' column
                # and 'symbol' column for the ticker.
                # Need to ensure the DataBase class can execute queries and return DataFrame
                query = f"SELECT symbol FROM {INSTRUMENTS_TABLE_NAME} WHERE instrument_type = 'stock'"
                # Assuming execute_query returns a pandas DataFrame
                stock_df = db.execute_query(query)
                if not stock_df.empty:
                    tickers_to_collect = stock_df['symbol'].tolist()
                    self.logger.info(f"Found {len(tickers_to_collect)} stock tickers in DB.")
                else:
                    self.logger.warning("No stock tickers found in the database.")

            except Exception as e:
                self.logger.error(f"Failed to fetch tickers from database: {e}")
                # Fallback to configured tickers if DB fetch fails and tickers are configured
                if configured_tickers:
                     self.logger.info("Falling back to configured tickers.")
                     tickers_to_collect = configured_tickers
                else:
                    self.logger.error("No tickers specified in config and failed to fetch from DB. Aborting collection.")
                    return # Cannot proceed without tickers

        elif configured_tickers:
            tickers_to_collect = configured_tickers
            self.logger.info(f"Using {len(tickers_to_collect)} tickers from configuration.")
        else:
            self.logger.error("No tickers specified in config and 'fetch_tickers_from_db' is false. Aborting collection.")
            return # Cannot proceed without tickers

        if not tickers_to_collect:
             self.logger.warning("No tickers available for options chain collection.")
             return

        all_option_instruments_data: List[Dict[str, Any]] = []
        all_ohlcv_data: List[Dict[str, Any]] = []
        collection_timestamp = datetime.utcnow() # Timestamp for this collection run

        # Ensure Instrument and OHLCV tables exist (optional, but good practice)
        # This requires access to the model dataclasses and DataBase.create_table
        # try:
        #     # Assuming DataBase.create_table takes dataclass type and table name
        #     # from data_feed_collect.models.instrument import Option # Need to import if using dataclass
        #     # from data_feed_collect.models.ohlcv import OHLCV # Need to import if using dataclass
        #     # db.create_table(Option, INSTRUMENTS_TABLE_NAME, if_not_exists=True)
        #     # db.create_table(OHLCV, OHLCV_TABLE_NAME, if_not_exists=True)
        #     # self.logger.info("Ensured Instrument and OHLCV tables exist.")
        # except Exception as e:
        #      self.logger.error(f"Failed to ensure tables exist: {e}")
        #      # Decide if this is a fatal error or just log and continue


        self.logger.info(f"Starting parallel collection for {len(tickers_to_collect)} tickers with {max_workers} workers.")

        # Use ThreadPoolExecutor for parallel collection
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks for each ticker
            future_to_ticker = {
                executor.submit(self._collect_single_ticker, ticker_symbol, collection_timestamp): ticker_symbol
                for ticker_symbol in tickers_to_collect
            }

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_ticker):
                ticker_symbol = future_to_ticker[future]
                try:
                    # Get the result from the completed future
                    option_instruments, ohlcv_snapshots = future.result()

                    # Extend the main lists with data from this ticker
                    all_option_instruments_data.extend(option_instruments)
                    all_ohlcv_data.extend(ohlcv_snapshots)

                    self.logger.info(f"Successfully collected data for {ticker_symbol}. Instruments: {len(option_instruments)}, OHLCV: {len(ohlcv_snapshots)}")

                except Exception as exc:
                    # Log any exceptions that occurred in the worker thread
                    self.logger.error(f"Ticker {ticker_symbol} generated an exception: {exc}")

        self.logger.info("Parallel collection finished.")
        self.logger.info(f"Total instruments collected: {len(all_option_instruments_data)}")
        self.logger.info(f"Total OHLCV snapshots collected: {len(all_ohlcv_data)}")


        # --- Save collected data to the database ---
        if all_option_instruments_data:
            self.logger.info(f"Saving {len(all_option_instruments_data)} option instruments...")
            try:
                instruments_df = pd.DataFrame(all_option_instruments_data)
                # Assuming 'instrument_id' is the unique column for upsert
                db.upsert_dataframe(instruments_df, INSTRUMENTS_TABLE_NAME, unique_cols=['instrument_id'])
                self.logger.info("Option instruments saved successfully.")
            except Exception as e:
                self.logger.error(f"Failed to save option instruments: {e}")
        else:
            self.logger.info("No new option instruments to save.")


        if all_ohlcv_data:
            self.logger.info(f"Saving {len(all_ohlcv_data)} OHLCV snapshots...")
            try:
                ohlcv_df = pd.DataFrame(all_ohlcv_data)
                # Ensure timestamp is in the correct format if needed by upsert_dataframe
                # ClickHouse DateTime64(3) expects datetime objects or compatible strings
                # pandas datetime objects should work with clickhouse-connect
                # Assuming 'instrument_id' and 'timestamp' together form the unique key for OHLCV snapshots
                # Note: This assumes you want to potentially overwrite if collecting multiple times at the exact same timestamp
                # A better approach for history might be to append or use a different unique key strategy
                db.upsert_dataframe(ohlcv_df, OHLCV_TABLE_NAME, unique_cols=['instrument_id', 'timestamp'])
                self.logger.info("OHLCV snapshots saved successfully.")
            except Exception as e:
                self.logger.error(f"Failed to save OHLCV snapshots: {e}")
        else:
            self.logger.info("No OHLCV snapshots to save.")

# Example usage (for testing purposes, not part of the class)
# This block would typically be orchestrated by the main DataCollector class
if __name__ == '__main__':
    # This requires a running ClickHouse instance and environment variables set
    # Also requires the Instrument table to exist and potentially contain stock tickers
    from data_feed_collect.storage.database import DataBase
    from dotenv import load_dotenv
    import os

    load_dotenv() # Load environment variables from .env

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    try:
        # Initialize DB connection
        # Assumes DB connection details are in environment variables
        db_conn = DataBase()

        # Example config: fetch tickers from DB
        # config = {"fetch_tickers_from_db": True, "max_workers": 8}

        # Example config: specify tickers directly
        config = {"tickers": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"], "max_workers": 8}

        collector = YahooFinanceOptionsChainCollector()
        collector.collect(config, db_conn)

    except Exception as e:
        logging.error(f"An error occurred during the example run: {e}")
    finally:
        if 'db_conn' in locals() and db_conn:
            db_conn.close()
            logging.info("Database connection closed.")
