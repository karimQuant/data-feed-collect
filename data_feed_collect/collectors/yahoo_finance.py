"""Collector for Yahoo Finance data, including options chains."""

import yfinance as yf
import pandas as pd
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date
import time
import concurrent.futures # Import concurrent.futures for threading

# Assume these models and database class exist based on summaries
from data_feed_collect.models.instrument import Instrument, Stock, Option # Keep Instrument/Option import
# from data_feed_collect.models.ohlcv import OHLCV # Remove OHLCV import
from data_feed_collect.models.option_chain import OptionChain # Import the new OptionChain model
# from data_feed_collect.storage.database import DataBase # Use Any for db type hint
# Import limiter and the key from the utils package
from data_feed_collect.utils import limiter, YAHOO_FINANCE_LIMIT_KEY
# Import the new logging setup function
from data_feed_collect.utils.logging_config import setup_logging


# Assume table names based on test file summary and new requirement
INSTRUMENTS_TABLE_NAME = "instruments" # Assuming a general instruments table
# OHLCV_TABLE_NAME = "ohlcv" # Remove OHLCV table name
OPTION_CHAINS_TABLE_NAME = "option_chains" # New table name for raw option chain data

# --- Helper functions moved inside the class as static methods ---
# Removed module-level definitions

class YahooFinanceOptionsChainCollector:
    """
    Collects options chain data for a list of stock tickers from Yahoo Finance.
    Saves option instruments and raw option chain snapshot data to the database.
    """

    def __init__(self):
        """
        Initializes the YahooFinanceOptionsChainCollector.
        """
        self.logger = logging.getLogger(__name__)
        # No specific initialization needed for yfinance itself
        pass

    @staticmethod
    def generate_option_instrument_id(underlying_ticker: str, expiration_date: date, strike: float, contract_type: str) -> str:
        """Generates a unique ID for an option instrument."""
        # Format: UNDERLYING_YYYYMMDD_TYPE_STRIKE
        # Ensure strike is formatted consistently, e.g., remove trailing .0 if integer
        strike_str = f"{strike:.2f}".rstrip('0').rstrip('.') if isinstance(strike, (int, float)) else str(strike)
        return f"{underlying_ticker.upper()}_{expiration_date.strftime('%Y%m%d')}_{contract_type.upper()[0]}_{strike_str}"

    @staticmethod
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

    @staticmethod
    def map_option_snapshot_to_option_chain(
        instrument_id: str,
        snapshot_data: Dict[str, Any],
        collection_timestamp: datetime,
        # underlying_ticker: str, # Not directly used in mapping to OptionChain fields
        # expiration_date: date # Not directly used in mapping to OptionChain fields
    ) -> Dict[str, Any]:
        """Maps a single option contract snapshot to an OptionChain dictionary."""
        # Map directly from the pandas row dictionary to the OptionChain dataclass fields
        # Handle potential NaN values by letting pandas convert them to None
        mapped_data = {
            "instrument_id": instrument_id,
            "timestamp": collection_timestamp,
            "contractSymbol": snapshot_data.get('contractSymbol'),
            # yfinance lastTradeDate can be a timestamp, convert to datetime if needed
            # Assuming it's already a datetime or convertible by pandas/clickhouse-connect
            "lastTradeDate": snapshot_data.get('lastTradeDate'),
            "strike": snapshot_data.get('strike'),
            "lastPrice": snapshot_data.get('lastPrice'),
            "bid": snapshot_data.get('bid'),
            "ask": snapshot_data.get('ask'),
            "change": snapshot_data.get('change'),
            "percentChange": snapshot_data.get('percentChange'),
            "volume": snapshot_data.get('volume'),
            "openInterest": snapshot_data.get('openInterest'),
            "impliedVolatility": snapshot_data.get('impliedVolatility'),
            "inTheMoney": snapshot_data.get('inTheMoney'),
            "contractSize": snapshot_data.get('contractSize'),
            "currency": snapshot_data.get('currency'),
            # Add other fields if they exist in the yfinance data and OptionChain model
        }

        # Ensure numeric NaNs are converted to None for ClickHouse Nullable types
        # pandas to_dict() with orient='records' usually handles this, but explicit check is safer
        # This loop is a safeguard, pandas often handles this correctly for None/NaN
        for key, value in mapped_data.items():
            if pd.isna(value):
                mapped_data[key] = None

        return mapped_data


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
            - List of dictionaries for raw option chain snapshots.
        """
        option_instruments_data: List[Dict[str, Any]] = []
        option_chains_data: List[Dict[str, Any]] = [] # Use list for raw option chain data

        self.logger.info(f"Collecting options chain for ticker: {ticker_symbol}")

        try:
            # Use the centralized rate limiter before making the request
            # Call try_acquire directly. Since the limiter is configured with
            # max_delay and raise_when_fail=False, it will wait if needed.
            limiter.try_acquire(YAHOO_FINANCE_LIMIT_KEY)
            ticker = yf.Ticker(ticker_symbol)

            # Get available expiration dates
            # Apply rate limit again for the next API call
            limiter.try_acquire(YAHOO_FINANCE_LIMIT_KEY)
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

                    # Apply rate limit again for fetching option chain for a specific date
                    limiter.try_acquire(YAHOO_FINANCE_LIMIT_KEY)
                    # Get option chain for the specific date
                    option_chain = ticker.option_chain(date_str)

                    # Process calls and puts
                    calls_df = option_chain.calls
                    calls_df['contractType'] = 'Call'
                    puts_df = option_chain.puts
                    puts_df['contractType'] = 'Put'

                    # Combine and process
                    combined_df = pd.concat([calls_df, puts_df], ignore_index=True)

                    if combined_df.empty:
                        self.logger.debug(f"No option contracts found for {ticker_symbol} on {date_str}.")
                        continue

                    # Process each option contract in the chain
                    for index, row in combined_df.iterrows():
                        try:
                            # Generate unique instrument ID using the static method
                            instrument_id = self.generate_option_instrument_id(
                                underlying_ticker=ticker_symbol,
                                expiration_date=expiration_date,
                                strike=row['strike'],
                                contract_type=row['contractType']
                            )

                            # Map data for Instrument table using the static method
                            # For simplicity, we'll add it every time and rely on DB upsert
                            # to handle duplicates based on instrument_id.
                            option_instrument_data = self.map_option_snapshot_to_instrument(
                                instrument_id=instrument_id,
                                underlying_ticker=ticker_symbol,
                                expiration_date=expiration_date,
                                snapshot_data=row.to_dict() # Pass row as dict
                            )
                            option_instruments_data.append(option_instrument_data)

                            # Map data for OptionChain table (snapshot) using the static method
                            option_chain_snapshot_data = self.map_option_snapshot_to_option_chain(
                                instrument_id=instrument_id,
                                snapshot_data=row.to_dict(), # Pass row as dict
                                collection_timestamp=collection_timestamp,
                                # underlying_ticker=ticker_symbol, # Not needed by mapping function
                                # expiration_date=expiration_date # Not needed by mapping function
                            )
                            option_chains_data.append(option_chain_snapshot_data)


                        except Exception as e:
                            self.logger.error(f"Error processing option contract for {ticker_symbol} on {date_str}: {e}")
                            # Continue processing other contracts

                except Exception as e:
                    self.logger.error(f"Error fetching options for {ticker_symbol} on {date_str}: {e}")
                    # Continue with the next expiration date

        except Exception as e:
            self.logger.error(f"An error occurred during Yahoo Options chain collection for {ticker_symbol}: {e}")
            # Return whatever data was collected so far, or empty lists if error was early
            return option_instruments_data, option_chains_data # Return option_chains_data

        self.logger.info(f"Finished collecting data for {ticker_symbol}.")
        return option_instruments_data, option_chains_data # Return option_chains_data


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
        all_option_chains_data: List[Dict[str, Any]] = [] # Use list for aggregated raw option chain data
        collection_timestamp = datetime.utcnow() # Timestamp for this collection run

        # Ensure Instrument and OptionChain tables exist (optional, but good practice)
        # This requires access to the model dataclasses and DataBase.create_table
        try:
            # Need to import Option and OptionChain dataclasses
            from data_feed_collect.models import Option, OptionChain
            # Assuming 'instrument_id' is the ORDER BY key for the instruments table
            db.create_table(Option, INSTRUMENTS_TABLE_NAME, order_by=['instrument_id'], if_not_exists=True)
            # Assuming 'instrument_id' and 'timestamp' are the ORDER BY key for the option_chains table
            # Using ReplacingMergeTree to handle potential duplicate snapshots at the exact same timestamp
            db.create_table(
                OptionChain,
                OPTION_CHAINS_TABLE_NAME,
                engine='ReplacingMergeTree()', # Use ReplacingMergeTree for upsert-like behavior
                order_by=['instrument_id', 'timestamp'], # Order by instrument and timestamp
                primary_key=['instrument_id'], # Primary key can be a prefix of order_by
                if_not_exists=True
            )
            self.logger.info("Ensured Instrument and OptionChain tables exist.")
        except Exception as e:
             self.logger.error(f"Failed to ensure tables exist: {e}")
             # Decide if this is a fatal error or just log and continue


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
                    # Expecting (option_instruments, option_chains)
                    option_instruments, option_chains = future.result()

                    # Extend the main lists with data from this ticker
                    all_option_instruments_data.extend(option_instruments)
                    all_option_chains_data.extend(option_chains) # Extend option_chains_data

                    self.logger.info(f"Successfully collected data for {ticker_symbol}. Instruments: {len(option_instruments)}, Option Chains: {len(option_chains)}") # Update log message

                except Exception as exc:
                    # Log any exceptions that occurred in the worker thread
                    self.logger.error(f"Ticker {ticker_symbol} generated an exception: {exc}")

        self.logger.info("Parallel collection finished.")
        self.logger.info(f"Total instruments collected: {len(all_option_instruments_data)}")
        self.logger.info(f"Total Option Chain snapshots collected: {len(all_option_chains_data)}") # Update log message


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


        # Save raw option chain data
        if all_option_chains_data:
            self.logger.info(f"Saving {len(all_option_chains_data)} Option Chain snapshots...")
            try:
                option_chains_df = pd.DataFrame(all_option_chains_data)
                # Ensure timestamp is in the correct format if needed by upsert_dataframe
                # ClickHouse DateTime64(3) expects datetime objects or compatible strings
                # pandas datetime objects should work with clickhouse-connect
                # Assuming 'instrument_id' and 'timestamp' together form the unique key for snapshots
                # Note: This assumes you want to potentially overwrite if collecting multiple times at the exact same timestamp
                db.upsert_dataframe(option_chains_df, OPTION_CHAINS_TABLE_NAME, unique_cols=['instrument_id', 'timestamp']) # Use instrument_id and timestamp as unique key
                self.logger.info("Option Chain snapshots saved successfully.")
            except Exception as e:
                self.logger.error(f"Failed to save Option Chain snapshots: {e}")
        else:
            self.logger.info("No Option Chain snapshots to save.")

# Example usage (for testing purposes, not part of the class)
# This block would typically be orchestrated by the main DataCollector class
if __name__ == '__main__':
    # This requires a running ClickHouse instance and environment variables set
    # Also requires the Instrument table to exist and potentially contain stock tickers
    from data_feed_collect.storage.database import DataBase
    from dotenv import load_dotenv
    import os

    # Setup logging first
    setup_logging(logging.INFO)

    load_dotenv() # Load environment variables from .env

    try:
        # Initialize DB connection
        # Assumes DB connection details are in environment variables
        db_conn = DataBase()

        # Example config: fetch tickers from DB
        # config = {"fetch_tickers_from_db": True, "max_workers": 8}

        # Example config: specify tickers directly
        config = {"tickers": ["AAPL", "MSFT"], "max_workers": 2} # Use a couple tickers for testing

        collector = YahooFinanceOptionsChainCollector()
        collector.collect(config, db_conn)

    except Exception as e:
        logging.error(f"An error occurred during the example run: {e}")
    finally:
        if 'db_conn' in locals() and db_conn:
            db_conn.close()
            logging.info("Database connection closed.")
