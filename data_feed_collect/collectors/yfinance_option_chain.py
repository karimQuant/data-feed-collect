import threading
import concurrent.futures
import yfinance as yf
import requests
from curl_cffi import requests
# Remove pyrate-limiter imports
# from pyrate_limiter import Limiter, Rate, Duration, BucketFactory, AbstractBucket, RateItem, InMemoryBucket
# from ratelimiter import RateLimiter # Import ratelimiter - REMOVED
from limiter import Limiter # Import limiter
from sqlalchemy.orm import Session
from data_feed_collect.models import YFinanceOption, StocksCollection
from data_feed_collect.database import get_db
import pandas as pd
from typing import List, Optional, Dict
from datetime import datetime
import time # Needed for TimeClock (though less critical with ratelimiter decorator)
import logging
from data_feed_collect.logging_config import setup_logging
import backoff
import os # Import os to read environment variables
import itertools # Import itertools for cycling proxies

# Get a logger instance for this module
logger = logging.getLogger(__name__)

# Define the rate limits using limiter parameters
# 50 calls per minute -> rate = 50/60 calls per second
YFINANCE_RATE_PER_SECOND = 50 / 60
YFINANCE_CAPACITY = 5 # Example burst capacity

# Create a global Limiter instance
yfinance_limiter = Limiter(rate=YFINANCE_RATE_PER_SECOND, capacity=YFINANCE_CAPACITY)

# --- Proxy Management ---

# Read proxies from environment variable DECODO_PROXIES
# Expected format: "http://user:pass@host:port,http://user2:pass2@host2:port2"
def get_proxies_from_env() -> List[str]:
    """Reads a comma-separated list of proxies from the DECODO_PROXIES environment variable."""
    proxies_str = os.getenv("DECODO_PROXIES")
    if not proxies_str:
        logger.info("DECODO_PROXIES environment variable not set. No proxies will be used.")
        return []
    proxies = [p.strip() for p in proxies_str.split(',') if p.strip()]
    logger.info(f"Loaded {len(proxies)} proxies from DECODO_PROXIES.")
    return proxies

# Global list of proxies and an iterator for cycling
AVAILABLE_PROXIES = get_proxies_from_env()
yf.set_config(proxy=f'{AVAILABLE_PROXIES[0]}')





@backoff.on_exception(backoff.expo,
                      Exception, # Retry on any Exception. Refine this if specific errors are known.
                      max_time=180, # Max retry duration in seconds (3 minutes)
                      logger=logger) # Use the module logger for backoff messages
# Apply the new limiter decorator
@yfinance_limiter
def fetch_option_chain_for_date(ticker_obj: yf.Ticker, date: str):
    """
    Fetches option chain for a specific date for a yfinance Ticker object,
    applying rate limiting and exponential backoff on errors.
    This function is designed to be run in a thread.
    """
    ticker_symbol = ticker_obj.ticker # Get ticker symbol for logging

    logger.info(f"Fetching option chain data for {ticker_obj.ticker} on {date}...")
    # The limiter decorator handles waiting if the rate limit is hit.
    # The backoff decorator handles retries if an exception occurs here after waiting.
    # The proxy is already configured on the ticker_obj instance.
    option_chain_data = ticker_obj.option_chain(date)
    logger.info(f"Successfully fetched option chain data for {ticker_obj.ticker} on {date}.")
    return date, option_chain_data # Return date along with data to identify result

@backoff.on_exception(backoff.expo,
                      Exception, # Retry on any Exception. Refine this if specific errors are known.
                      max_time=180, # Max retry duration in seconds (3 minutes)
                      logger=logger) # Use the module logger for backoff messages
# Apply the new limiter decorator
@yfinance_limiter
def _fetch_ticker_options(ticker_obj: yf.Ticker) -> List[str]:
    """
    Fetches expiration dates for a yfinance Ticker object,
    applying rate limiting and exponential backoff on errors.
    """
    logger.info(f"Fetching expiration dates for {ticker_obj.ticker}...")
    # The limiter decorator handles waiting if the rate limit is hit.
    # The backoff decorator handles retries if an exception occurs here after waiting.
    # The proxy is already configured on the ticker_obj instance.
    expiration_dates = ticker_obj.options
    logger.info(f"Successfully fetched expiration dates for {ticker_obj.ticker}.")
    return expiration_dates

@backoff.on_exception(backoff.expo,
                      Exception, # Retry on any Exception. Refine this if specific errors are known.
                      max_time=180, # Max retry duration in seconds (3 minutes)
                      logger=logger) # Use the module logger for backoff messages
# Apply the new limiter decorator
@yfinance_limiter
def _fetch_current_stock_price(ticker_obj: yf.Ticker) -> Optional[float]:
    """
    Fetches the current stock price for a yfinance Ticker object,
    applying rate limiting and exponential backoff on errors.
    """
    ticker_symbol = ticker_obj.ticker
    logger.info(f"Fetching current price for {ticker_symbol}...")
    try:
        # Fetching info is a common way to get current price
        # The proxy is already configured on the ticker_obj instance.
        info = ticker_obj.info
        # yfinance info dictionary keys can vary, 'currentPrice' is common
        current_price = info.get('currentPrice')
        if current_price is not None:
             logger.info(f"Successfully fetched current price for {ticker_symbol}: {current_price}")
             return float(current_price) # Ensure it's a float
        else:
             logger.warning(f"Could not find 'currentPrice' in info for {ticker_symbol}. Info keys: {info.keys()}")
             return None
    except Exception as e:
        logger.error(f"Error fetching current price for {ticker_symbol}: {e}")
        # Re-raise the exception to trigger backoff
        raise


def transform_option_data(ticker_symbol: str, expiration_date: str, df: pd.DataFrame, option_type: str, underlying_price: Optional[float]) -> List[YFinanceOption]:
    """
    Transforms a pandas DataFrame of option data (calls or puts) into a list
    of YFinanceOption model instances, including the underlying price and calculated mid-price.
    """
    options = []
    if df is not None and not df.empty:
        # Add a timestamp for when the data was collected
        collected_at = datetime.utcnow()
        for _, row in df.iterrows():
            try:
                # Calculate mid_price: (bid + ask) / 2
                # Handle cases where bid or ask might be None/NaN
                bid = row.get('bid')
                ask = row.get('ask')
                mid_price = None
                if pd.notna(bid) and pd.notna(ask):
                    try:
                        mid_price = (float(bid) + float(ask)) / 2.0
                    except (ValueError, TypeError):
                        logger.warning(f"Could not calculate mid_price for {ticker_symbol} {expiration_date} {option_type} due to invalid bid/ask values: bid={bid}, ask={ask}")
                        mid_price = None # Ensure mid_price is None if calculation fails


                # Map DataFrame columns to YFinanceOption model attributes
                # Use .get() to safely access columns that might be missing
                # Convert pandas types (like numpy.int64, numpy.float64) to Python types
                option = YFinanceOption(
                    ticker=ticker_symbol,
                    expiration=pd.to_datetime(expiration_date).to_pydatetime(), # Keep as DateTime
                    optionType=option_type,
                    contractSymbol=row.get('contractSymbol'),
                    strike=float(row.get('strike')) if pd.notna(row.get('strike')) else None,
                    lastPrice=float(row.get('lastPrice')) if pd.notna(row.get('lastPrice')) else None, # Corrected attribute name
                    bid=float(row.get('bid')) if pd.notna(row.get('bid')) else None,
                    ask=float(row.get('ask')) if pd.notna(row.get('ask')) else None,
                    volume=int(row.get('volume')) if pd.notna(row.get('volume')) else None,
                    openInterest=int(row.get('openInterest')) if pd.notna(row.get('openInterest')) else None, # Corrected attribute name
                    impliedVolatility=float(row.get('impliedVolatility')) if pd.notna(row.get('impliedVolatility')) else None, # Corrected attribute name
                    inTheMoney=bool(row.get('inTheMoney')) if pd.notna(row.get('inTheMoney')) else False,
                    contractSize=row.get('contractSize'), # Corrected attribute name
                    currency=row.get('currency'), # Assuming string
                    # Handle date conversion if necessary. yfinance often returns datetime objects.
                    # Ensure lastTradeDate is converted to a standard Python datetime if needed by SQLAlchemy model
                    lastTradeDate=pd.to_datetime(row.get('lastTradeDate')) if pd.notna(row.get('lastTradeDate')) else None,
                    change=float(row.get('change')) if pd.notna(row.get('change')) else None,
                    percentChange=float(row.get('percentChange')) if pd.notna(row.get('percentChange')) else None, # Corrected attribute name
                    data_collected_timestamp=collected_at, # Corrected attribute name
                    underlying_price=underlying_price, # Add the underlying price
                    mid_price=mid_price # Add the calculated mid price
                )
                options.append(option)
            except Exception as e:
                logger.error(f"Error transforming row for {ticker_symbol} {expiration_date} {option_type}: {e}\nRow data: {row.to_dict()}")
                # Log and re-raise the error during transformation
                raise # Re-raise the exception

    return options


def collect_option_chain(ticker_symbol: str):
    """
    Collects option chain data for a given ticker across all expiration dates,
    saves the data to the database.

    Uses threading for parallel fetching and limiter for rate limiting.
    Fetches underlying price once per ticker.
    Uses a proxy if available.
    """
    logger.info(f"Starting option chain collection for {ticker_symbol}...")

    session = requests.Session(impersonate="chrome",proxy=AVAILABLE_PROXIES[0])
    ticker_obj = yf.Ticker(ticker_symbol,session)

    # Fetch the current underlying stock price first
    try:
        underlying_price = _fetch_current_stock_price(ticker_obj)
        if underlying_price is None:
             logger.warning(f"Could not fetch underlying price for {ticker_symbol}. Option data will be collected without it.")
             # Continue with None if price couldn't be fetched
    except Exception as e:
        # Catch potential errors from _fetch_current_stock_price (after backoff attempts)
        logger.error(f"Error fetching underlying price for {ticker_symbol}: {e}")
        # Decide whether to stop or continue without price.
        # For now, we'll log and re-raise, stopping collection for this ticker.
        # If you want to continue without the price, remove the 'raise' below.
        raise
        # underlying_price = None # Uncomment this line if you want to continue without the price


    # Fetch expiration dates using the new helper function with decorators
    try:
        expiration_dates = _fetch_ticker_options(ticker_obj)
        if not expiration_dates:
            logger.info(f"No expiration dates found for {ticker_symbol}. Skipping.")
            return
        logger.info(f"Found {len(expiration_dates)} expiration dates for {ticker_symbol}.")
    except Exception as e:
        # Catch potential errors from _fetch_ticker_options (after backoff attempts)
        logger.error(f"Error fetching expiration dates for {ticker_symbol}: {e}")
        # Log and re-raise the error
        raise

    all_options_to_save: List[YFinanceOption] = []

    # Use ThreadPoolExecutor to fetch data for different expiration dates in parallel
    # Max workers can be adjusted based on system resources and desired concurrency
    # Note: The limiter decorator is applied to the function, so each call
    # to fetch_option_chain_for_date will respect the global rate limit.
    # The ticker_obj instance (with its configured proxy) is passed to the threads.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit tasks to the executor
        # The fetch_option_chain_for_date function now has backoff and Limiter built-in
        future_to_date = {executor.submit(fetch_option_chain_for_date, ticker_obj, date): date for date in expiration_dates}

        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_date):
            # Corrected variable name from future_to_ticker to future_to_date
            date = future_to_date[future]
            try:
                # Get the result from the future (this will re-raise exceptions
                # that occurred in the thread, including those handled/retried by backoff
                # if max_time is reached or backoff is exhausted)
                fetched_date, option_chain_data = future.result()

                if option_chain_data is None:
                    # This case should be less likely now with backoff, but kept for safety.
                    # If fetch_option_chain_for_date returned None (e.g., due to an error
                    # that wasn't re-raised, though we've changed that now), skip.
                    # With the changes above, future.result() will likely raise instead.
                    continue

                # Transform calls and puts dataframes into model instances
                # Pass the fetched underlying_price to the transform function
                calls_options = transform_option_data(ticker_symbol, fetched_date, option_chain_data.calls, 'call', underlying_price)
                puts_options = transform_option_data(ticker_symbol, fetched_date, option_chain_data.puts, 'put', underlying_price)

                all_options_to_save.extend(calls_options)
                all_options_to_save.extend(puts_options)

            except Exception as e:
                # Catch exceptions re-raised from fetch_option_chain_for_date (after backoff attempts)
                # or from transform_option_data
                logger.error(f"Error processing result for {ticker_symbol} on {date}: {e}")
                # Log and re-raise the error. This will stop the as_completed loop
                # and the exception will propagate out of the 'with executor:' block.
                # If you wanted to allow other tickers/dates to continue, you would
                # catch the exception here and *not* re-raise it, perhaps just logging it.
                # For now, we'll let it stop the current ticker's processing.
                raise

    logger.info(f"Finished fetching and transforming data for {ticker_symbol}. Collected {len(all_options_to_save)} option contracts.")

    # Save all collected option contracts to the database in a single transaction
    if all_options_to_save:
        db: Session = None
        try:
            # Get a database session using the generator pattern
            db = next(get_db())
            logger.info(f"Saving {len(all_options_to_save)} option contracts to database for {ticker_symbol}...")
            # Use bulk_save_objects for efficiency when saving many objects
            db.bulk_save_objects(all_options_to_save)
            db.commit()
            logger.info(f"Successfully saved data for {ticker_symbol}.")
        except Exception as e:
            logger.error(f"Error saving data to database for {ticker_symbol}: {e}")
            if db:
                db.rollback() # Roll back the transaction on error
            # Log and re-raise the database error
            raise
        finally:
            if db:
                db.close() # Close the session
    else:
        logger.info(f"No option contracts to save for {ticker_symbol}.")

def database_update():
    """
    Queries the StocksCollection table for tickers and collects option chain
    data for each ticker in parallel.
    """
    logger.info("Starting database update for option chains...")
    db: Session = None
    tickers_to_collect = []
    try:
        # Get a database session
        db = next(get_db())
        # Query all tickers from the StocksCollection table
        stocks = db.query(StocksCollection).all()
        tickers_to_collect = [stock.ticker for stock in stocks]
        logger.info(f"Found {len(tickers_to_collect)} tickers to collect from database.")
    except Exception as e:
        logger.error(f"Error querying database for tickers: {e}")
        # Log and re-raise the error
        raise
    finally:
        if db:
            db.close() # Close the session

    if not tickers_to_collect:
        logger.info("No tickers found in StocksCollection. Database update finished.")
        return

    # Use ThreadPoolExecutor to run collect_option_chain for each ticker in parallel
    # Set max_workers to 5 as requested.
    # Note: The limiter decorator is applied to the functions making API calls,
    # ensuring the global rate limit is respected across all threads/tickers.
    # Each call to collect_option_chain will get its own proxy from the cycle.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit tasks to the executor
        # collect_option_chain itself doesn't have the rate limit decorator,
        # but the functions it calls (_fetch_ticker_options, fetch_option_chain_for_date, _fetch_current_stock_price) do.
        future_to_ticker = {executor.submit(collect_option_chain, ticker): ticker for ticker in tickers_to_collect}

        # Process results as they complete (or handle exceptions)
        for future in concurrent.futures.as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                # Get the result from the future. This will re-raise any exceptions
                # that occurred within the collect_option_chain function for that ticker.
                future.result()
                logger.info(f"Successfully completed collection for {ticker}.")
            except Exception as e:
                # Catch exceptions re-raised from collect_option_chain
                logger.error(f"Collection failed for {ticker}: {e}")
                # Decide how to handle failures:
                # - Just log and continue with other tickers (current behavior)
                # - Re-raise to stop the entire process (uncomment the next line)
                # raise

    logger.info("Database update for option chains finished.")


def main():
    # Setup logging first
    setup_logging()
    # Call the new database_update function
    database_update()


if __name__ == "__main__":
    # main() # Call the synchronous main function
    collect_option_chain("AAPL") # Test with a single ticker
