import threading
import concurrent.futures
import yfinance as yf
# Remove pyrate-limiter imports
# from pyrate_limiter import Limiter, Rate, Duration, BucketFactory, AbstractBucket, RateItem, InMemoryBucket
# from ratelimiter import RateLimiter # Import ratelimiter - REMOVED
from limiter import Limiter # Import limiter
from sqlalchemy.orm import Session
from data_feed_collect.models import YFinanceOption, StocksCollection
from data_feed_collect.database import get_db
import pandas as pd
from typing import List, Optional
from datetime import datetime
import time # Needed for TimeClock (though less critical with ratelimiter decorator)
import logging
from data_feed_collect.logging_config import setup_logging
import backoff

# Get a logger instance for this module
logger = logging.getLogger(__name__)

# Define the rate limits using limiter parameters
# 50 calls per minute -> rate = 50/60 calls per second
YFINANCE_RATE_PER_SECOND = 50 / 60
YFINANCE_CAPACITY = 5 # Example burst capacity

# Remove pyrate-limiter setup
# rate = Rate(50, Duration.MINUTE)
# rates = [rate]
# class SingleBucketFactory(BucketFactory):
#     def __init__(self, rates: List[Rate]):
#         self._bucket = InMemoryBucket(rates)
#         self._clock = time
#     def wrap_item(self, name: str, weight: int = 1) -> RateItem:
#         now = self._clock.time()
#         return RateItem(name, now, weight=1)
#     def get(self, _item: RateItem) -> AbstractBucket:
#         return self._bucket
# bucket_factory = SingleBucketFactory(rates)
# limiter = Limiter(bucket_factory, max_delay=float('inf'))
# GLOBAL_YFINANCE_ITEM_NAME = "yfinance_api_call"

# Create a global Limiter instance
yfinance_limiter = Limiter(rate=YFINANCE_RATE_PER_SECOND, capacity=YFINANCE_CAPACITY)


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

    # Remove pyrate-limiter acquire call
    # try:
    #     limiter.try_acquire(GLOBAL_YFINANCE_ITEM_NAME)
    # except Exception as e:
    #     logger.error(f"Error acquiring rate limit for {ticker_symbol} on {date}: {e}")
    #     raise

    logger.info(f"Fetching data for {ticker_obj.ticker} on {date}...")
    # The limiter decorator handles waiting if the rate limit is hit.
    # The backoff decorator handles retries if an exception occurs here after waiting.
    option_chain_data = ticker_obj.option_chain(date)
    logger.info(f"Successfully fetched data for {ticker_obj.ticker} on {date}.")
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
    expiration_dates = ticker_obj.options
    logger.info(f"Successfully fetched expiration dates for {ticker_obj.ticker}.")
    return expiration_dates


def transform_option_data(ticker_symbol: str, expiration_date: str, df: pd.DataFrame, option_type: str) -> List[YFinanceOption]:
    """
    Transforms a pandas DataFrame of option data (calls or puts) into a list
    of YFinanceOption model instances.
    """
    options = []
    if df is not None and not df.empty:
        # Add a timestamp for when the data was collected
        collected_at = datetime.utcnow()
        for _, row in df.iterrows():
            try:
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
                    data_collected_timestamp=collected_at # Corrected attribute name
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
    """
    logger.info(f"Starting option chain collection for {ticker_symbol}...")
    ticker_obj = yf.Ticker(ticker_symbol)

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
                # This call might re-raise if transformation fails for any row
                calls_options = transform_option_data(ticker_symbol, fetched_date, option_chain_data.calls, 'call')
                puts_options = transform_option_data(ticker_symbol, fetched_date, option_chain_data.puts, 'put')

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
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit tasks to the executor
        # collect_option_chain itself doesn't have the rate limit decorator,
        # but the functions it calls (_fetch_ticker_options and fetch_option_chain_for_date) do.
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
    main() # Call the synchronous main function
