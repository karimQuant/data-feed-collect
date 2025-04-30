import threading
import concurrent.futures
import yfinance as yf
from pyrate_limiter import Limiter, Rate, Duration, BucketFactory, AbstractBucket, RateItem, InMemoryBucket
from sqlalchemy.orm import Session
from data_feed_collect.models import YFinanceOption
from data_feed_collect.database import get_db
import pandas as pd
from typing import List, Optional
from datetime import datetime
import time # Needed for TimeClock

# Define the rate limits
# Example: Allow up to 50 calls per minute. Adjust as needed based on yfinance limits.
# Note: yfinance limits are not officially documented and can change.
# We'll use a single rate for simplicity, applied globally across all tickers/calls.
rate = Rate(50, Duration.MINUTE)
rates = [rate]

# Define a simple BucketFactory that uses a single InMemoryBucket
# This aligns with the documentation's structure for using a Limiter with a factory.
class SingleBucketFactory(BucketFactory):
    def __init__(self, rates: List[Rate]):
        # Create a single InMemoryBucket instance with the defined rates
        self._bucket = InMemoryBucket(rates)
        # Use the default TimeClock (which uses time.time())
        self._clock = time

        # The Limiter instance is responsible for calling schedule_leak on the factory's buckets.
        # We don't need to call schedule_leak manually here.

    def wrap_item(self, name: str, weight: int = 1) -> RateItem:
        """Time-stamping item, return a RateItem"""
        # Use the factory's clock to get the current timestamp
        now = self._clock.time()
        return RateItem(name, now, weight=weight)

    def get(self, _item: RateItem) -> AbstractBucket:
        """Route all items to the single bucket"""
        # For this simple case, all items go to the same bucket
        return self._bucket

# Instantiate the BucketFactory with the defined rates
bucket_factory = SingleBucketFactory(rates)

# Instantiate the Limiter with the BucketFactory
# Set max_delay to float('inf') to make try_acquire block indefinitely if the rate limit is hit.
# This replicates the blocking behavior of the previous limiter.block() usage.
# raise_when_fail=True (default) means it will raise LimiterDelayException if the required
# wait time exceeds max_delay. With max_delay=inf, this exception will not be raised.
limiter = Limiter(bucket_factory, max_delay=float('inf'))

# Define a constant item name for the global rate limit
# Using a constant name ensures all calls contribute to the same bucket/rate limit.
GLOBAL_YFINANCE_ITEM_NAME = "yfinance_api_call"


def fetch_option_chain_for_date(ticker_obj: yf.Ticker, date: str):
    """
    Fetches option chain for a specific date for a yfinance Ticker object,
    applying rate limiting. This function is designed to be run in a thread.
    """
    ticker_symbol = ticker_obj.ticker # Get ticker symbol for logging

    # Apply rate limiting before making the call
    # Use try_acquire with the constant item name for the global limit.
    # Since max_delay is set to inf on the limiter, this will block if necessary.
    try:
        limiter.try_acquire(GLOBAL_YFINANCE_ITEM_NAME)
    except Exception as e:
        # This catch is mostly for unexpected errors from try_acquire,
        # as max_delay=inf should prevent LimiterDelayException.
        print(f"Error acquiring rate limit for {ticker_symbol} on {date}: {e}")
        return date, None # Treat rate limit error as a fetch failure

    print(f"Fetching data for {ticker_obj.ticker} on {date}...")
    try:
        # yfinance calls are synchronous
        option_chain_data = ticker_obj.option_chain(date)
        print(f"Successfully fetched data for {ticker_obj.ticker} on {date}.")
        return date, option_chain_data # Return date along with data to identify result
    except Exception as e:
        print(f"Error fetching data for {ticker_obj.ticker} on {date}: {e}")
        # Return date even on error
        return date, None


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
                    symbol=ticker_symbol,
                    expiration_date=expiration_date,
                    option_type=option_type, # 'call' or 'put'
                    contract_symbol=row.get('contractSymbol'),
                    strike=float(row.get('strike')) if pd.notna(row.get('strike')) else None,
                    last_price=float(row.get('lastPrice')) if pd.notna(row.get('lastPrice')) else None,
                    bid=float(row.get('bid')) if pd.notna(row.get('bid')) else None,
                    ask=float(row.get('ask')) if pd.notna(row.get('ask')) else None,
                    volume=int(row.get('volume')) if pd.notna(row.get('volume')) else None,
                    open_interest=int(row.get('openInterest')) if pd.notna(row.get('openInterest')) else None,
                    implied_volatility=float(row.get('impliedVolatility')) if pd.notna(row.get('impliedVolatility')) else None,
                    in_the_money=bool(row.get('inTheMoney')) if pd.notna(row.get('inTheMoney')) else None,
                    contract_size=row.get('contractSize'), # Assuming string or similar
                    currency=row.get('currency'), # Assuming string
                    # Handle date conversion if necessary. yfinance often returns datetime objects.
                    # Ensure lastTradeDate is converted to a standard Python datetime if needed by SQLAlchemy model
                    last_trade_date=row.get('lastTradeDate'), # Assuming this is already a datetime or compatible
                    change=float(row.get('change')) if pd.notna(row.get('change')) else None,
                    percent_change=float(row.get('percentChange')) if pd.notna(row.get('percentChange')) else None,
                    collected_at=collected_at # Add the collection timestamp
                )
                options.append(option)
            except Exception as e:
                print(f"Error transforming row for {ticker_symbol} {expiration_date} {option_type}: {e}\nRow data: {row.to_dict()}")
                # Decide whether to skip the row or handle the error differently
                continue
    return options

def collect_option_chain(ticker_symbol: str):
    """
    Collects option chain data for a given ticker across all expiration dates,
    saves the data to the database.

    Uses threading for parallel fetching and pyrate-limiter for rate limiting.
    """
    print(f"Starting option chain collection for {ticker_symbol}...")
    ticker_obj = yf.Ticker(ticker_symbol)

    # Fetch expiration dates. This call is synchronous.
    try:
        # Apply rate limiting to the initial options call as well
        # Use try_acquire with the constant item name for the global limit.
        # Since max_delay is set to inf on the limiter, this will block if necessary.
        limiter.try_acquire(GLOBAL_YFINANCE_ITEM_NAME)
        expiration_dates = ticker_obj.options
        if not expiration_dates:
            print(f"No expiration dates found for {ticker_symbol}. Skipping.")
            return
        print(f"Found {len(expiration_dates)} expiration dates for {ticker_symbol}.")
    except Exception as e:
        # Catch potential errors from try_acquire or ticker_obj.options
        print(f"Error fetching expiration dates for {ticker_symbol}: {e}")
        return

    all_options_to_save: List[YFinanceOption] = []

    # Use ThreadPoolExecutor to fetch data for different expiration dates in parallel
    # Max workers can be adjusted based on system resources and desired concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit tasks to the executor
        future_to_date = {executor.submit(fetch_option_chain_for_date, ticker_obj, date): date for date in expiration_dates}

        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_date):
            date = future_to_date[future]
            try:
                # Get the result from the future (this will raise exceptions if the task failed)
                # The result is a tuple: (date, option_chain_data or None)
                fetched_date, option_chain_data = future.result()

                if option_chain_data is None:
                    # Error was already printed in fetch_option_chain_for_date
                    continue

                # Transform calls and puts dataframes into model instances
                calls_options = transform_option_data(ticker_symbol, fetched_date, option_chain_data.calls, 'call')
                puts_options = transform_option_data(ticker_symbol, fetched_date, option_chain_data.puts, 'put')

                all_options_to_save.extend(calls_options)
                all_options_to_save.extend(puts_options)

            except Exception as e:
                print(f"Error processing result for {ticker_symbol} on {date}: {e}")
                # Continue processing other results even if one fails

    print(f"Finished fetching and transforming data for {ticker_symbol}. Collected {len(all_options_to_save)} option contracts.")

    # Save all collected option contracts to the database in a single transaction
    if all_options_to_save:
        db: Session = None
        try:
            # Get a database session using the generator pattern
            db = next(get_db())
            print(f"Saving {len(all_options_to_save)} option contracts to database for {ticker_symbol}...")
            # Use bulk_save_objects for efficiency when saving many objects
            db.bulk_save_objects(all_options_to_save)
            db.commit()
            print(f"Successfully saved data for {ticker_symbol}.")
        except Exception as e:
            print(f"Error saving data to database for {ticker_symbol}: {e}")
            if db:
                db.rollback() # Roll back the transaction on error
        finally:
            if db:
                db.close() # Close the session
    else:
        print(f"No option contracts to save for {ticker_symbol}.")

# Example of how you might run this function (e.g., in a main script or another module)
# import asyncio # No longer needed for this version
from data_feed_collect.models import init_schema
from data_feed_collect.database import get_engine

def main():
    # Ensure schema is initialized before running collectors
    # engine = get_engine()
    # init_schema(engine) # Run this once when setting up the database

    collect_option_chain("AAPL")
    collect_option_chain("MSFT") # Example for another ticker

if __name__ == "__main__":
    main() # Call the synchronous main function
