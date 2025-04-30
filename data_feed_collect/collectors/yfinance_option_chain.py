import asyncio
import yfinance as yf
from pyrate_limiter import Limiter, Rate, Duration
from sqlalchemy.orm import Session
from data_feed_collect.models import YFinanceOption
from data_feed_collect.database import get_db
import pandas as pd
from typing import List

# Define the global rate limiter
# Example: Allow up to 10 calls per minute. Adjust as needed based on yfinance limits.
# Note: yfinance limits are not officially documented and can change.
limiter = Limiter(Rate(10, Duration.MINUTE))

async def fetch_option_chain_for_date(ticker_obj: yf.Ticker, date: str):
    """
    Fetches option chain for a specific date for a yfinance Ticker object,
    applying rate limiting and running the synchronous call in a thread.
    """
    # Apply rate limiting before making the call
    async with limiter:
        # Run the synchronous yfinance call in a separate thread to avoid blocking the event loop
        print(f"Fetching data for {ticker_obj.ticker} on {date}...")
        try:
            option_chain_data = await asyncio.to_thread(ticker_obj.option_chain, date)
            print(f"Successfully fetched data for {ticker_obj.ticker} on {date}.")
            return option_chain_data
        except Exception as e:
            print(f"Error fetching data for {ticker_obj.ticker} on {date}: {e}")
            # Return None or raise the exception, depending on desired error handling
            return None


def transform_option_data(ticker_symbol: str, expiration_date: str, df: pd.DataFrame, option_type: str) -> List[YFinanceOption]:
    """
    Transforms a pandas DataFrame of option data (calls or puts) into a list
    of YFinanceOption model instances.
    """
    options = []
    if df is not None and not df.empty:
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
                    last_trade_date=row.get('lastTradeDate'),
                    change=float(row.get('change')) if pd.notna(row.get('change')) else None,
                    percent_change=float(row.get('percentChange')) if pd.notna(row.get('percentChange')) else None,
                    # Add a timestamp for when the data was collected? (Optional but good practice)
                    # collected_at=datetime.utcnow()
                )
                options.append(option)
            except Exception as e:
                print(f"Error transforming row for {ticker_symbol} {expiration_date} {option_type}: {e}\nRow data: {row.to_dict()}")
                # Decide whether to skip the row or handle the error differently
                continue
    return options

async def collect_option_chain(ticker_symbol: str):
    """
    Collects option chain data for a given ticker across all expiration dates,
    saves the data to the database.

    Uses asyncio for parallel fetching and pyrate-limiter for rate limiting.
    """
    print(f"Starting option chain collection for {ticker_symbol}...")
    ticker_obj = yf.Ticker(ticker_symbol)

    # Fetch expiration dates. This call might also benefit from rate limiting
    # if called frequently, but for a single ticker run, it's usually fine.
    try:
        # Running this in a thread just in case it makes a network call
        expiration_dates = await asyncio.to_thread(lambda: ticker_obj.options)
        if not expiration_dates:
            print(f"No expiration dates found for {ticker_symbol}. Skipping.")
            return
        print(f"Found {len(expiration_dates)} expiration dates for {ticker_symbol}.")
    except Exception as e:
        print(f"Error fetching expiration dates for {ticker_symbol}: {e}")
        return

    # Create a list of coroutines, one for fetching data for each expiration date
    fetch_tasks = [fetch_option_chain_for_date(ticker_obj, date) for date in expiration_dates]

    # Run the fetch tasks concurrently
    # return_exceptions=True allows other tasks to complete even if one fails
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    all_options_to_save: List[YFinanceOption] = []

    # Process the results from the parallel fetches
    for i, result in enumerate(results):
        date = expiration_dates[i]
        if isinstance(result, Exception):
            # Error was already printed in fetch_option_chain_for_date
            continue
        if result is None:
             # Fetch failed and returned None
             continue

        # result is the option chain data object from yfinance
        option_chain_data = result

        # Transform calls and puts dataframes into model instances
        calls_options = transform_option_data(ticker_symbol, date, option_chain_data.calls, 'call')
        puts_options = transform_option_data(ticker_symbol, date, option_chain_data.puts, 'put')

        all_options_to_save.extend(calls_options)
        all_options_to_save.extend(puts_options)

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
# import asyncio
# from data_feed_collect.models import init_schema
# from data_feed_collect.database import get_engine

async def main():
    # Ensure schema is initialized before running collectors
    # engine = get_engine()
    # init_schema(engine) # Run this once when setting up the database

    await collect_option_chain("AAPL")
    await collect_option_chain("MSFT") # Example for another ticker

if __name__ == "__main__":
    asyncio.run(main())
