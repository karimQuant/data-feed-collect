import pandas as pd
import os
import sys
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm.exc import FlushError # Import FlushError

# Assuming data_feed_collect is in the Python path or accessible
# Add the parent directory of data_feed_collect to the path if necessary
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_feed_collect.database import get_db
from data_feed_collect.models import StocksCollection

# Define the path to the CSV file
# Assuming the script is run from the project root or scripts directory
CSV_FILE_PATH = 'data/qqq_composition.csv'

def insert_qqq_composition():
    """
    Reads the QQQ composition CSV and inserts/updates stock information
    into the StocksCollection table using db.merge().
    """
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: CSV file not found at {CSV_FILE_PATH}", file=sys.stderr)
        sys.exit(1)

    try:
        df = pd.read_csv(CSV_FILE_PATH)

        required_columns = ['Ticker', 'Description'] # Adjust based on your CSV headers
        if not all(col in df.columns for col in required_columns):
            print(f"Error: CSV must contain columns: {required_columns}", file=sys.stderr)
            sys.exit(1)

        print(f"Reading data from {CSV_FILE_PATH}...")
        print(f"Found {len(df)} rows.")

        db = next(get_db())

        processed_count = 0

        try:
            for index, row in df.iterrows():
                ticker = row['Ticker']
                description = row['Description']

                # Create a StocksCollection instance with the data
                # We don't set the 'id' as it's auto-generated.
                # merge() will use the 'ticker' unique constraint to find existing rows.
                stock_data = StocksCollection(
                    ticker=ticker,
                    company_description=description
                )

                # Use merge() which handles both insert and update based on unique constraints.
                # It returns the object as it exists in the session after the merge.
                # If an object with the same ticker exists, it's loaded and updated.
                # If not, a new object is prepared for insertion.
                db.merge(stock_data)
                processed_count += 1
                # print(f"Processed ticker: {ticker}") # Uncomment for verbose output

            # Commit the transaction
            # This is where the batched inserts/updates are executed.
            db.commit()
            print("\nDatabase transaction committed successfully.")
            print(f"Summary: Processed {processed_count} rows (inserted or updated).")

        except SQLAlchemyError as e:
            db.rollback() # Roll back the transaction on error
            print(f"Database error occurred: {e}", file=sys.stderr)
            # Re-raise the exception to ensure the script exits with an error code
            raise
        except Exception as e:
            db.rollback() # Roll back on other errors too
            print(f"An unexpected error occurred: {e}", file=sys.stderr)
            # Re-raise the exception
            raise
        finally:
            db.close() # Close the session

    except FileNotFoundError:
        print(f"Error: CSV file not found at {CSV_FILE_PATH}", file=sys.stderr)
        sys.exit(1)
    except pd.errors.EmptyDataError:
        print(f"Error: CSV file is empty at {CSV_FILE_PATH}", file=sys.stderr)
        sys.exit(1)
    except pd.errors.ParserError:
        print(f"Error: Could not parse CSV file at {CSV_FILE_PATH}. Check format.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during file processing: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    # Check for necessary environment variables
    if not os.getenv("DATABASE_URL") and not (os.getenv("POSTGRES_HOST") and os.getenv("POSTGRES_PORT") and os.getenv("POSTGRES_DATABASE")):
         print("Error: DATABASE_URL or sufficient POSTGRES_ environment variables are not set.", file=sys.stderr)
         sys.exit(1)

    try:
        insert_qqq_composition()
    except Exception:
        # Catch exceptions re-raised from insert_qqq_composition to ensure non-zero exit code
        sys.exit(1)
