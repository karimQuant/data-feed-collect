import pandas as pd
import os
import sys
from sqlalchemy.exc import SQLAlchemyError

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
    into the StocksCollection table.
    """
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: CSV file not found at {CSV_FILE_PATH}", file=sys.stderr)
        sys.exit(1)

    try:
        # Read the CSV file
        # Assuming the CSV has columns like 'Ticker' and 'Description'
        # Adjust column names if your CSV uses different headers
        df = pd.read_csv(CSV_FILE_PATH)

        # Ensure required columns exist
        required_columns = ['Ticker', 'Description'] # Adjust based on your CSV headers
        if not all(col in df.columns for col in required_columns):
            print(f"Error: CSV must contain columns: {required_columns}", file=sys.stderr)
            sys.exit(1)

        print(f"Reading data from {CSV_FILE_PATH}...")
        print(f"Found {len(df)} rows.")

        # Get a database session
        db = next(get_db()) # Use next() to get the session generator's value

        inserted_count = 0
        updated_count = 0
        skipped_count = 0

        try:
            for index, row in df.iterrows():
                ticker = row['Ticker']
                description = row['Description'] # Adjust column name

                # Check if the stock already exists
                existing_stock = db.query(StocksCollection).filter(StocksCollection.ticker == ticker).first()

                if existing_stock:
                    # Optionally update existing record if description changes
                    if existing_stock.company_description != description: # Corrected column name to company_description
                        existing_stock.company_description = description # Corrected column name to company_description
                        updated_count += 1
                        # print(f"Updating {ticker}") # Uncomment for verbose output
                    else:
                        skipped_count += 1
                        # print(f"Skipping {ticker} (already exists and matches)") # Uncomment for verbose output
                else:
                    # Create a new StocksCollection instance
                    new_stock = StocksCollection(
                        ticker=ticker,
                        company_description=description # Corrected column name to company_description
                        # Add other fields if necessary based on your model
                    )
                    db.add(new_stock)
                    inserted_count += 1
                    # print(f"Inserting {ticker}") # Uncomment for verbose output

            # Commit the transaction
            db.commit()
            print("\nDatabase transaction committed successfully.")
            print(f"Summary: Inserted {inserted_count}, Updated {updated_count}, Skipped {skipped_count}")

        except SQLAlchemyError as e:
            db.rollback() # Roll back the transaction on error
            print(f"Database error occurred: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            db.rollback() # Roll back on other errors too
            print(f"An unexpected error occurred: {e}", file=sys.stderr)
            sys.exit(1)
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
    insert_qqq_composition()
