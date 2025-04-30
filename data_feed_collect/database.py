import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus # Import for URL encoding password

# Load environment variables from .env file
load_dotenv()

# Get the database URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

# If DATABASE_URL is not set, try to construct it from individual ClickHouse variables
if not DATABASE_URL:
    ch_host = os.getenv("CLICKHOUSE_HOST")
    ch_port = os.getenv("CLICKHOUSE_PORT")
    ch_database = os.getenv("CLICKHOUSE_DATABASE")
    ch_user = os.getenv("CLICKHOUSE_USER")
    ch_password = os.getenv("CLICKHOUSE_PASSWORD")

    # Check if required ClickHouse variables are set for construction
    if ch_host and ch_port and ch_database:
        # Construct the ClickHouse URL
        # Format: clickhouse://user:password@host:port/database
        # Note: Requires the 'clickhouse-sqlalchemy' driver
        auth_part = ""
        if ch_user:
            auth_part = ch_user
            if ch_password:
                # URL encode the password in case it contains special characters
                auth_part += f":{quote_plus(ch_password)}"
            auth_part += "@"

        DATABASE_URL = f"clickhouse://{auth_part}{ch_host}:{ch_port}/{ch_database}"
        print(f"Constructed DATABASE_URL from ClickHouse variables: {DATABASE_URL}")
    else:
        # If DATABASE_URL is not set and ClickHouse variables are insufficient
        raise ValueError(
            "DATABASE_URL environment variable not set, "
            "and insufficient CLICKHOUSE_ variables (CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DATABASE) "
            "to construct the URL."
        )

# Ensure DATABASE_URL is set after attempts
if not DATABASE_URL:
     raise ValueError("Failed to determine DATABASE_URL.")


# Create the SQLAlchemy engine
# echo=True will log SQL statements (useful for debugging)
# Note: For ClickHouse, you might need specific connection arguments depending on the driver
engine = create_engine(DATABASE_URL, echo=False)

# Create a configured "Session" class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_engine():
    """
    Provides the SQLAlchemy engine instance.
    """
    return engine

def get_db():
    """
    Provides a database session.
    Use this in a 'with' statement or ensure you close the session.
    e.g., with get_db() as db: ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Note: init_schema is defined in models.py and should be called
# from an application entry point, using the engine from get_engine().
# Example:
# from data_feed_collect.models import init_schema
# from data_feed_collect.database import get_engine
# engine = get_engine()
# init_schema(engine)
