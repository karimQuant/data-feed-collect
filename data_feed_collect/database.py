import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus # Import for URL encoding password

# Load environment variables from .env file
load_dotenv()

# Get the database URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

# If DATABASE_URL is not set, try to construct it from individual PostgreSQL variables
if not DATABASE_URL:
    pg_host = os.getenv("POSTGRES_HOST")
    pg_port = os.getenv("POSTGRES_PORT")
    pg_database = os.getenv("POSTGRES_DATABASE")
    pg_user = os.getenv("POSTGRES_USER")
    pg_password = os.getenv("POSTGRES_PASSWORD")

    # Check if required PostgreSQL variables are set for construction
    if pg_host and pg_port and pg_database:
        # Construct the PostgreSQL URL
        # Format: postgresql://user:password@host:port/database
        # Note: Requires the 'psycopg2' or 'pg8000' driver (or similar)
        auth_part = ""
        if pg_user:
            auth_part = pg_user
            if pg_password:
                # URL encode the password in case it contains special characters
                auth_part += f":{quote_plus(pg_password)}"
            auth_part += "@"

        DATABASE_URL = f"postgresql://{auth_part}{pg_host}:{pg_port}/{pg_database}"
        print(f"Constructed DATABASE_URL from PostgreSQL variables: {DATABASE_URL}")
    else:
        # If DATABASE_URL is not set and PostgreSQL variables are insufficient
        raise ValueError(
            "DATABASE_URL environment variable not set, "
            "and insufficient POSTGRES_ variables (POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE) "
            "to construct the URL."
        )

# Ensure DATABASE_URL is set after attempts
if not DATABASE_URL:
     raise ValueError("Failed to determine DATABASE_URL.")


# Create the SQLAlchemy engine
# echo=True will log SQL statements (useful for debugging)
# Note: For PostgreSQL, ensure you have a compatible driver installed (e.g., psycopg2)
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
