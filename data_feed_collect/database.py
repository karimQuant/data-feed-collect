import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Load environment variables from .env file
load_dotenv()

# Get the database URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    # Fallback or raise an error if DATABASE_URL is not set
    # For now, let's raise an error to ensure it's configured
    raise ValueError("DATABASE_URL environment variable not set.")

# Create the SQLAlchemy engine
# echo=True will log SQL statements (useful for debugging)
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
