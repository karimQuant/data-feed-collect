from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, DateTime
from sqlalchemy.orm import declarative_base
from datetime import datetime
from data_feed_collect.database import get_engine # Import get_engine
# No need to import Table from sqlalchemy.schema for this approach

# Define the base for declarative models
Base = declarative_base()

class YFinanceOption(Base):
    """
    SQLAlchemy model for storing Yahoo Finance option chain data.
    """
    __tablename__ = 'yfinance_options'

    id = Column(Integer, primary_key=True, autoincrement=True)
    contractSymbol = Column(String, index=True)
    ticker = Column(String, index=True)
    data_collected_timestamp = Column(DateTime, index=True) # Timestamp when data was collected

    # Option specific data fields from Yahoo Finance
    strike = Column(Float, nullable=False)
    currency = Column(String, nullable=False)
    lastPrice = Column(Float) # Can be None if no trades
    change = Column(Float)
    percentChange = Column(Float)
    volume = Column(Integer) # Can be None
    openInterest = Column(Integer) # Can be None
    bid = Column(Float) # Can be None
    ask = Column(Float) # Can be None
    contractSize = Column(String, nullable=False) # e.g., 'REGULAR'
    expiration = Column(DateTime, nullable=False) # Stored as DateTime type
    lastTradeDate = Column(DateTime) # Unix timestamp of last trade date, can be None
    impliedVolatility = Column(Float) # Can be None
    inTheMoney = Column(Boolean, nullable=False)
    optionType = Column(String, nullable=False) # 'call' or 'put'

    __table_args__ = (
        {'comment': 'Yahoo Finance Option Chain Data'},
    )


    def __repr__(self):
        return (f"<YFinanceOption(contractSymbol='{self.contractSymbol}', ticker='{self.ticker}', "
                f"data_collected_timestamp='{self.data_collected_timestamp.isoformat()}', "
                f"strike={self.strike}, optionType='{self.optionType}')>")

def init_schema(engine):
    """
    Creates the database tables defined in the models if they do not exist.

    Args:
        engine: SQLAlchemy engine instance.
    """
    print("Initializing database schema...")
    # Base.metadata.create_all will now use the __table_args__ with the engine instance
    Base.metadata.create_all(engine)
    print("Database schema initialization complete.")

# Add __main__ block to initialize schema when the file is run directly
if __name__ == '__main__':
    engine = get_engine() # Get the engine from the database module
    init_schema(engine) # Initialize the schema
