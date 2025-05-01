from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, DateTime, Text
from sqlalchemy.orm import declarative_base
from datetime import datetime
from data_feed_collect.database import get_engine

# Define the base for declarative models
Base = declarative_base()

class YFinanceOption(Base):
    """
    SQLAlchemy model for storing Yahoo Finance option chain data.
    """
    __tablename__ = 'yfinance_options'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='Unique identifier for the option record')
    contractSymbol = Column(String, index=True, comment='The unique symbol identifying the option contract (e.g., AAPL230915C00150000)')
    ticker = Column(String, index=True, comment='The ticker symbol of the underlying asset (e.g., AAPL)')
    data_collected_timestamp = Column(DateTime, index=True, comment='Timestamp when this specific option data record was collected')

    # Option specific data fields from Yahoo Finance
    strike = Column(Float, nullable=False, comment='The strike price of the option contract')
    currency = Column(String, nullable=False, comment='The currency of the option contract (e.g., USD)')
    lastPrice = Column(Float, comment='The last traded price of the option contract') # Can be None if no trades
    change = Column(Float, comment='The change in price from the previous trading day close')
    percentChange = Column(Float, comment='The percentage change in price from the previous trading day close')
    volume = Column(Integer, comment='The trading volume for the day') # Can be None
    openInterest = Column(Integer, comment='The total number of outstanding option contracts') # Can be None
    bid = Column(Float, comment='The current highest price a buyer is willing to pay') # Can be None
    ask = Column(Float, comment='The current lowest price a seller is willing to accept') # Can be None
    contractSize = Column(String, nullable=False, comment='The size of the contract (e.g., REGULAR)') # e.g., 'REGULAR'
    expiration = Column(DateTime, nullable=False, comment='The expiration date of the option contract') # Stored as DateTime type
    lastTradeDate = Column(DateTime, comment='The date and time of the last trade for this contract') # Unix timestamp of last trade date, can be None
    impliedVolatility = Column(Float, comment='The implied volatility of the option contract') # Can be None
    inTheMoney = Column(Boolean, nullable=False, comment='Indicates if the option is currently in the money (True) or out of the money (False)')
    optionType = Column(String, nullable=False, comment='The type of option contract (e.g., call or put)') # 'call' or 'put'

    # New column for underlying price
    underlying_price = Column(Float, comment='The price of the underlying stock at the time the option data was collected')

    # New column for calculated mid price
    mid_price = Column(Float, comment='The calculated mid price of the option contract ((bid + ask) / 2)')

    __table_args__ = (
        {'comment': 'Stores detailed option chain data fetched from Yahoo Finance for various tickers and expiration dates.'},
    )


    def __repr__(self):
        return (f"<YFinanceOption(contractSymbol='{self.contractSymbol}', ticker='{self.ticker}', "
                f"data_collected_timestamp='{self.data_collected_timestamp.isoformat()}', "
                f"strike={self.strike}, optionType='{self.optionType}', underlying_price={self.underlying_price}, mid_price={self.mid_price})>")

class StocksCollection(Base):
    """
    SQLAlchemy model for storing basic stock information like ticker and description.
    """
    __tablename__ = 'stocks_collection'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='Unique identifier for the stock record')
    ticker = Column(String, unique=True, index=True, nullable=False, comment='The unique ticker symbol for the stock (e.g., AAPL, MSFT)') # Ticker symbol, should be unique
    company_description = Column(Text, comment='A brief description or profile of the company associated with the ticker') # Use Text for potentially long descriptions

    __table_args__ = (
        {'comment': 'Stores basic information about collected stocks, primarily ticker symbols and company descriptions.'},
    )

    def __repr__(self):
        # Truncate description for repr to keep it readable
        desc_preview = self.company_description[:50] + '...' if self.company_description and len(self.company_description) > 50 else self.company_description
        return (f"<StocksCollection(ticker='{self.ticker}', "
                f"company_description='{desc_preview}')>")


def init_schema(engine):
    """
    Creates the database tables defined in the models if they do not exist.

    Args:
        engine: SQLAlchemy engine instance.
    """
    print("Initializing database schema...")
    # Base.metadata.create_all will now create tables for all models inheriting from Base
    # and include comments defined in the models.
    Base.metadata.create_all(engine)
    print("Database schema initialization complete.")

# Add __main__ block to initialize schema when the file is run directly
if __name__ == '__main__':
    engine = get_engine() # Get the engine from the database module
    init_schema(engine) # Initialize the schema
