from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, DateTime
from sqlalchemy.orm import declarative_base
from datetime import datetime

# Define the base for declarative models
Base = declarative_base()

class YFinanceOption(Base):
    """
    SQLAlchemy model for storing Yahoo Finance option chain data.
    """
    __tablename__ = 'yfinance_options'

    # Composite Primary Key to uniquely identify an option snapshot
    contractSymbol = Column(String, primary_key=True)
    ticker = Column(String, primary_key=True)
    data_collected_timestamp = Column(DateTime, primary_key=True) # Timestamp when data was collected

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
    expiration = Column(Integer, nullable=False) # Unix timestamp of expiration date
    lastTradeDate = Column(Integer) # Unix timestamp of last trade date, can be None
    impliedVolatility = Column(Float)
    inTheMoney = Column(Boolean, nullable=False)
    optionType = Column(String, nullable=False) # 'call' or 'put'

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
    Base.metadata.create_all(engine)
    print("Database schema initialization complete.")

# Example usage (optional, for testing purposes)
# if __name__ == '__main__':
#     # Replace with your actual database URL
#     # For a simple test, you can use an in-memory SQLite database
#     DATABASE_URL = "sqlite:///:memory:"
#     engine = create_engine(DATABASE_URL)
#
#     # Initialize the schema (create tables)
#     init_schema(engine)
#
#     # You can now use the engine and session to interact with the database
#     # from sqlalchemy.orm import sessionmaker
#     # SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
#     # db = SessionLocal()
#     #
#     # # Example: Add a dummy option record
#     # dummy_option = YFinanceOption(
#     #     contractSymbol="AAPL240719C00150000",
#     #     ticker="AAPL",
#     #     data_collected_timestamp=datetime.utcnow(),
#     #     strike=150.0,
#     #     currency="USD",
#     #     lastPrice=10.0,
#     #     change=0.5,
#     #     percentChange=5.0,
#     #     volume=100,
#     #     openInterest=500,
#     #     bid=9.8,
#     #     ask=10.2,
#     #     contractSize="REGULAR",
#     #     expiration=1626652800, # Example timestamp
#     #     lastTradeDate=1626566400, # Example timestamp
#     #     impliedVolatility=0.25,
#     #     inTheMoney=True,
#     #     optionType="call"
#     # )
#     # db.add(dummy_option)
#     # db.commit()
#     # db.close()
#     # print("Added dummy option.")
