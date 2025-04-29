from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass(frozen=True)
class Instrument:
    """Base dataclass for financial instruments."""
    symbol: str
    currency: str
    exchange: str
    name: Optional[str] = None # Optional common name

@dataclass(frozen=True)
class Stock(Instrument):
    """Dataclass for a Stock instrument."""
    # Inherits symbol, currency, exchange, name
    pass # Add stock-specific fields here if needed later

@dataclass(frozen=True)
class Option(Instrument):
    """Dataclass for an Option instrument."""
    # Inherits symbol (underlying symbol), currency, exchange, name
    option_type: str # e.g., "call", "put"
    strike_price: float
    expiration_date: date
    symbol: str # Option symbol (e.g., "AAPL230920C00150000" for a call option)
    def __post_init__(self):
        """Validate option_type."""
        if self.option_type.lower() not in ["call", "put"]:
            raise ValueError("option_type must be 'call' or 'put'")

