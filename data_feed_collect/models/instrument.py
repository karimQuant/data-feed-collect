from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass(frozen=True)
class Instrument:
    """Base dataclass for financial instruments."""
    # Removed 'id: str' - relying on 'symbol' as the primary identifier for stocks
    symbol: str # Using symbol as the primary identifier for stocks
    currency: str
    exchange: str
    name: str

@dataclass(frozen=True)
class Stock(Instrument):
    """Dataclass for a Stock instrument."""
    # Inherits symbol, currency, exchange, name
    pass # Add stock-specific fields here if needed later

@dataclass(frozen=True)
class Option(Instrument):
    """Dataclass for an Option instrument."""
    # Inherits symbol (underlying symbol), currency, exchange, name
    # Added contract_symbol as the unique identifier for the option contract
    contract_symbol: str
    expiration_date: date
    option_type: str  # 'call' or 'put'
    strike_price: float
    # Removed option_symbol as contract_symbol serves this purpose

    def __post_init__(self):
        """Validate option_type."""
        if self.option_type.lower() not in ["call", "put"]:
            raise ValueError("option_type must be 'call' or 'put'")

