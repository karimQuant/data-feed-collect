"""Dataclass for raw Option Chain snapshot data."""

from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional

@dataclass(frozen=True)
class OptionChain:
    """Dataclass for raw Option Chain snapshot data."""
    # Primary Key / Identifying fields for this snapshot record
    # contract_symbol links to the Option instrument in the options table
    # Changed from instrument_symbol to contract_symbol
    contract_symbol: str
    # timestamp is when this specific snapshot was collected
    timestamp: datetime

    # Removed underlying_symbol as it can be derived from the Option contract linked by contract_symbol
    # underlying_symbol: str # Removed

    # Fields directly from yfinance option chain DataFrame
    # contractSymbol is redundant with contract_symbol field above, assuming contract_symbol is the unique identifier
    # contractSymbol: str # Removed, assuming contract_symbol is used instead

    # lastTradeDate can be NaN/NaT, so it's Optional
    lastTradeDate: Optional[datetime]
    strike: float # Keeping strike here as it's part of the snapshot data
    # Numeric fields can be NaN if data is missing, so they are Optional
    lastPrice: Optional[float]
    bid: Optional[float]
    ask: Optional[float]
    change: Optional[float]
    percentChange: Optional[float]
    volume: Optional[int]
    openInterest: Optional[int]
    impliedVolatility: Optional[float]
    # inTheMoney can be NaN, so it's Optional
    inTheMoney: Optional[bool]
    contractSize: Optional[str] # Usually 'REGULAR'
    currency: Optional[str]

    # Note: yfinance might return NaN for some numeric fields if data is missing.
    # pandas converts these to numpy.nan. When converting to dict and then to
    # a dataclass/DataFrame, these should become Python None, which maps
    # correctly to ClickHouse Nullable types.
