from dataclasses import dataclass
from datetime import datetime

@dataclass(frozen=True)
class OHLCV:
    """Dataclass for Open-High-Low-Close-Volume data."""
    instrument_id: str     # Added unique identifier for the instrument (links to Instrument.id)
    instrument_symbol: str # Symbol of the instrument this data belongs to (kept for convenience)
    timestamp: datetime    # Timestamp for the OHLCV bar (e.g., start time)
    open: float
    high: float
    low: float
    close: float
    volume: int

