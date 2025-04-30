# data_feed_collect/models/__init__.py
"""Data models for the data_feed_collect package."""

from .instrument import Instrument, Stock, Option # Keep Option import for now, but it's removed from __all__
from .ohlcv import OHLCV
from .option_chain import OptionChain # Import the new model

__all__ = [
    "Instrument",
    "Stock",
    # "Option", # Removed as its data is merged into OptionChain and it's not used for table creation here
    "OHLCV",
    "OptionChain", # Add the new model to __all__
]
