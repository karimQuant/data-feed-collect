# data_feed_collect/models/__init__.py
"""Data models for the data_feed_collect package."""

from .instrument import Instrument, Stock, Option
from .ohlcv import OHLCV
from .option_chain import OptionChain # Import the new model

__all__ = [
    "Instrument",
    "Stock",
    "Option",
    "OHLCV",
    "OptionChain", # Add the new model to __all__
]
