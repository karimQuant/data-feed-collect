"""Tests for the DataCollector class."""

import pytest

from data_feed_collect import DataCollector


def test_collector_initialization():
    """Test that the DataCollector can be initialized."""
    collector = DataCollector()
    assert collector is not None


def test_collect_missing_source():
    """Test collecting from a non-existent source raises ValueError."""
    collector = DataCollector()
    with pytest.raises(ValueError):
        collector.collect("nonexistent_source")


def test_process_data():
    """Test data processing."""
    collector = DataCollector()
    data = {"source": "test", "data": [1, 2, 3]}
    processed = collector.process(data)
    assert processed["processed"] is True
    assert processed["source"] == "test"
