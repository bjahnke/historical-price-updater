"""Tests for `historical_price_updator` package."""
import pandas as pd
from src.historical_price_updater import mytypes, price_updater
import env
import pytest

def test_connection():
    """does database connection to neon db work?"""
    a = pd.DataFrame()
    a.to_sql('test', env.ConnectionEngines.HistoricalPrices.NEON, index=False, if_exists="replace")
