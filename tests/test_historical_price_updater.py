"""Tests for `historical_price_updator` package."""
import pandas as pd
from src.historical_price_updater import mytypes, price_updater
import env
import pytest


def test_build_tables():
    """does build test run without errors?"""
    download_args = mytypes.DownloadParams()
    stock_data, bench_data, stock_info = price_updater._download_data(**download_args.dict())
    historical_data, timestamp_data = price_updater._build_tables(
        downloaded_data=stock_data,
        bench_data=bench_data,
        interval_str=download_args.interval_str,
    )
    # historical_data.bar_number should be an integer greater than or equal to 0
    assert historical_data.bar_number.dtype == 'int64'
    assert historical_data.bar_number.min() >= 0
    # no historical_data.bar_number should be null
    assert historical_data.bar_number.isnull().sum() == 0
    # is_relative should be a boolean and never null
    assert historical_data.is_relative.dtype == 'bool'
    assert historical_data.is_relative.isnull().sum() == 0
    # historical_data.symbol should be a string and never null
    assert historical_data.symbol.dtype == 'O'
assert historical_data.symbol.isnull().sum() == 0


def test_connection():
    """does database connection to neon db work?"""
    a = pd.DataFrame()
    a.to_sql('test', env.ConnectionEngines.HistoricalPrices.NEON, index=False, if_exists="replace")
