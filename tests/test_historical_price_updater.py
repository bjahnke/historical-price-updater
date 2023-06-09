"""Tests for `historical_price_updator` package."""
import pandas as pd
from src.historical_price_updater import mytypes, price_updater
import env


def test_build_tables():
    """does build test run without errors?"""
    download_args = mytypes.DownloadParams()
    stock_data, bench_data = price_updater._download_data(**download_args.dict())
    historical_data, timestamp_data = price_updater._build_tables(
        downloaded_data=stock_data,
        bench_data=bench_data,
        interval_str=download_args.interval_str,
    )


def test_connection():
    """does database connection to neon db work?"""
    a = pd.DataFrame()
    a.to_sql('test', env.ConnectionEngines.HistoricalPrices.NEON, index=False, if_exists="replace")
