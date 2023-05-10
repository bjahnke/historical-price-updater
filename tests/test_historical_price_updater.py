"""Tests for `historical_price_updator` package."""
import pytest
import historical_price_updater.mytypes as mytypes
import historical_price_updater.price_updater as price_updater


def test_build_tables():
    """does build test run without errors?"""
    download_args = mytypes.DownloadParams()
    stock_data, bench_data = price_updater._download_data(**download_args.dict())
    historical_data, timestamp_data = price_updater._build_tables(
        downloaded_data=stock_data,
        bench_data=bench_data,
        interval_str=download_args.interval_str,
    )
