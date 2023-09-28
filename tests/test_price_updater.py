import pandas as pd
import pytest
import src.historical_price_updater.price_updater as hpu

def test_task_save_historical_data_to_database():
    assert False


def test__download_data():
    watchlist = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT', 'TSLA'],
        'interval': ['1d', '1d', '1d'],
        'days': [365, 365, 365],
        'bench_symbol': ['SPY', 'SPY', 'SPY'],
        'data_source': ['yahoo', 'yahoo', 'yahoo']
    })
    hpu._download_data(watchlist)
