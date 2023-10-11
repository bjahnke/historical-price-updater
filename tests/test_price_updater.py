import pandas as pd
import pytest
import src.historical_price_updater.price_updater as hpu


def test__download_data():
    watchlist = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT', 'TSLA'],
        'interval': ['1d', '1d', '1d'],
        'days': [365, 365, 365],
        'market_index': ['SPY', 'SPY', 'SPY'],
        'data_source': ['yahoo', 'yahoo', 'yahoo']
    })
    hpu._download_data(watchlist)


def test_transform_data_for_db():
    watchlist = pd.DataFrame({
        'symbol': ['SPY', 'ONEQ'],
        'interval': ['1h', '1h'],
        'data_source': ['yahoo', 'yahoo'],
        'market_index': ['ONEQ', 'SPY']
    })
    historical_data1, timestamp_data1, stock1 = hpu.transform_data_for_db(watchlist)

    watchlist = pd.DataFrame({
        'symbol': ['SPY', 'ONEQ'],
        'interval': ['1d', '1d'],
        'data_source': ['yahoo', 'yahoo'],
        'market_index': ['ONEQ', 'SPY']
    })
    historical_data2, timestamp_data2, stock2 = hpu.transform_data_for_db(watchlist)
    print('done')


