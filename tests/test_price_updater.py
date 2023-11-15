import pandas as pd
import pytest
import src.historical_price_updater.price_updater as hpu

class TestTransform:
    def test_transform_data_for_db1(self):
        watchlist = pd.DataFrame({
            'symbol': ['SPY', 'ONEQ'],
            'interval': ['1h', '1h'],
            'data_source': ['yahoo', 'yahoo'],
            'market_index': ['ONEQ', 'SPY']
        })
        historical_data1, timestamp_data1, stock1 = hpu.transform_data_for_db(watchlist)

        # assert stock table has market_index col
        assert 'market_index' in stock1.columns.to_list()

        watchlist = pd.DataFrame({
            'symbol': ['SPY', 'ONEQ'],
            'interval': ['1d', '1d'],
            'data_source': ['yahoo', 'yahoo'],
            'market_index': ['ONEQ', 'SPY']
        })
        historical_data2, timestamp_data2, stock2 = hpu.transform_data_for_db(watchlist)
        assert 'market_index' in stock2.columns.to_list()

    def test_transform_data_for_db2(self):
        watchlist = pd.DataFrame({
            'symbol': ['SPY', 'ONEQ'],
            'interval': ['1h', '1h'],
            'data_source': ['yahoo', 'yahoo'],
            'market_index': ['ONEQ', 'SPY']
        })
        historical_data, timestamp_data, stock = hpu.transform_data_for_db(watchlist)

        # assert historical_data has expected columns
        assert set(historical_data.columns) == {'bar_number', 'close', 'stock_id', 'market_index'}

        # assert timestamp_data has expected columns
        assert set(timestamp_data.columns) == {'bar_number', 'interval', 'timestamp', 'data_source'}

        # assert stock has expected columns
        assert set(stock.columns) == {'id', 'symbol', 'is_relative', 'interval', 'data_source', 'market_index'}

        # assert stock table has market_index col
        assert 'market_index' in stock.columns.to_list()

        # assert historical_data has no NaN values
        assert not historical_data.isna().any().any()

        # assert timestamp_data has no NaN values
        assert not timestamp_data.isna().any().any()

        # assert stock has no NaN values
        assert not stock.isna().any().any()

    # def test_build_relative_data_against_interval_markets1(self):
    #     watchlist = pd.DataFrame({
    #         'symbol': ['SPY', 'ONEQ'],
    #         'interval': ['1h', '1h'],
    #         'data_source': ['yahoo', 'yahoo'],
    #         'market_index': ['ONEQ', 'SPY']
    #     })
    #     hpu.build_relative_data_against_interval_markets(watchlist, pd.DataFrame(), '1h')

    # def test_build_relative_data_against_interval_markets2(self):
    #     source_watchlist = pd.DataFrame({
    #         'symbol': ['AAPL', 'MSFT', 'TSLA', 'SPY', 'ONEQ'],
    #         'interval': ['1d', '1d', '1d', '1h', '1h'],
    #         'days': [365, 365, 365, 365, 365],
    #         'market_index': ['SPY', 'SPY', 'SPY', 'ONEQ', 'SPY'],
    #         'data_source': ['yahoo', 'yahoo', 'yahoo', 'yahoo', 'yahoo']
    #     })
    #     data = pd.DataFrame({
    #         'AAPL': [1, 2, 3],
    #         'MSFT': [4, 5, 6],
    #         'TSLA': [7, 8, 9],
    #         'SPY': [10, 11, 12],
    #         'ONEQ': [13, 14, 15]
    #     })
    #     interval = '1d'

    #     absolute_data, relative_data = hpu.build_relative_data_against_interval_markets(source_watchlist, data, interval)

    #     # assert absolute_data has expected columns
    #     assert set(absolute_data.columns) == {'bar_number', 'AAPL', 'MSFT', 'TSLA'}

    #     # assert relative_data has expected columns
    #     assert set(relative_data.columns) == {'bar_number', 'AAPL', 'MSFT', 'TSLA'}

    #     # assert absolute_data has expected values
    #     assert absolute_data.to_dict() == {
    #         'bar_number': {0: 0, 1: 1, 2: 2},
    #         'AAPL': {0: 1, 1: 2, 2: 3},
    #         'MSFT': {0: 4, 1: 5, 2: 6},
    #         'TSLA': {0: 7, 1: 8, 2: 9}
    #     }

    #     # assert relative_data has expected values
    #     assert relative_data.to_dict() == {
    #         'bar_number': {0: 0, 1: 1, 2: 2},
    #         'AAPL': {0: 1.0, 1: 1.0, 2: 1.0},
    #         'MSFT': {0: 4.0, 1: 5.0, 2: 6.0},
    #         'TSLA': {0: 7.0, 1: 8.0, 2: 9.0}
    #     }