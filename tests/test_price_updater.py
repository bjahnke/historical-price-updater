import pandas as pd
import pytest
import src.historical_price_updater.price_updater as hpu
from ib_insync import *
from src.historical_price_updater.extract import *

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
            'market_index': ['ONEQ', 'SPY'],
        })
        historical_data2, timestamp_data2, stock2 = hpu.transform_data_for_db(watchlist)
        assert 'market_index' in stock2.columns.to_list()

    def test_transform_data_for_db2(self):
        watchlist = pd.DataFrame({
            'symbol': ['SPY', 'ONEQ', 'AAPL'],
            'interval': ['1d', '1d', '1d'],
            'data_source': ['ibkr', 'ibkr', 'ibkr'],
            'market_index': ['ONEQ', 'SPY', 'AAPL'],
            'sec_type': ['STK', 'STK', 'STK']
        })

        historical_data, timestamp_data, stock = hpu.transform_data_for_db(watchlist)

        # assert historical_data has expected columns
        assert set(historical_data.columns) == {'bar_number', 'close', 'stock_id'}

        # assert timestamp_data has expected columns
        assert set(timestamp_data.columns) == {'bar_number', 'interval', 'timestamp', 'data_source'}

        # assert stock has expected columns
        assert set(stock.columns) == {'id', 'symbol', 'is_relative', 'interval', 'data_source', 'market_index', 'sec_type'}

        # assert stock table has market_index col
        assert 'market_index' in stock.columns.to_list()

        # assert historical_data has no NaN values
        assert not historical_data.isna().any().any()

        # assert timestamp_data has no NaN values
        assert not timestamp_data.isna().any().any()

        # assert stock has no NaN values
        assert not stock.isna().any().any()


    def test_transform_data_for_db_multi_data_source(self):
        watchlist = pd.DataFrame({
            'symbol': ['SPY', 'ONEQ', 'AAPL', 'BTC-USD'],
            'interval': ['1d', '1d', '1d', '1d'],
            'data_source': ['ibkr', 'ibkr', 'ibkr', 'yahoo'],
            'market_index': ['ONEQ', 'SPY', 'AAPL', 'BTC-USD'],
            'sec_type': ['STK', 'STK', 'STK', 'STK']
        })

        historical_data, timestamp_data, stock = hpu.transform_data_for_db_multi_data_source(watchlist)

        # assert historical_data has expected columns
        assert set(historical_data.columns) == {'bar_number', 'close', 'stock_id'}

        # assert timestamp_data has expected columns
        assert set(timestamp_data.columns) == {'bar_number', 'interval', 'timestamp', 'data_source'}

        # assert stock has expected columns
        assert set(stock.columns) == {'id', 'symbol', 'is_relative', 'interval', 'data_source', 'market_index', 'sec_type'}

        # assert stock table has market_index col
        assert 'market_index' in stock.columns.to_list()

        # assert historical_data has no NaN values
        assert not historical_data.isna().any().any()

        # assert timestamp_data has no NaN values
        assert not timestamp_data.isna().any().any()

        # assert stock has no NaN values
        assert not stock.isna().any().any()

        # assert index is unique
        assert historical_data.index.is_unique

    def test_transform_data_for_db_multi_data_source2(self):
        watchlist = pd.DataFrame({
            'symbol': ['MES', 'MNQ'],
            'interval': ['1d', '1d'],
            'data_source': ['ibkr', 'ibkr'],
            'market_index': ['ONEQ', 'MES'],
            'sec_type': ['FUT', 'FUT']
        })

        ibkr = IbkrPriceExtractor()
        data = ibkr.download(watchlist.symbol.to_list(), '1d', 1000, sec_types=watchlist.sec_type.to_list())

        historical_data, timestamp_data, stock = hpu.transform_data_for_db_multi_data_source(watchlist)
        # assert historical_data has expected columns
        assert set(historical_data.columns) == {'bar_number', 'close', 'stock_id'}

        # assert timestamp_data has expected columns
        assert set(timestamp_data.columns) == {'bar_number', 'interval', 'timestamp', 'data_source'}

        # assert stock has expected columns
        assert set(stock.columns) == {'id', 'symbol', 'is_relative', 'interval', 'data_source', 'market_index', 'sec_type'}

        # assert stock table has market_index col
        assert 'market_index' in stock.columns.to_list()

        # assert historical_data has no NaN values
        assert not historical_data.isna().any().any()

        # assert timestamp_data has no NaN values
        assert not timestamp_data.isna().any().any()

        # assert stock has no NaN values
        assert not stock.isna().any().any()

        # assert index is unique
        assert historical_data.index.is_unique