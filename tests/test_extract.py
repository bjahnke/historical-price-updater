import pandas as pd
import pytest
from typing import List
from src.historical_price_updater.extract import IbkrPriceExtractor, IbkrSymbol
from ib_insync import IB, util, Contract, BarData

class TestIbkrPriceExtractor:
    @pytest.fixture
    def ibkr_price_extractor(self):
        return IbkrPriceExtractor()
    
    def test__get_data(self, ibkr_price_extractor):
        symbol = IbkrSymbol('AAPL', 'STK')
        interval = '1 day'
        num_bars = 30
        with IB() as ib:
            ib.connect('127.0.0.1', 7496, clientId=1)
            df = ibkr_price_extractor._get_data(ib, symbol, interval, num_bars)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == num_bars
        
    def test_get_data(self, ibkr_price_extractor):
        symbol = IbkrSymbol('AAPL', 'STK')
        interval = '1 day'
        num_bars = 30
        df = ibkr_price_extractor.get_data(symbol, interval, num_bars)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == num_bars
        
    def test_download(self, ibkr_price_extractor):
        symbols = [IbkrSymbol('AAPL', 'STK'), IbkrSymbol('GOOGL', 'STK')]
        interval = '1 day'
        num_bars = 30
        df = ibkr_price_extractor.download(symbols, interval, num_bars)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(symbols) * num_bars
            
    def test_transform(self, ibkr_price_extractor):
        # TODO: Implement this test once the `transform` method is implemented
        pass