from typing import List
import pandas as pd
import pytest
from src.historical_price_updater.extract import *
import env
import src.watchlist as watchlist
from ib_insync import *


class BaseTestHistoricalPriceExtractor:
    def test_download(self, data_extractor: HistoricalPriceExtractor):
        df = data_extractor.download(['AAPL', 'GOOGL'], '1d', 366, sec_types=['STK', 'STK'])
        assert isinstance(df, pd.DataFrame)
        expected_columns = [('open', 'AAPL'), ('high', 'AAPL'), ('low', 'AAPL'), ('close', 'AAPL'), ('open', 'GOOGL'), ('high', 'GOOGL'), ('low', 'GOOGL'), ('close', 'GOOGL')]
        for col in expected_columns:
            assert col in df.columns.tolist()
        assert df.index.name == 'Date'

        df = data_extractor.download(['AAPL'], '1d', 366, sec_types=['STK'])
        expected_columns = [('open', 'AAPL'), ('high', 'AAPL'), ('low', 'AAPL'), ('close', 'AAPL')]
        for col in expected_columns:
            assert col in df.columns.tolist()
        assert df.index.name == 'Date'

    def test_get_data(self, data_extractor: HistoricalPriceExtractor):
        symbol = "MES" if data_extractor.__class__.__name__ == 'IbkrPriceExtractor' else "MES=F"
        sec_type = "FUT"
        interval = '1m'
        num_bars = 300
        df = data_extractor.get_data(symbol, interval, num_bars, sec_type)
        assert isinstance(df, pd.DataFrame)
        has_columns = ['open', 'high', 'low', 'close']
        for col in has_columns:
            assert col in df.columns.tolist()
        assert df.index.name == 'Date'
        assert len(df) > 0


class TestIbkrPriceExtractor(BaseTestHistoricalPriceExtractor):
    @pytest.fixture
    def data_extractor(self):
        return IbkrPriceExtractor()
    
    def test__get_data(self, data_extractor):
        symbol = 'SPY'
        interval = '1d'
        sec_type = 'STK'
        num_bars = 1000
        with IB() as ib:
            ib.connect(port=7496)
            df = data_extractor._get_data(ib, symbol, interval, num_bars, sec_type)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == num_bars

    def test_contracts_generator(self, data_extractor):
        symbol = "AAPL"
        sec_type = "STK"
        with IB() as ib:
            ib.connect(port=7496)
            contracts = list(data_extractor.contracts_generator(ib, symbol, sec_type))
        # asssert a list of contracts were returned
        assert isinstance(contracts, list)
        assert isinstance(contracts[0], ContractDetails)

    def test__get_quote(self, data_extractor):
        symbol = 'AAPL'
        sec_type = 'STK'
        with IB() as ib:
            ib.connect(port=7496)
            quote = data_extractor._get_quote(ib, symbol, sec_type)
        assert isinstance(quote, Ticker)
        # ask is float
        assert isinstance(quote.ask, float)
        # bid is float
        assert isinstance(quote.bid, float)
        # close is float
        assert isinstance(quote.close, float)

    def test__get_quotes(self, data_extractor):
        symbols = ['AAPL', 'GOOGL']
        sec_types = ['STK', 'STK']
        with IB() as ib:
            ib.connect(port=7496)
            quotes = data_extractor._get_quotes(ib, symbols, sec_types)

        # df = util.df(quotes).dropna(axis=1)
        assert len(quotes) == len(symbols)
        for quote in quotes:
            assert quote.contract.symbol in symbols
            assert isinstance(quote, Ticker)
            # ask is float
            assert isinstance(quote.ask, float)
            # bid is float
            assert isinstance(quote.bid, float)
            # close is float
            assert isinstance(quote.close, float)


class TestYahooPriceExtractor(BaseTestHistoricalPriceExtractor):
    @pytest.fixture
    def data_extractor(self):
        return YahooPriceExtractor()


def get_watchlist():
    w = watchlist.MongoWatchlistClient(env.WATCHLIST_API_KEY)
    res = w.get_latest('asset-tracking')['watchlist']
    pd.DataFrame.from_records(res).to_excel('watchlist.xlsx')

