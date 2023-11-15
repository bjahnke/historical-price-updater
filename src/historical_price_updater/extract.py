
from abc import ABC, abstractmethod
from typing import List
import pandas as pd
from ib_insync import IB, util, Contract, BarData
from functools import wraps
from dataclasses import dataclass
import src.historical_price_updater.utils as utils


@dataclass
class IbkrSymbol:
    symbol: str
    sec_type: str


class HistoricalPriceExtractor(ABC):
    @abstractmethod
    def download(self, symbols: List[str], interval: str, num_bars: int, *args, **kwargs):
        pass

    def get_data(self, symbol, interval, num_bars):
        raise NotImplementedError
    
    @abstractmethod
    def transform(self, *args, **kwargs):
        pass


_ib = IB()
def ib_connect(func):
    """
    A decorator function that establishes a connection to the Interactive Brokers API and passes the connection object
    to the decorated function.

    Args:
        func (callable): The function to be decorated.

    Returns:
        callable: The decorated function.

    Raises:
        ConnectionError: If the connection to the Interactive Brokers API cannot be established.

    Example:
        >>> @ib_connect
        ... def my_function(ib_conn):
        ...     # Use the ib_conn object to interact with the Interactive Brokers API
        ...     pass
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        with _ib as ib:
            ib.connect('127.0.0.1', 7496, clientId=1)
            result = func(*args, **kwargs)
        return result
    wrapper.ib = _ib
    return wrapper

class IbkrPriceExtractor(HistoricalPriceExtractor):
    def __init__(self):
        # TODO this class can only be initialized once.
        self.ib = IB()
        
    def _get_data(self, ib, symbol: IbkrSymbol, interval: str, num_bars: int) -> pd.DataFrame:
        sec_type = symbol.sec_type
        symbol = symbol.symbol
        contracts: List[Contract] = ib.reqContractDetails(Contract(symbol=symbol, secType=sec_type))
        if len(contracts) == 0:
            raise ValueError(f'No contract found for symbol={symbol}, sec_type={sec_type}')
        contract = contracts[0].contract
        bars = ib.reqHistoricalData(
            contract=contract,
            endDateTime='',
            durationStr=f'{num_bars} D',
            barSizeSetting=interval,
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1
        )
        df = util.df(bars)
        return df
    
    @ib_connect
    def get_data(self, symbol: IbkrSymbol, interval, num_bars):
        return self._get_data(self.get_data.ib, symbol, interval, num_bars)
    
    @ib_connect
    def download(self, symbols: List[IbkrSymbol], interval: str, num_bars: int) -> pd.DataFrame:
        price_data = [
            self._get_data(self.download.ib, symbol, interval, num_bars) 
            for symbol in symbols
            ]
        return pd.concat(price_data)
    
    def transform(self, *args, **kwargs):
        pass


class YahooPriceExtractor(HistoricalPriceExtractor):
    def __init__(self):
        pass

    def get_data(self, symbol, interval, num_bars):
        return utils.yf_download_data(symbol, num_bars, interval)

    def download(self, symbols: List[str], interval: str, num_bars: int, *args, **kwargs) -> pd.DataFrame:
        return utils.yf_download_data(symbols, num_bars, interval)

    def transform(self, *args, **kwargs):
        pass
