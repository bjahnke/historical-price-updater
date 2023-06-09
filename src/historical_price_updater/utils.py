"""
put utility functions here
"""
import pandas as pd
import typing as t
import yfinance as yf
from datetime import datetime, timedelta


def get_wikipedia_stocks(url) -> t.Tuple[t.List[str], pd.DataFrame]:
    """get stock data (names, sectors, etc) from wikipedia"""
    wiki_df = pd.read_html(url)[0]
    tickers_list = list(wiki_df["Symbol"])
    return tickers_list[:], wiki_df


def simple_relative(df, bm_close, rebase=True) -> pd.DataFrame:
    """simplified version of relative calculation"""
    bm = bm_close.ffill()
    if rebase is True:
        bm = bm.div(bm[0])
    return df.div(bm, axis=0)


def yf_download_data(tickers, days, interval) -> pd.DataFrame:
    """batch download of stock history download, normalize columns"""
    data = yf.download(
        tickers,
        start=(datetime.now() - timedelta(days=days)),
        end=datetime.now(),
        interval=interval,
    )
    return _normalize_yfinance_dataframe(data)


def yf_get_stock_data(symbol, days, interval: str) -> pd.DataFrame:
    """get price data from yahoo finance"""
    data = yf.ticker.Ticker(symbol).history(
        start=(datetime.now() - timedelta(days=days)),
        end=datetime.now(),
        interval=interval,
    )
    data.index = data.index.tz_localize(None)
    return _normalize_yfinance_dataframe(data)


def _normalize_yfinance_dataframe(yf_data: pd.DataFrame) -> pd.DataFrame:
    """
    helper function for normalizing table format of yfinance data
    :param yf_data:
    :return:
    """
    yf_data = yf_data.rename(
        columns={"Open": "open", "High": "high", "Low": "low", "Close": "close"}
    )
    return yf_data[["open", "high", "low", "close"]]
