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


def simple_relative(df: pd.DataFrame, bm_close: pd.Series, rebase=True) -> pd.DataFrame:
    """simplified version of relative calculation"""
    bm = bm_close.ffill()
    if rebase is True:
        bm = bm.div(bm[0])
        multiple = 1
    else:
        multiple = 1000
    return df.div(bm, axis=0) * multiple


def yf_download_data(tickers, bars, interval) -> pd.DataFrame:
    """
    batch download of stock history download, normalize columns
    :param tickers: list of tickers to download data
    :param bars: number of bars to download
    :param interval: interval of bars
    """
    # calculate start date by multiplying bars by interval 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk
    interval_to_timedelta = {
        '1m': timedelta(minutes=1),
        '2m': timedelta(minutes=2),
        '5m': timedelta(minutes=5),
        '15m': timedelta(minutes=15),
        '30m': timedelta(minutes=30),
        '60m': timedelta(hours=1),
        '1h': timedelta(hours=1),
        '1d': timedelta(days=1),
        '5d': timedelta(days=5)
    }
    if interval in interval_to_timedelta:
        interval_timedelta = interval_to_timedelta[interval]
    else:
        raise ValueError("Invalid interval")

    end = datetime.now()
    start = end - (bars * interval_timedelta)

    data = yf.download(
        tickers,
        start=start,
        end=end,
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
