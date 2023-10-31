from typing import Tuple, Any

import pandas as pd
import sqlalchemy
from pandas import DataFrame

import src.historical_price_updater.utils as utils
import src.historical_price_updater.mytypes as mytypes
import typing as t
import env


def build_relative_data_against_interval_markets(
        source_watchlist: pd.DataFrame, data: pd.DataFrame, interval: str) -> t.Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Perform relative calculation for all market indexes at a given interval and source watchlist
    :param source_watchlist:
    :param data:
    :param interval:
    :return:
    """
    base_data_list = []
    relative_data_list = []

    market_index_at_interval = source_watchlist.loc[
        source_watchlist["interval"] == interval, "market_index"].unique()
    for market_index in market_index_at_interval:
        if pd.isna(market_index) or market_index not in data.close.columns.to_list():
            continue
        bench_data = data.close[[market_index]].copy()
        # exclude bench data from base data
        base_data = data.close[
            source_watchlist.loc[
                (source_watchlist["market_index"] == market_index) &
                (source_watchlist["interval"] == interval)
                ].symbol.to_list()
        ].copy()
        relative_data_list.append(utils.simple_relative(base_data, bench_data[market_index]))
        base_data_list.append(base_data)

    absolute_data = pd.concat(base_data_list, axis=1)
    absolute_data = absolute_data.reset_index().rename(columns={"index": "bar_number"})

    relative_data = pd.concat(relative_data_list, axis=1)
    relative_data = relative_data.reset_index().rename(columns={"index": "bar_number"})

    return absolute_data, relative_data


def transform_data_for_db(watchlist) -> t.Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Schedule the script to save historical data to a database.
    """
    data_source = 'yahoo'
    yahoo_source_stocks = watchlist.loc[watchlist["data_source"] == data_source].copy()
    timestamp_data_list = []
    interval_data_list = []
    for interval in yahoo_source_stocks["interval"].unique():
        data = utils.yf_download_data(
            yahoo_source_stocks.symbol.loc[yahoo_source_stocks.interval == interval].to_list(),
            1000,
            interval
        )
        # drop open high low from the first level
        data = data.drop(columns=['open', 'high', 'low'], level=0)
        data = data.reset_index()
        data_timestamp = data.columns.to_list()[0]
        data_date_time = data[data_timestamp]
        # remove timestamp column
        data = data.iloc[:, 1:]

        absolute_data, relative_data = build_relative_data_against_interval_markets(
            yahoo_source_stocks, data, interval)

        timestamp_data = absolute_data[["bar_number"]].copy()
        timestamp_data["interval"] = interval
        timestamp_data["timestamp"] = data_date_time
        timestamp_data_list.append(timestamp_data)

        absolute_data = absolute_data.melt(
            id_vars="bar_number", var_name=["symbol"], value_name="close"
        )
        absolute_data["is_relative"] = False
        relative_data = relative_data.melt(
            id_vars="bar_number", var_name=["symbol"], value_name="close"
        )
        relative_data["is_relative"] = True
        interval_data = pd.concat([absolute_data, relative_data], axis=0)
        interval_data["interval"] = interval
        interval_data_list.append(interval_data)

    # create timestamp table
    timestamp_data = pd.concat(timestamp_data_list, axis=0)
    timestamp_data['data_source'] = data_source
    historical_data = pd.concat(interval_data_list, axis=0)
    historical_data['data_source'] = data_source

    historical_candidate_key = ["symbol", 'is_relative', 'interval', 'data_source']

    stock = historical_data[historical_candidate_key].drop_duplicates().copy()

    stock = stock.reset_index(drop=True).reset_index().rename(columns={"index": "id"})
    # set stock_id as column in historical data where symbol, is_relative, interval match,
    # then drop those columns from historical data
    historical_data = historical_data.merge(stock, on=historical_candidate_key)
    historical_data = historical_data.drop(columns=historical_candidate_key)
    historical_data = historical_data.rename(columns={"id": "stock_id"})
    historical_data = historical_data.dropna()

    return historical_data, timestamp_data, stock


def task_save_historical_data_to_database(
    watchlist, connection_engine: sqlalchemy.Engine
) -> None:
    """
    Schedule the script to save historical data to a database.
    """
    historical_data, timestamp_data, stock = transform_data_for_db(watchlist)

    stock.to_sql('stock', connection_engine, index=False, if_exists="replace")
    historical_data.to_sql(mytypes.HistoricalPrices.stock_data, connection_engine, index=False, if_exists="replace")
    timestamp_data.to_sql(mytypes.HistoricalPrices.timestamp_data, connection_engine, index=False, if_exists="replace")
