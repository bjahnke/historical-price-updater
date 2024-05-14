import math
from typing import Tuple, Any

import pandas as pd
import sqlalchemy
from pandas import DataFrame

import src.historical_price_updater.utils as utils
import src.historical_price_updater.mytypes as mytypes
import typing as t
import env
import src.historical_price_updater.extract as extract
import src.watchlist


def build_relative_data_against_interval_markets(
        source_watchlist: pd.DataFrame, data: pd.DataFrame, interval: str) -> t.Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Perform relative calculation for all market indexes at a given interval and source watchlist
    :param source_watchlist:
    :param data:
    :param interval:
    :return:
    """
    # init with empty df to cover data for only one symbol
    base_data_list = [pd.DataFrame()]
    relative_data_list = [pd.DataFrame()]

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
        relative_data_list.append(utils.simple_relative(base_data, bench_data[market_index], rebase=False))
        base_data_list.append(base_data)

    absolute_data = pd.concat(base_data_list, axis=1)
    absolute_data = absolute_data.reset_index().rename(columns={"index": "bar_number"})

    relative_data = pd.concat(relative_data_list, axis=1)
    relative_data = relative_data.reset_index().rename(columns={"index": "bar_number"})

    return absolute_data, relative_data


_EXTRACTORS_MAP = {
    'ibkr': extract.IbkrPriceExtractor(),
    'yahoo': extract.YahooPriceExtractor(),
}


def transform_data_for_db_multi_data_source(watchlist: pd.DataFrame):
    """
    call transform_data_for_db for each unique data source in watchlist,
    concat results and return
    """
    data_source_list = watchlist.data_source.unique()
    historical_data_list = []
    timestamp_data_list = []
    for data_source in data_source_list:
        watchlist_by_source = watchlist.loc[watchlist["data_source"] == data_source].copy()
        historical_data, timestamp_data = transform_data_for_db(watchlist_by_source)
        historical_data_list.append(historical_data)
        timestamp_data_list.append(timestamp_data)

    historical_data = pd.concat(historical_data_list, axis=0).reset_index(drop=True)
    timestamp_data = pd.concat(timestamp_data_list, axis=0).reset_index(drop=True)

    historical_candidate_key = ["symbol", 'is_relative', 'interval', 'data_source']

    stock = historical_data[historical_candidate_key].drop_duplicates().copy()

    stock = stock.merge(
        watchlist,
        on=["symbol", "interval", "data_source"],
        how="left",
    )

    stock = stock.reset_index(drop=True).reset_index().rename(columns={"index": "id"})

    # set stock_id as column in historical data where symbol, is_relative, interval match,
    # then drop those columns from historical data
    historical_data = historical_data.merge(stock[historical_candidate_key + ['id']], on=historical_candidate_key)
    historical_data = historical_data.drop(columns=historical_candidate_key)
    historical_data = historical_data.rename(columns={"id": "stock_id"})
    historical_data = historical_data.dropna()

    return historical_data, timestamp_data, stock


def transform_data_for_db(watchlist: pd.DataFrame) -> t.Tuple[DataFrame, DataFrame]:
    """
    Schedule the script to save historical data to a database.
    """
    timestamp_data_list = []
    interval_data_list = []
    data_source = watchlist.data_source.iloc[0]
    extractor = _EXTRACTORS_MAP[data_source]
    for interval in watchlist["interval"].unique():
        watchlist_by_interval = watchlist.loc[watchlist["interval"] == interval].copy()
        data = extractor.download(
            watchlist_by_interval.symbol.to_list(),
            interval, 
            1000, 
            watchlist_by_interval.sec_type.to_list()
        )
        # keep only close column
        data = data.drop(columns=data.columns.levels[0].difference(['close']), level=0)
        data = data.reset_index()
        data_timestamp = data.columns.to_list()[0]
        data_date_time = data[data_timestamp]
        # remove timestamp column
        data = data.iloc[:, 1:]

        absolute_data, relative_data = build_relative_data_against_interval_markets(
            watchlist, data, interval)

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

    return historical_data, timestamp_data


def custom_round(value):
    decimal_places = max(0, 5 - int(math.floor(math.log10(abs(value)))))
    return round(value, decimal_places)


def task_save_historical_data_to_database(
    watchlist, connection_engine: sqlalchemy.Engine
) -> None:
    """
    Schedule the script to save historical data to a database.
    """
    historical_data, timestamp_data, stock = transform_data_for_db_multi_data_source(watchlist)

    stock.to_sql('stock', connection_engine, index=False, if_exists="replace")

    historical_data.to_sql(mytypes.HistoricalPrices.stock_data, connection_engine, index=False, if_exists="replace")
    timestamp_data.to_sql(mytypes.HistoricalPrices.timestamp_data, connection_engine, index=False, if_exists="replace")


def get_sp500_watchlist(engine):

    _, latest_sp500 = utils.get_wikipedia_stocks('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    latest_sp500 = latest_sp500.rename(columns={'Symbol': 'symbol'})

    latest_sp500.to_sql('stock_info', engine, if_exists='replace')

    new_watchlist_records_from_sp500 = latest_sp500[['symbol']].copy()
    new_watchlist_records_from_sp500['data_source'] = 'yahoo'
    new_watchlist_records_from_sp500['interval'] = '1d'
    new_watchlist_records_from_sp500['market_index'] = 'SPY'
    new_watchlist_records_from_sp500['sec_type'] = 'STK'

    return new_watchlist_records_from_sp500


def main():
    """
    This is the main function that will be called by the cloud function.
    :return:
    """
    watchlist_client = src.watchlist.MongoWatchlistClient(env.WATCHLIST_API_KEY)
    watchlist = watchlist_client.get_latest('asset-tracking')['watchlist']
    engine = env.ConnectionEngines.HistoricalPrices.NEON

    watchlist = pd.DataFrame.from_records(watchlist)
    # if SP500 is in watchlist, track all current SP500 stocks
    if 'SP500' in watchlist['symbol'].unique():
        sp500_watchlist = get_sp500_watchlist(engine)
        # remove the SP500 from the watchlist
        watchlist = watchlist.loc[watchlist['symbol'] != 'SP500'].copy()
        # add the SP500 watchlist to the watchlist
        watchlist = pd.concat([watchlist, sp500_watchlist]).reset_index(drop=True).drop_duplicates()

    task_save_historical_data_to_database(
        watchlist,
        connection_engine=engine,
    )
