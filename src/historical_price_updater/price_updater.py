import pathlib
from typing import Type
import pandas as pd
import sqlalchemy
import src.historical_price_updater.utils as utils
import src.historical_price_updater.mytypes as mytypes
import typing as t
import socket

import env


def _download_data(
    stock_table_url: str = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
    bench: str = "SPY",
    days: int = 365,
    interval_str: str = "1d",
) -> t.Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Download stock price data and benchmark data.

    :param stock_table_url: URL of the stock table on Wikipedia.
    :param bench: The symbol of the benchmark to use.
    :param days: Number of days of historical data to download.
    :param interval_str: The interval of the historical data to download.
    :return: A tuple containing the downloaded stock data and benchmark data.
    """
    ticks, stock_info = utils.get_wikipedia_stocks(stock_table_url)
    downloaded_data = utils.yf_download_data(ticks, days, interval_str)
    bench_data = utils.yf_get_stock_data(bench, days, interval_str)
    bench_data["symbol"] = bench
    return downloaded_data, bench_data, stock_info


def _build_tables(
    downloaded_data: pd.DataFrame, bench_data: pd.DataFrame, interval_str: str
) -> tuple:
    """
    Build the historical and timestamp tables.

    :param downloaded_data: The downloaded stock price data.
    :param bench_data: The benchmark data.
    :param interval_str: The interval of the historical data.
    :return: A tuple containing the historical data and timestamp data.
    """
    downloaded_data = downloaded_data.reset_index()
    dd_timestamp = downloaded_data.columns.to_list()[0]
    dd_date_time = downloaded_data[dd_timestamp]
    bench_data = bench_data.reset_index()
    bd_date_time = bench_data[bench_data.columns.to_list()[0]]
    # remove timestamp column
    downloaded_data = downloaded_data.iloc[:, 1:]
    bench_data = bench_data.iloc[:, 1:]

    # ensure bench has same dates as stock data
    assert dd_date_time.equals(bd_date_time)
    bench_data = bench_data.reset_index().rename(columns={"index": "bar_number"})
    # melt multiindex df (stretch vertically)
    relative = utils.simple_relative(downloaded_data, bench_data.close)
    downloaded_data = downloaded_data.reset_index().rename(
        columns={"index": "bar_number"}
    )
    relative = relative.reset_index().rename(columns={"index": "bar_number"})
    relative = modify_dataframe(relative)

    # create timestamp table
    timestamp_data = downloaded_data[["bar_number"]].copy()
    timestamp_data["interval"] = interval_str
    timestamp_data["timestamp"] = dd_date_time
    timestamp_data.columns = timestamp_data.columns.droplevel(1)

    downloaded_data = modify_dataframe(downloaded_data)

    # create historical table
    relative["is_relative"] = True
    downloaded_data["is_relative"] = False
    bench_data["is_relative"] = False

    historical_data = pd.concat([downloaded_data, relative, bench_data], axis=0)
    historical_data["interval"] = interval_str

    return historical_data, timestamp_data


def modify_dataframe(data: pd.DataFrame) -> pd.DataFrame:
    """
    Modify the input dataframe to make it suitable for the database.

    :param data: The dataframe to modify.
    :return: The modified dataframe.
    """
    # Melt dataframe to stack the columns
    data = data.melt(
        id_vars="bar_number", var_name=["type", "symbol"], value_name="value"
    )
    # Pivot the 'type' column to expand the dataframe vertically
    data = data.pivot_table(
        index=["symbol", "bar_number"], columns="type", values="value"
    ).reset_index()
    return data


def save_historical_data_to_database(
    stock_data: pd.DataFrame,
    bench_data: pd.DataFrame,
    stock_info: pd.DataFrame,
    engine: sqlalchemy.Engine,
    hp: Type[mytypes.HistoricalPrices],
    interval_str: str,
) -> None:
    """
    Save historical stock price data to a database.

    :param stock_info:
    :param stock_data: The downloaded stock price data.
    :param bench_data: The benchmark data.
    :param engine: A `ConnectionSettings` object containing the database connection settings.
    :param hp: A `HistoricalPrices` class object containing the name of the table to save the data to.
    :param interval_str: interval code of the data bars.
    """
    # tables = _download_data(**download_params.dict())
    historical_data, timestamp_data = _build_tables(
        stock_data, bench_data, interval_str=interval_str
    )
    historical_data.to_sql(hp.stock_data, engine, index=False, if_exists="replace")
    timestamp_data.to_sql(hp.timestamp_data, engine, index=False, if_exists="replace")
    stock_info.to_sql(hp.stock_info, engine, index=False, if_exists="replace")


def task_save_historical_data_to_database(
    download_args: mytypes.DownloadParams, connection_engine: sqlalchemy.Engine
) -> None:
    """
    Schedule the script to save historical data to a database.
    """
    stock_data, bench_data, stock_info = _download_data(**download_args.dict())
    save_historical_data_to_database(
        stock_data,
        bench_data,
        stock_info,
        connection_engine,
        mytypes.HistoricalPrices,
        interval_str=download_args.interval_str,
    )


def re_download_stock_data() -> None:
    """
    Download and cache stock data locally for testing.
    """
    stock_data, bench_data = _download_data(**mytypes.DownloadParams().dict())
    stock_data.to_pickle(pathlib.Path("../..") / "data" / "stock_data_raw_push_test.pkl")
    bench_data.to_pickle(pathlib.Path("../..") / "data" / "bench_data_raw_push_test.pkl")


def load_pickle_stock_data() -> tuple:
    """
    Load price data from pickle for testing.

    :return: A tuple containing the stock data and benchmark data.
    """
    stock_data = pd.read_pickle(
        pathlib.Path("../..") / "data" / "stock_data_raw_push_test.pkl"
    )
    bench_data = pd.read_pickle(
        pathlib.Path("../..") / "data" / "bench_data_raw_push_test.pkl"
    )
    return stock_data, bench_data


def initiate_data_updater_schedule(
    download_args: mytypes.DownloadParams,
    connection_engine: sqlalchemy.Engine,
    host: str,
    port: int,
) -> None:
    """
    Schedule the script to run once a day at a specific time.

    :param host:
    :param port:
    :param connection_engine:
    :param download_args:
    :param trigger_time: The time to run the script.
    """
    # Create a socket object
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Bind the socket to a specific address and port
        s.bind((host, port))

        # Listen for incoming connections
        s.listen()

        task_save_historical_data_to_database(
            download_args=download_args, connection_engine=connection_engine
        )


if __name__ == '__main__':
    initiate_data_updater_schedule(
        download_args=mytypes.DownloadParams(
            stock_table_url=env.DOWNLOAD_STOCK_TABLE_URL,
            bench=env.DOWNLOAD_BENCHMARK_SYMBOL,
            days=env.DOWNLOAD_DAYS_BACK,
            interval_str=env.DOWNLOAD_DATA_INTERVAL,
        ),
        connection_engine=env.ConnectionEngines.HistoricalPrices.NEON,
        host=env.HOST,
        port=env.PORT
    )
