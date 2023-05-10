import historical_price_updater.price_updater as hpu
import historical_price_updater.mytypes as mytypes
import historical_price_updater.env as env
import sqlalchemy
import schedule
import time


def initiate_data_updater_schedule(
    download_args: mytypes.DownloadParams,
    connection_engine: sqlalchemy.Engine,
    trigger_time: str = "02:30",
) -> None:
    """
    Schedule the script to run once a day at a specific time.

    :param connection_engine:
    :param download_args:
    :param trigger_time: The time to run the script.
    """
    # Schedule the script to run once a day at trigger_time
    schedule.every().day.at(trigger_time).do(
        hpu.task_save_historical_data_to_database,
        download_args=download_args,
        connection_engine=connection_engine,
    )

    while True:
        # Check if any scheduled jobs are due to run
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    hpu.task_save_historical_data_to_database(
        mytypes.DownloadParams(
            stock_table_url=env.DOWNLOAD_STOCK_TABLE_URL,
            bench=env.DOWNLOAD_BENCHMARK_SYMBOL,
            days=env.DOWNLOAD_DAYS_BACK,
            interval_str=env.DOWNLOAD_DATA_INTERVAL,
        ),
        env.ConnectionEngines.HistoricalPrices.NEON,
    )
    initiate_data_updater_schedule(
        mytypes.DownloadParams(
            stock_table_url=env.DOWNLOAD_STOCK_TABLE_URL,
            bench=env.DOWNLOAD_BENCHMARK_SYMBOL,
            days=env.DOWNLOAD_DAYS_BACK,
            interval_str=env.DOWNLOAD_DATA_INTERVAL,
        ),
        env.ConnectionEngines.HistoricalPrices.NEON,
        env.UPDATER_TRIGGER_TIME,
    )
