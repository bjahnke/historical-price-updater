import price_updater as hpu
import mytypes as mytypes
import env as env
import sqlalchemy
import schedule
import time
import socket


def initiate_data_updater_schedule(
    download_args: mytypes.DownloadParams,
    connection_engine: sqlalchemy.Engine,
    trigger_time: str,
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
    initiate_data_updater_schedule(
        download_args=mytypes.DownloadParams(
            stock_table_url=env.DOWNLOAD_STOCK_TABLE_URL,
            bench=env.DOWNLOAD_BENCHMARK_SYMBOL,
            days=env.DOWNLOAD_DAYS_BACK,
            interval_str=env.DOWNLOAD_DATA_INTERVAL,
        ),
        connection_engine=env.ConnectionEngines.HistoricalPrices.NEON,
        trigger_time=env.UPDATER_TRIGGER_TIME,
        host=env.HOST,
        port=env.PORT
    )
