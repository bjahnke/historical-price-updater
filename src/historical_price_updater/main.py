import price_updater as hpu
import mytypes as mytypes
import env as env
from flask import Flask

app = Flask(__name__)


@app.route('/', methods=['POST'])
def handle_request():
    """
    This is the main function that will be called by the cloud function.
    :return:
    """
    hpu.task_save_historical_data_to_database(
        download_args=mytypes.DownloadParams(
            stock_table_url=env.DOWNLOAD_STOCK_TABLE_URL,
            bench=env.DOWNLOAD_BENCHMARK_SYMBOL,
            days=env.DOWNLOAD_DAYS_BACK,
            interval_str=env.DOWNLOAD_DATA_INTERVAL,
        ),
        connection_engine=env.ConnectionEngines.HistoricalPrices.NEON,
    )
    return 'Service executed with no errors'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

