"""
Desc: loads configuration data to python environment
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv()

UPDATER_TRIGGER_TIME = os.environ.get('UPDATER_TRIGGER_TIME')

DOWNLOAD_STOCK_TABLE_URL = os.environ.get(
    'DOWNLOAD_STOCK_TABLE_URL',
    'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
)
DOWNLOAD_BENCHMARK_SYMBOL = os.environ.get('DOWNLOAD_BENCHMARK_SYMBOL', 'SPY')
DOWNLOAD_DAYS_BACK = os.environ.get('DOWNLOAD_DAYS_BACK', 365)
DOWNLOAD_DATA_INTERVAL = os.environ.get('DOWNLOAD_DATA_INTERVAL', '1d')


class HistoricalPrices:
    stock_data = 'stock_data'
    timestamp_data = 'timestamp_data'


class ConnectionEngines:
    class HistoricalPrices:
        NEON = create_engine(
            os.environ.get('NEON_DB_CONSTR')
        )
