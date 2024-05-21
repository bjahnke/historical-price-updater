"""
Desc: loads configuration data to python environment
"""
from sqlalchemy import create_engine
from env.env_auto import *

# Load environment variables from .env file
load_dotenv()

# DOWNLOAD_STOCK_TABLE_URL = os.environ.get(
#     "DOWNLOAD_STOCK_TABLE_URL",
#     "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
# )
DOWNLOAD_BENCHMARK_SYMBOL = os.environ.get("DOWNLOAD_BENCHMARK_SYMBOL", "SPY")
DOWNLOAD_DAYS_BACK = os.environ.get("DOWNLOAD_DAYS_BACK", 365)
DOWNLOAD_DATA_INTERVAL = os.environ.get("DOWNLOAD_DATA_INTERVAL", "1d")


# class ConnectionEngines:
#     class HistoricalPrices:
#         NEON = create_engine(NEON_DB_CONSTR)
