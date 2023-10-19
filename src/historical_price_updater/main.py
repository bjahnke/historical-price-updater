import typing
import pandas as pd
from src.historical_price_updater import price_updater
import env as env
from flask import Flask
import certifi
from pymongo import MongoClient
from src.historical_price_updater import utils
import src.watchlist

app = Flask(__name__)
ca = certifi.where()


# class MongoWatchlistClient:
#     def __init__(
#             self,
#             username,
#             password,
#             db_deployment,
#             db_name: str,
#             collection_name: str
#     ):
#         url = f'mongodb+srv://{username}:{password}@{db_deployment}.mongodb.net/?retryWrites=true&w=majority'
#         self.mongo_client = MongoClient(url, tlsCAFile=ca)
#         self.db = self.mongo_client[db_name]
#         self.collection = self.db[collection_name]
#         self.username = username
#         self._latest_watchlist = None
#
#     @property
#     def watchlist(self):
#         if self._latest_watchlist is None:
#             self._latest_watchlist = self.get_latest()
#         return self._latest_watchlist
#
#     def get_latest(self):
#         latest_entry = self.collection.find_one({"username": self.username}, sort=[("_id", -1)])
#         return latest_entry['watchlist']
#
#     def get_latest_as_csv(self, address: str):
#         """
#         Get the latest watchlist as a csv file.
#         :param address:
#         :return:
#         """
#         return pd.DataFrame(self.get_latest()).to_csv(address)
#
#     def update(self, watchlist: typing.List):
#         return self.collection.insert_one({
#             'username': self.username,
#             'watchlist': watchlist
#         })
#
#     def update_with_csv(self, address: str):
#         """
#         Update the watchlist with a csv file.
#         :param address:
#         :return:
#         """
#         df = pd.read_csv(address)
#         return self.update(df.to_dict(orient='records'))
#
#     def yf_download_data(self):
#         """
#         Download price data of watchlist from yahoo finance with the watchlist.
#         :return:
#         """
#         yahoo_source_stocks = self.watchlist.loc[self.watchlist["data_source"] == "yahoo"]
#         downloaded_data = pd.concat([
#             utils.yf_download_data(
#                 yahoo_source_stocks.symbol.loc[yahoo_source_stocks.interval == interval].to_list(),
#                 int(env.DOWNLOAD_DAYS_BACK),
#                 interval
#             )
#             for interval in yahoo_source_stocks["interval"].unique()
#         ])
#         return downloaded_data


def sync_watchlist(watchlist: pd.DataFrame, engine):

    _, latest_sp500 = utils.get_wikipedia_stocks('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    latest_sp500 = latest_sp500.rename(columns={'Symbol': 'symbol'})

    latest_sp500.to_sql('stock_info', engine, if_exists='replace')

    new_watchlist_records_from_sp500 = latest_sp500[['symbol']].copy()
    new_watchlist_records_from_sp500['data_source'] = 'yahoo'
    new_watchlist_records_from_sp500['interval'] = '1d'
    new_watchlist_records_from_sp500['auto_synced'] = True
    new_watchlist_records_from_sp500['market_index'] = 'SPY'

    synced_watchlist = watchlist.loc[watchlist['auto_synced'] == False].copy()
    synced_watchlist = pd.concat([synced_watchlist, new_watchlist_records_from_sp500]).reset_index(drop=True)

    return synced_watchlist


@app.route('/', methods=['POST'])
def handle_request():
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
        synced_watchlist = sync_watchlist(watchlist, engine)
        if not synced_watchlist.equals(watchlist):
            watchlist_client.update_watchlist(synced_watchlist.to_dict(orient='records'), 'asset-tracking')
            watchlist = synced_watchlist

    price_updater.task_save_historical_data_to_database(
        watchlist,
        connection_engine=engine,
    )
    return 'Service executed with no errors'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

