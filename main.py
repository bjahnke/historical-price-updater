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





@app.route('/', methods=['POST'])
def handle_request():
    """
    This is the main function that will be called by the cloud function.
    :return:
    """
    price_updater.main()
    return 'Service executed with no errors'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

