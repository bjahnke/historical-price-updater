import typing
import pandas as pd
from src.historical_price_updater import price_updater
import env as env
from flask import Flask
import certifi
from pymongo import MongoClient
from src.historical_price_updater import utils
import src.watchlist
from pykeepass import PyKeePass
import os
from pathlib import Path
import subprocess
from sqlalchemy import create_engine, URL

app = Flask(__name__)
ca = certifi.where()


def get_latest_keypass_db():
    keypass_repo = os.environ['KEYPASS_REPO']
    subprocess.run(['git', '-C', keypass_repo, 'pull'], check=True)
    return PyKeePass(
        filename=Path(os.environ['KEYPASS_REPO']) / os.environ['KEYPASS_DB'],
        password=os.environ['KEYPASS_PHRASE'],
        keyfile=os.environ['KEYPASS_KEYFILE']
    )


def get_sp500_watchlist(engine):
    """get the latest SP500 stocks and save them to the database, return the watchlist records for the SP500 stocks"""
    _, latest_sp500 = utils.get_wikipedia_stocks('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    latest_sp500 = latest_sp500.rename(columns={'Symbol': 'symbol'})

    latest_sp500.to_sql('stock_info', engine, if_exists='replace')

    new_watchlist_records_from_sp500 = latest_sp500[['symbol']].copy()
    new_watchlist_records_from_sp500['data_source'] = 'yahoo'
    new_watchlist_records_from_sp500['interval'] = '1d'
    new_watchlist_records_from_sp500['market_index'] = 'SPY'
    new_watchlist_records_from_sp500['sec_type'] = 'STK'

    return new_watchlist_records_from_sp500


def main(watchlist_api_key: str, engine):
    """
    This is the main function that will be called by the cloud function.
    :return:
    """
    watchlist_client = src.watchlist.MongoWatchlistClient(watchlist_api_key)
    watchlist = watchlist_client.get_latest('asset-tracking')['watchlist']

    watchlist = pd.DataFrame.from_records(watchlist)
    # if SP500 is in watchlist, track all current SP500 stocks
    if 'SP500' in watchlist['symbol'].unique():
        sp500_watchlist = get_sp500_watchlist(engine)
        # remove the SP500 from the watchlist
        watchlist = watchlist.loc[watchlist['symbol'] != 'SP500'].copy()
        # add the SP500 watchlist to the watchlist
        watchlist = pd.concat([watchlist, sp500_watchlist]).reset_index(drop=True).drop_duplicates()

    price_updater.task_save_historical_data_to_database(
        watchlist,
        connection_engine=engine,
    )


@app.route('/', methods=['POST'])
def handle_request():
    """
    This is the main function that will be called by the cloud function.
    :return:
    """
    connection_string = URL.create(
        'postgresql',
        username='bjahnke71',
        password=os.environ.get('NEON_DB_PASSWORD'),
        host='ep-spring-tooth-474112.us-east-2.aws.neon.tech',
        database='historical-stock-data',
    )
    main(env.WATCHLIST_API_KEY, create_engine(connection_string, connect_args={'sslmode':'require'}))
    return 'Service executed with no errors'


if __name__ == '__main__':
    handle_request()
    # app.run(host='0.0.0.0', port=8080)

