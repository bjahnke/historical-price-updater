import pandas as pd
import src.watchlist as watchlist
import env


def update_watchlist():
    df = pd.read_excel('..\\watchlist.xlsx', index_col=0)
    w = watchlist.MongoWatchlistClient(env.WATCHLIST_API_KEY)
    w.update_watchlist(df.to_dict(orient='records'), 'asset-tracking')


if __name__ == '__main__':
    update_watchlist()
