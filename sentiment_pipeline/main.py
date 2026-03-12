'''main file for cloud function'''
from news_scraper import GatherYahooNews
from reddit_scraper import GatherRedditPosts
from ticker_list import get_list_of_ticker


def run(request_send=None):
    '''don't need any input from trigger'''
    # get a list of tickers to track
    tickers = get_list_of_ticker()
    # grab news and reddit posts
    for ticker in tickers:
        GatherYahooNews(ticker=ticker).upload_to_bigquery()
        GatherRedditPosts(ticker=ticker).upload_to_bigquery()

        print(f'finished running {ticker}')

    return 'runs finished'
