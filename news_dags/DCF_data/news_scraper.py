from __future__ import absolute_import
import yfinance as yf
import finviz
import pandas as pd
from datetime import datetime
from news_dags.DCF_data.news_sentiment import NewsSentiment


class GatherNews:

    def __init__(self, ticker) -> None:
        self.ticker = str(ticker).upper()

    @property
    def _from_yahoo(self) -> list:
        '''
        gather news from yahoo and
        run Vader sentimental analysis on news title
        '''
        # apparently yahoo only returns 8 articles
        yahoo_news = []
        for news in yf.Ticker(self.ticker).news[:10]:
            news.update({'ticker': self.ticker})
            news.pop('uuid')
            news.pop('type')
            news['providerPublishTime'] = (datetime.utcfromtimestamp(news['providerPublishTime']).strftime('%Y-%m-%d'))
            yahoo_news.append(news)

        return yahoo_news

    @property
    def _from_finviz(self) -> pd.DataFrame:
        '''
        gather news from Finviz and
        run vader sentimental analysis on news title
        '''
        finviz_news = []
        for news in finviz.get_news(self.ticker)[:20]:
            analyzed = NewsSentiment(link=news[2]).analysis()
            finviz_news.append({
                'ticker': self.ticker,
                'title': news[1],
                'publisher': news[-1],
                'link': news[2],
                'providerPublishTime': news[0],
                'news_sentiment': analyzed['score'],
                'newsText': analyzed['textBody']
            })

        return finviz_news

    def gather_news(self) -> pd.DataFrame:
        # combine both news source
        df_finviz = pd.DataFrame.from_dict(self._from_finviz)
        #df_yahoo = pd.DataFrame.from_dict(self._from_yahoo)

        #df = pd.concat([df_yahoo, df_finviz], axis=0)
        df = df_finviz
        df['providerPublishTime'] = pd.to_datetime(df['providerPublishTime'],
                                                   format='%Y-%m-%d').dt.date
        df.drop_duplicates(subset=['title'], inplace=True)

        return df

    def analysts_targets(self) -> pd.DataFrame:
        # what to do with analyst ratings
        _rating = finviz.get_analyst_price_targets(self.ticker)
        _rating = pd.DataFrame.from_dict(_rating)

        return _rating


if __name__ == '__main__':
    # 'nvda', 'amd', 'snow', 'axp', 'gs', 'intc', 'net', 'msft'
    #icker_list = get_list_of_ticker()
   print(GatherNews(ticker='amd').gather_news())
