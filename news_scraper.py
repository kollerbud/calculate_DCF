import yfinance as yf
import finviz
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
from datetime import datetime


class GatherNews:
    analyzer = SentimentIntensityAnalyzer()

    def __init__(self, ticker) -> None:
        self.ticker = str(ticker).upper()
    
    @property
    def _from_yahoo(self) -> list:
        '''
        gather news from yahoo and
        run Vader sentimental analysis on news title
        '''
        yahoo_news = []
        for news in yf.Ticker(self.ticker).news[:10]:
            news.pop('uuid')
            news.pop('type')
            news.update({'title_sentiment_score':
                         (self.analyzer.polarity_scores(news['title']).values())})
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
        for news in finviz.get_news(self.ticker)[:10]:
            finviz_news.append({'title': news[1],
                                'publisher': news[-1],
                                'link': news[2],
                                'providerPublishTime': news[0],
                                'title_sentiment_score':
                                (self.analyzer.polarity_scores(news[1]).values())
                                })

        return finviz_news

    def analysts_targets(self) -> pd.DataFrame:
        _rating = finviz.get_analyst_price_targets(self.ticker)
        _rating = pd.DataFrame.from_dict(_rating)

        return _rating

    def gather_news(self) -> pd.DataFrame:
        df_yahoo = pd.DataFrame.from_dict(self._from_yahoo)
        df_finviz = pd.DataFrame.from_dict(self._from_finviz)

        df = pd.concat([df_yahoo, df_finviz], axis=0)
        df = df.reset_index(drop=True)

        return df


if __name__ == '__main__':
    x = GatherNews('snow').gather_news()
    print(x)