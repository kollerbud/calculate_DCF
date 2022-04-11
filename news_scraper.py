import yfinance as yf
import finviz
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class GatherNews:

    def __init__(self, ticker) -> None:
        self.ticker = ticker
        self.analyzer = SentimentIntensityAnalyzer()

    @property
    def _from_yahoo(self) -> list:
        '''
        gather news from yahoo and
        run Vader sentimental analysis on news title
        '''
        yahoo_news = []
        for news in yf.Ticker(self.ticker).news:
            news.pop('uuid')
            news.pop('type')
            news.update({'title_sentiment_score':
                         self.analyzer.polarity_scores(news['title'])})
            yahoo_news.append(news)
        return yahoo_news

    @property
    def _from_finviz(self) -> list:
        '''
        gather news from Finviz and
        run vader sentimental analysis on news title
        '''
        finviz_news = []
        for news in finviz.get_news(self.ticker):
            finviz_news.append({'title': news[1],
                                'publisher': news[-1],
                                'link': news[2],
                                'publish_time': news[0],
                                'title_sentiment_score':
                                self.analyzer.polarity_scores(news[1])
                                })
        return finviz_news

    def gather_news(self) -> list:
        return self._from_yahoo + self._from_finviz


if __name__ == '__main__':

    r = GatherNews('SMH').gather_news()
    print(r)
