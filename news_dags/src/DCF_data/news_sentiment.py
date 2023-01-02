import requests
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dataclasses import dataclass
import functools
import time


@dataclass
class NewsSentiment:

    # getting requests header ready for web scraping
    header = {
        # Chrome 83 Windows
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Referer": "https://www.google.com/",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9"
    }
    r = requests.Session()
    r.headers = header

    # link of website
    link: str

    @property
    def yahoo_news_check(self):
        'check if link is yahoo news'
        
        scrape_ready = ['https://finance.yahoo.com/news',
                        'https://finance.yahoo.com/video']

        if any(x in self.link for x in scrape_ready):
            return True
        else:
            return False

    @functools.cached_property
    def scrape_content(self):
        '''
        grab yahoo news articles main body text
        '''
        time.sleep(0.1)

        response = self.r.get(self.link)
        soup = BeautifulSoup(response.content, 'html.parser')

        paragraphs = soup.find('div', class_='caas-body')
        # append text block
        news_text = str()
        for p in paragraphs.find_all('p'):
            news_text += p.text

        return news_text

    def analysis(self) -> dict:

        if self.yahoo_news_check:

            news_text = self.scrape_content
            analyzer = SentimentIntensityAnalyzer()

            # text sentiment analyzer from vader analyzer
            score = analyzer.polarity_scores(news_text)
            # convert score to list of tuples
            self.sentiment_score = [(key, value) for key, value in score.items()]

            return {
                'textBody': news_text,
                'score': self.sentiment_score
            }

        else:
            return {
                'textBody': None,
                'score': None
            }

if __name__ == '__main__':
    url = 'https://finance.yahoo.com/video/meta-amazon-tesla-stocks-fall-152718526.html'
    print(NewsSentiment(link=url).analysis())