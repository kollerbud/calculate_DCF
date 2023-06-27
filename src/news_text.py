import requests
from bs4 import BeautifulSoup
import time
import httpx


class ExtractYahooNewsText:
    '''analyze news text and output sentiments'''
    # getting requests header ready for web scraping
    header = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
    }
    sessions = httpx.Client(headers=header)

    def build_cookies(self):
        '''build cookies'''
        cookie = None
        response = self.sessions.get(
            "https://fc.yahoo.com", headers=self.header,
            allow_redirects=True, timeout=10,
            )
        if not response.cookies:
            raise Exception("Failed to obtain Yahoo auth cookie.")

        cookie = list(response.cookies)[0]

        return cookie

    def extract_text(self, link: str):
        '''
        grab yahoo news articles main body text
        '''
        # get cookies
        time.sleep(0.5)

        response = self.sessions.get(link, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        paragraphs = soup.find('div', class_='caas-body')
        if paragraphs is None:
            raise ValueError('paragraphs is None, cookie roadblock?')
        # append text block
        news_text = str()
        for p in paragraphs.find_all('p'):
            news_text += p.text

        return news_text


if __name__ == '__main__':
    url = 'https://finance.yahoo.com/m/a0beff36-0374-32bf-b8f6-bcd2e2d7352d/a-bull-market-is-coming%3A-2.html'
    print(ExtractYahooNewsText().extract_text(link=url))
