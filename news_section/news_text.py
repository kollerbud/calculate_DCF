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

    def extract_text(self, link: str):
        '''
        grab yahoo news articles main body text
        '''
        # get cookies
        time.sleep(0.5)
        news_text = str()
        try:
            response = self.sessions.get(link, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            paragraphs = soup.find('div', class_='caas-body')
            # append text block
            for p in paragraphs.find_all('p'):
                news_text += p.text

        except AttributeError:
            news_text += ''

        return news_text
