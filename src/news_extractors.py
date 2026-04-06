import yfinance as yf
import pandas as pd
from datetime import datetime
import uuid
import trafilatura
from concurrent.futures import ThreadPoolExecutor, as_completed

class YahooNewsExtractor:
    """
    get recent news from yfinance
    """

    def __init__(self, ticker: str):
        self.ticker = str(ticker).upper()
        self.raw_news = pd.DataFrame()

    def get_metadata(self) -> pd.DataFrame:
        """
        pulls latest news metadata from yfinance
        return
            pandas dataframe
        """
        print(f'getting yahoo finance news from {self.ticker}')
        # yfinance return a list of dicts
        yf_ticker = yf.Ticker(self.ticker)
        news_items = yf_ticker.news

        if not news_items:
            print(f'no news found for {self.ticker}')
            return pd.DataFrame()

        clean_news = []
        for item in news_items:
            content = item.get('content', {})

            # iso dates
            pub_date_str = content.get('pubDate')
            formatted_date = None

            if pub_date_str:
                try:
                    clean_date_str = pub_date_str.replace('Z', '+00:00')
                    formatted_date = datetime.fromisoformat(clean_date_str).date()
                except ValueError:
                    pass
            provider = content.get('provider', {})
            # get urls
            click_url = content.get('clickThroughUrl') or {}
            canonical_url = content.get('canonicalUrl') or {}
            link = click_url.get('url') or canonical_url.get('url', '')

            if link:
                clean_news.append({
                    'uuid': str(uuid.uuid4()),
                    'ticker': self.ticker,
                    'title': content.get('title', ''),
                    'summary': content.get('summary', ''),
                    'publisher': provider.get('displayName', ''),
                    'link': link,
                    'publish_date': formatted_date,
                    'body_text': ''
                })

        self.raw_news = pd.DataFrame(clean_news)
        return self.raw_news

    def _scrape_single_article(self, url: str) -> str:
        'helper function to scrape from single url'
        if not url:
            return ""

        # download html
        downloaded = trafilatura.fetch_url(url)
        if downloaded is None:
            return ""

        # extract the main text
        # output in markdown for later LLM work
        text = trafilatura.extract(downloaded, include_links=False, include_images=False)

        return text if text else ""

    def get_full_text(self) -> pd.DataFrame:
        """
        iterates through metadata links and concurrently scrapes full article body
        """

        if self.raw_news.empty:
            print('no metadata to scrape, run get_metadata first')
            return self.raw_news

        print(f'scraping full text for {len(self.raw_news)} articles concurrently')

        urls = self.raw_news['link'].tolist()
        results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {executor.submit(self._scrape_single_article, url): url for url in urls}
            # gather results as they come in
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    text = future.result()
                    results.append({'link': url, 'body_text': text})
                except Exception as e:
                    print(f'error scraping {url}: {e}')
                    results.append({'link': url, 'body_text': ''})

        results_df = pd.DataFrame(results)
        # update news dataframe
        self.raw_news = self.raw_news.drop(columns=['body_text']).merge(results_df, on='link', how='left')
        # filter out failed scrapes
        successful_scrapes = self.raw_news[self.raw_news['body_text'].str.strip() != ""]
        print(f"Successfully extracted text for {len(successful_scrapes)} out of {len(self.raw_news)} articles.")

        return self.raw_news

    def run_pipeline(self) -> pd.DataFrame:
        self.get_metadata()
        return self.get_full_text()
    


if __name__ == '__main__':
    print(YahooNewsExtractor(ticker='zm').run_pipeline())