from gnews import GNews

google_news = GNews()
us_news = google_news.get_news('US')
print(us_news[0])