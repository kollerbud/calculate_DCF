import praw
from api_keys import REDDIT_API_KEYS


reddit = praw.Reddit(client_id = REDDIT_API_KEYS.client_id,
                          client_secret = REDDIT_API_KEYS.client_secret,
                          user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0'
                           )
reddit.read_only=True

subreddit = reddit.subreddit('STOCKS').search(query='NVDA', time_filter='week')
for submission in subreddit:
    print(submission.title)

