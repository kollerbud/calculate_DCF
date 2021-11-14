import requests
import json

from requests.models import Response
from api_keys import NEWS_API_KEYS


api_key = NEWS_API_KEYS.api_key

req_header = { 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
                }
# REST address https://newsapi.org/v2/everything?q=Apple&from=2021-10-18&sortBy=popularity&apiKey=API_KEY

url = 'https://newsapi.org/v2/everything?q=Apple&from=2021-10-17&sortBy=popularity&apiKey='
top_lines_url = f'https://newsapi.org/v2/top-headlines?category=business&q=SPAC&country=us&apiKey={api_key}'

response = requests.get(top_lines_url, headers=req_header)
print(response.json())
