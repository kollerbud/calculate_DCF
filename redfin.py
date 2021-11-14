import requests
import json
import os
import csv
from bs4 import BeautifulSoup
import sqlite3
from datetime import date



requests.packages.urllib3.disable_warnings()

view_box = {}


url = 'https://www.redfin.com/city/29470/IL/Chicago/filter/property-type=house+condo+townhouse+multifamily,max-price=500k,min-beds=2'

class GetFromRedfin:

    def __init__(self) -> None:
        # 
        self.base_url = 'https://www.redfin.com/city'
        self.request_header = { 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
                                'Referer': 'https://www.redfin.com/',
                                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                                'Accept-Encoding': 'gzip, deflate, br',
                                'Accept-Language': 'en-US,en;q=0.5',
                                'Connection': 'keep-alive',
                                }
        self.session = requests.Session()
        self.login_payload = {'username':os.environ.get('redfin_username'), 'password':os.environ.get('redfin_password')}
        self.update_session_cookie()
    
    def update_session_cookie(self):

        login_url = 'https://www.redfin.com/stingray/do/api-login'

        if self.load_cookie() ==200:
            print('re-using existing cookie')
        else:
            response = self.session.post(login_url,
                                    data=self.login_payload,
                                    headers= self.request_header
                                    )
            with open(r'.\cookies\session_cookie.json', 'w+') as f:
                f.write(json.dumps(self.session.cookies.items()))
                print('session cookie updated')

    
    def load_cookie(self):
        if not os.path.exists(r'.\cookies\session_cookie.json'):
            print('no cookie file found')
            return 400

        cookie = json.load(open(r'.\cookies\session_cookie.json'))
        for c in cookie:
            self.session.cookies.set(c[0], c[1])
        
        response = self.session.get(url=url, 
                                    verify=False, 
                                    headers=self.request_header)

        print('load existing cookie response is ', response.status_code)

        if response.status_code != 200:
            self.session.cookies.clear()
            return 400
        else:
            return 200

class DownloadCSV(GetFromRedfin):
        
    def _download_csv(self):

        response = self.session.get(url=url, headers=self.request_header)        
        download_link =[]
        bsobj = BeautifulSoup(response.content, 'html.parser')
        for link in bsobj.find_all(id='download-and-save'):
            if link['href']:
                download_link.append('https://www.redfin.com'+link['href'])

        download = self.session.get(url=download_link[0],
                                    headers=self.request_header)

        decode_content = download.content.decode('utf-8')
        csv_file = csv.reader(decode_content.splitlines(), delimiter=',')
        my_list = list(csv_file)

        return my_list
        
    def to_sqlite(self):
        self.conn = sqlite3.connect(r'D:\projects\real_estate\House_prices.db')
        self.cursor = self.conn.cursor()

        get_csv = self._download_csv()
        today = date.today().strftime('%m-%d-%y')
        for listings in get_csv[1:]:
            sql_statement = ''' INSERT INTO Redfin
                                ("SALE_TYPE", "SOLD_DATE", "PROPERTY_TYPE", 
                                 "ADDRESS", "CITY", "STATE", 
                                 "ZIP_CODE", "PRICE", "BEDS", 
                                 "BATHS", "LOCATION", "SQUARE_FEET",
                                 "LOT_SIZE", "YEAR_BUILT", "DAYS_ON_MARKET",
                                 "DOLLAR_PER_SQUARE_FEET", "HOA_PER_MONTH",
                                 "STATUS", "LIST_URL", "SOURCE",
                                 "MLS_NUMBER", "LATITUDE", "LONGITUDE",
                                 "SCRAPE_TIME")
                                 VALUES (?,?,?,
                                         ?,?,?,
                                         ?,?,?,
                                         ?,?,?,
                                         ?,?,?,
                                         ?,?,
                                         ?,?,?,
                                         ?,?,?,
                                         ?);
                            '''
            self.cursor.execute( sql_statement, 
                            (listings[0], listings[1], listings[2],
                            listings[3], listings[4], listings[5],
                            listings[6], listings[7], listings[8],
                            listings[9], listings[10], listings[11],
                            listings[12],listings[13], listings[14],
                            listings[15],listings[16],
                            listings[17],listings[20],listings[21],
                            listings[22],listings[25],listings[26],
                            today,)
                          )
        
        self.conn.commit()
        self.conn.close()

if __name__ == '__main__':
    print(DownloadCSV().to_sqlite())