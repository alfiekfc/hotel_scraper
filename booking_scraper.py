import urllib3
import certifi
import ssl
import bs4
import json
import re
import pandas as pd
import sqlite3
from datetime import datetime
import time

keys = ['Marriott', 'Hilton']
start = 0 # start position of the first iteration

headers_values = {
    'user-agent': 'Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0', 
    'lang': 'en-gb', 
    'selected_currency': 'hotel_currency'
}
http = urllib3.PoolManager(
    cert_reqs='CERT_REQUIRED', 
    ca_certs=certifi.where(), 
    num_pools=15, 
    headers=headers_values
)

conn = sqlite3.connect('hotels.sqlite')
cur = conn.cursor()
cur.executescript('''
    CREATE TABLE IF NOT EXISTS Hotel_Booking (
        id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
        name    TEXT,
        hotel_chain    TEXT,
        star_rating    TEXT,
        property_type TEXT,
        url_link     TEXT UNIQUE,
        address_addressRegion TEXT,
        address_addressLocality TEXT,
        address_postalCode TEXT,
        address_streetAddress TEXT,
        address_addressCountry TEXT,
        coordinate STRING,
        listed_since STRING,
        priceRange_text TEXT,
        priceRange_value INT,
        priceRange_currency TEXT,
        reviewCount INTEGER,
        reviewScore_aggregate REAL,
        reviewScore_staff REAL,
        reviewScore_facilities REAL,
        reviewScore_cleanliness REAL,
        reviewScore_comfort REAL,
        reviewScore_valueForMoney REAL,
        reviewScore_location REAL,
        retrieved_at STRING
    );
''')

for key in keys:
    df = pd.read_sql_query("SELECT id, url_link FROM Search_Result_Booking WHERE name LIKE '%"+key+"%'", conn)
    print(df['url_link'])
    count = 0
    for id_, url_link in zip(list(df['id'])[start:],list(df['url_link'])[start:]):
        count = count + 1
        print('==== scraping', key, '#', count+start, url_link, '====')
        
        ''' get hotel page from Booking.com '''
        rspns = http.request('GET', url_link, timeout=30, retries=20)
        soup = bs4.BeautifulSoup(rspns.data, 'html.parser')
        juice = json.loads(soup.find('script', type='application/ld+json').get_text())
        
        ''' scrape soup '''
        try: id = soup.find(attrs={'data-hotel-id': True})['data-hotel-id']
        except: id = id_
        name = juice['name']
        try: hotel_chain  = soup.find('p', class_='summary hotel_meta_style').get_text().split(':')[1].strip()
        except: hotel_chain = None
        star_rating = None
        try: star_rating = soup.find('i', class_='bk-icon-wrapper bk-icon-stars star_track')['title']
        except: pass
        try: star_rating = soup.select_one('span.hp__hotel_ratings__stars.nowrap > div > i > svg')['class'][1]
        except: pass
        try:
            reviewScore_staff = soup.find_all('span', class_='c-score-bar__score')[0].get_text()
            reviewScore_facilities = soup.find_all('span', class_='c-score-bar__score')[1].get_text()
            reviewScore_cleanliness = soup.find_all('span', class_='c-score-bar__score')[2].get_text()
            reviewScore_comfort = soup.find_all('span', class_='c-score-bar__score')[3].get_text()
            reviewScore_valueForMoney = soup.find_all('span', class_='c-score-bar__score')[4].get_text()
            reviewScore_location = soup.find_all('span', class_='c-score-bar__score')[5].get_text()
        except:
            reviewScore_staff = None
            reviewScore_facilities = None
            reviewScore_cleanliness = None
            reviewScore_comfort = None
            reviewScore_valueForMoney = None
            reviewScore_location= None
        
        ''' scrape juice (json) '''
        property_type = juice['@type']
        url_link = juice['url']
        address_addressRegion = juice['address']['addressRegion']
        address_addressLocality = juice['address']['addressLocality']
        address_postalCode = juice['address']['postalCode']
        address_streetAddress = juice['address']['streetAddress']
        address_addressCountry = juice['address']['addressCountry']
        coordinate = juice['hasMap'].split('center=')[1].split('&')[0]
        listed_since = soup.find('span', 'hp-desc-highlighted').get_text().split('since ')[1].replace('.','')
        try:
            priceRange_text = juice['priceRange'].strip()
            priceRange_value = re.findall('([0-9]+)', juice['priceRange'].replace(',',''))[0]
            priceRange_currency = re.findall('start\sat\s(.+)?\s', juice['priceRange'])[0]
        except:
            priceRange_text = None
            priceRange_value = None
            priceRange_currency = None
        try:
            reviewCount = int(juice['aggregateRating']['reviewCount'])
            reviewScore_aggregate = juice['aggregateRating']['ratingValue']
        except:
            reviewCount = 0
            reviewScore_aggregate = None
        retrieved_at = datetime.now()
        
        ''' print out entry '''
        print(
            id,
            name,
            hotel_chain,
            star_rating,
            property_type,
            url_link,
            address_addressRegion,
            address_addressLocality,
            address_postalCode,
            address_streetAddress,
            address_addressCountry,
            coordinate,
            listed_since,
            priceRange_text,
            priceRange_value,
            priceRange_currency,
            reviewCount,
            reviewScore_aggregate,
            reviewScore_staff,
            reviewScore_facilities,
            reviewScore_cleanliness,
            reviewScore_comfort,
            reviewScore_valueForMoney,
            reviewScore_location,
            retrieved_at,
        )

        ''' store entry into SQLite '''
        conn = sqlite3.connect('hotels.sqlite')
        with conn:
            cur = conn.cursor()
            cur.execute(
                '''INSERT OR REPLACE INTO Hotel_Booking (
                    id,
                    name,
                    hotel_chain,
                    star_rating,
                    property_type,
                    url_link,
                    address_addressRegion,
                    address_addressLocality,
                    address_postalCode,
                    address_streetAddress,
                    address_addressCountry,
                    coordinate,
                    listed_since,
                    priceRange_text,
                    priceRange_value ,
                    priceRange_currency,
                    reviewCount,
                    reviewScore_aggregate,
                    reviewScore_staff,
                    reviewScore_facilities,
                    reviewScore_cleanliness,
                    reviewScore_comfort,
                    reviewScore_valueForMoney,
                    reviewScore_location,
                    retrieved_at
                )
                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    id,
                    name,
                    hotel_chain,
                    star_rating,
                    property_type,
                    url_link,
                    address_addressRegion,
                    address_addressLocality,
                    address_postalCode,
                    address_streetAddress,
                    address_addressCountry,
                    coordinate,
                    listed_since,
                    priceRange_text,
                    priceRange_value ,
                    priceRange_currency,
                    reviewCount,
                    reviewScore_aggregate,
                    reviewScore_staff,
                    reviewScore_facilities,
                    reviewScore_cleanliness,
                    reviewScore_comfort,
                    reviewScore_valueForMoney,
                    reviewScore_location,
                    retrieved_at
                )
            )
        conn.commit()
        print('==== completed and stored in SQLite ====')
        
    start = 0 # reset to zero for the next iteration
