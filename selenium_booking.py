from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from datetime import date, datetime
import time
import sqlite3
    
def open_browser():
    ''' set up Firefox driver; disable flash and image loading '''
    firefox_profile = webdriver.FirefoxProfile()
    firefox_profile.set_preference('browser.privatebrowsing.autostart', True)
    firefox_profile.set_preference('permissions.default.image', 2)
    firefox_profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', False)
    driver = webdriver.Firefox(firefox_profile=firefox_profile)
    return driver

def search_key(key, key_is_province=False):
    ''' submit search key; redirect to the province level (option) '''
    global country
    global area
    ss = wait.until(EC.presence_of_element_located((By.ID, 'ss'))).send_keys(key)
    bttn = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'sb-searchbox__button  '))).click()
    country = driver.find_element(By.XPATH, "//nav[@id='breadcrumb']/ol/li[2]/div/a").text
    if key_is_province:
        area = driver.find_element(By.XPATH, "//nav[@id='breadcrumb']/ol/li[3]/div/a").text
        province_href = wait.until(EC.element_to_be_clickable((By.XPATH, "//nav[@id='breadcrumb']/ol/li[3]/div/a"))).click()
        sbbutton = wait.until(EC.element_to_be_clickable((By.CLASS_NAME,'sb-searchbox__button  '))).click()
    else: area = ''

        
def select_filter(filter_id):
    ''' select filter by filter_id '''
    print('Selecting filter', filter_id, '...')
    try: filter = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[@data-id='"+filter_id+"']"))).click()
    except: pass

def get_results():
    ''' retrieve the number of items and pages returned '''
    global item_count
    global page_count
    global item_count_last_page
    time.sleep(15)
    s = driver.find_element_by_class_name('sorth1').text
    item_count = int(s.split(': ')[1].split(' ')[0].replace(',', ''))
    if item_count % 15 == 0:
        page_count = int(item_count / 15)
        item_count_last_page = 15
    else:
        page_count = int(item_count // 15) + 1
        item_count_last_page = int(item_count % 15)
    print(item_count, page_count, item_count_last_page)
    if item_count > 1000: print('Warning: items beyond 1000 will not be shown in the search result and thus scraped.')
    
def scrape_page(i, old_ids):
    ''' scrape one result page '''
    global scrape_search_counter
    corr = 0 # correct sequence error if review score is null
    ids = list() # store list of id's to the next page iteration for duplication check
    if i < page_count - 1: item_count_current_page = 15
    else: item_count_current_page = item_count_last_page
    
    ''' iterate through the result items on the page '''
    for j in range(item_count_current_page):
        print('==== scraping item',j+1,'of',item_count_current_page,
              '- page',i+1,'of',page_count,
              '- #', scrape_search_counter+1, 'of', item_count, '====')
        
        ''' retrieve data '''
        fail = 0
        while True:
            id = driver.find_elements(By.XPATH, "//div[@class='sr_item_photo']/..")[j].get_attribute('data-hotelid')
            if id == old_ids[j]:
                fail = fail + 1
                print('duplicate check failed - re-checking #', fail)
                time.sleep(3)
                if fail%10 == 0:
                    driver.refresh()
                    wait.until(EC.presence_of_all_elements_located((By.ID, 'hotellist_inner')))
                continue # end this trial and jump back to the start of the while loop 
            else: print('id(=0):', id, '\nid(t-1):', old_ids[j], '\ncheck okay!')
            name = driver.find_elements(By.CLASS_NAME, 'sr-hotel__name')[j].text
            url_link = driver.find_elements(By.CLASS_NAME, 'hotel_name_link')[j].get_attribute('href').split('?')[0]
            star_rating = driver.find_elements(By.XPATH, "//div[@class='sr_item_photo']/..")[j].get_attribute('data-class')
            review_score = driver.find_elements(By.XPATH, "//div[@class='sr_item_photo']/..")[j].get_attribute('data-score')
            if review_score == '': review_count = ''; corr = corr + 1
            else: review_count = int(driver.find_elements(By.CLASS_NAME, 'bui-review-score__text')[j-corr]
                                     .text.split(' ')[0].replace(',', ''))
            retrieved_at = datetime.now()
            
            '''output to sqlite'''
            conn = sqlite3.connect('hotels.sqlite')
            with conn:
                cur = conn.cursor()
                cur.execute('''INSERT OR REPLACE INTO Search_Result_Booking (
                id, name, country, area, url_link, star_rating, review_score, review_count, retrieved_at)
                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (id, name, country, area, url_link, star_rating, review_score, review_count, retrieved_at))
            conn.commit()
            ''' wrap up '''
            log_entry = str(i)+';'+str(j)+';'+id+';'+name+';'+key+';'+url_link+';'+star_rating\
            +';'+review_score+';'+str(review_count)+';'+str(retrieved_at)
            print(log_entry)
            ids.append(id)
            scrape_search_counter = scrape_search_counter + 1
            print('==== item scraped ====')
            break
    conn.commit()
    return ids
                    
def scrape_search():
    ''' navigate through pages and scrape'''
    old_ids = []
    for i in range(15): old_ids.append('')
    for i in range(page_count):
        old_ids = scrape_page(i, old_ids)
        if i == page_count - 1:
            print(key, '- scraping completed!')
            break
        print('Navigating to the next page ...')
        wait.until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Next page']")))
        driver.find_element(By.XPATH, "//a[@title='Next page']").click()
        wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'bui-u-inline')))
        time.sleep(1)

def create_sqlite_table():
    conn = sqlite3.connect('hotels.sqlite')
    cur = conn.cursor()
    cur.executescript('''
        CREATE TABLE IF NOT EXISTS Search_Result_Booking (
            id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
            name    TEXT,
            country TEXT,
            area    TEXT,
            url_link    TEXT UNIQUE,
            star_rating    INTEGER,
            review_score REAL,
            review_count INTEGER,
            retrieved_at STRING
    );
    ''')

domain = 'https://www.booking.com'
search_keys = ['Singapore','Hong Kong']
key_is_province = False

if __name__ == "__main__":
    print(len(search_keys), 'search keys received:\n', search_keys)
    driver = open_browser()
    wait = WebDriverWait(driver, 10) # higher if you have slow internet
    create_sqlite_table()
    for key in search_keys:
        ''' params '''
        country = None
        area = None
        item_count = None
        page_count = None
        item_count_last_page = None
        search_results = None
        scrape_search_counter = 0
        ''' access, set filters and scrape '''
        driver.get(domain)
        search_key(key, key_is_province)
        select_filter('class-4') # filter - hotel type: 4 stars
        select_filter('ht_id-204') # filter - hotel type: Hotel
        select_filter('ht_id-206') # filter - hotel type: Resort
        select_filter('hr_24-8') # filter - front desk open 24/7
        get_results()
        scrape_search()
