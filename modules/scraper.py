# modules/scraper.py

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import dateparser
import logging

from discogs import db_discogs
from modules import utils, db

logger = logging.getLogger(__name__)


class Scraper:
    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1280,1024")
        chrome_options.add_argument("--enable-websocket-over-http2")
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.165 Safari/537.36"
        )
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        self.service = Service(ChromeDriverManager(driver_version="134.0.6998.165").install())
        self.driver = webdriver.Chrome(service=self.service, options=chrome_options)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.quit()

    def scrape_table(self, url):
        try:
            self.driver.get(url)
            self.driver.implicitly_wait(10)

            table = self.driver.find_element(By.CLASS_NAME, 'table_c5ftk')
            if not table:
                logger.warning(f"No table found at {url}")
                return None

            headers = [th.text.strip() for th in table.find_elements(By.TAG_NAME, 'th')]
            rows = [td.text.strip() for td in table.find_elements(By.TAG_NAME, 'td')]

            return pd.DataFrame([rows], columns=headers) if headers else pd.DataFrame(rows)

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None


def scrape_row(discogs_client, discogs_id=0):
    row = db_discogs.fetch_row(discogs_id)
    discogs_release = discogs_client.release(discogs_id)
    release_url = discogs_release.url

    logger.debug(release_url.split('/')[-1])
    with Scraper() as scraper:
        df = scraper.scrape_table(release_url)
        if df is not None:
            try:
                release_date_str = df.at[0, 'Released:']
                if release_date_str:
                    settings = {
                        'PREFER_DAY_OF_MONTH': 'first',
                        'PREFER_MONTH_OF_YEAR': 'first',
                        'DATE_ORDER': 'DMY',
                        'PREFER_LOCALE_DATE_ORDER': False,
                        'REQUIRE_PARTS': ['day', 'month', 'year']
                    }

                    date_object = dateparser.parse(release_date_str, settings=settings)
                    if date_object:
                        release_date_str = date_object.strftime("%Y-%m-%d")
                    else:
                        settings['REQUIRE_PARTS'] = ['month', 'year']
                        date_object = dateparser.parse(release_date_str, settings=settings)
                        if date_object:
                            release_date_str = date_object.strftime("%Y-%m")
                        else:
                            settings['REQUIRE_PARTS'] = ['year']
                            date_object = dateparser.parse(release_date_str, settings=settings)
                            release_date_str = date_object.strftime("%Y") if date_object else ''
            except Exception as e:
                logger.warning(f"Could not parse release date: {e}")
                release_date_str = ''
            finally:
                release_date = utils.earliest_date(row.release_date, release_date_str)
                db_discogs.set_release_date(row.discogs_id, release_date)


def scrape_discogs(config):
    discogs_client, *_ = db_discogs.connect_to_discogs(config)
    with db.context_manager() as cur:
        cur.execute("SELECT * FROM discogs_releases ORDER BY sort_name, discogs_id")
        rows = cur.fetchall()
        print(f'{len(rows)} rows')
        with Scraper() as scraper:
            for row in rows:
                print(f'scrape {db.db_summarise_row(row.discogs_id)}')
                scrape_row(discogs_client=discogs_client, discogs_id=row.discogs_id)
