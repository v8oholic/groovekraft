#!/usr/bin/env python3

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import pandas as pd
import sys
import signal
from dateutil import parser
import datetime


def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)


def init_driver():

    # Set up headless Chrome options
    chrome_options = Options()

    chrome_options.add_argument("--headless=new")  # Improved headless support
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration (macOS-specific issue)
    chrome_options.add_argument("--no-sandbox")  # Bypass sandbox issues
    chrome_options.add_argument("--disable-dev-shm-usage")  # Avoid memory overflow errors
    chrome_options.add_argument("--window-size=1280,1024")  # Ensure screen dimensions
    chrome_options.add_argument("--enable-websocket-over-http2")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.165 Safari/537.36")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    # chrome_options.add_argument("--disable-software-rasterizer")  # Force CPU rasterizer
    # chrome_options.add_argument("--enable-logging")  # Enable verbose logs
    # chrome_options.add_argument("--log-level=0")  # Max log level for diagnostics

    # Force installation of the closest compatible driver
    service = Service(ChromeDriverManager(driver_version="134.0.6998.165").install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    return driver


def scrape_table(url):

    try:
        driver = init_driver()

        # Open the URL using Selenium
        driver.get(url)

        # Wait for page to load (adjust the timeout as needed)
        driver.implicitly_wait(10)  # Wait for up to 10 seconds

        x = driver.find_element(By.CLASS_NAME, "info_LD8Ql")

        # Find the table
        # table = driver.find_element(By.TAG_NAME, 'table_c5ftk')
        table = driver.find_element(By.CLASS_NAME, 'table_c5ftk')
        if not table:
            print(f"No table found at {url}")
            return None

        # Extract headers
        headers = table.find_elements(By.TAG_NAME, 'th')
        table_headers = [header.text.strip() for header in headers]

        # Extract rows
        rows = []
        rows_elements = table.find_elements(By.TAG_NAME, 'tr')  # [1:]  # Skip header row
        for row in rows_elements:
            cells = row.find_elements(By.TAG_NAME, 'td')
            row_data = [cell.text.strip() for cell in cells]
            if row_data:
                rows.append(row_data[0])

        # Create DataFrame
        return pd.DataFrame([rows], columns=table_headers) if table_headers else pd.DataFrame(rows)

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None
    finally:
        driver.quit()  # Close the browser session after scraping


# List of URLs to scrape
urls = [
    'https://www.discogs.com/release/177711-Snap-Rhythm-Is-A-Dancer',
    'https://www.discogs.com/master/67913-Snap-Rhythm-Is-A-Dancer'
]


def main():

    # Loop through URLs and scrape tables

    release_url = 'https://www.discogs.com/release/24393-Depeche-Mode-Just-Cant-Get-Enough-Schizo-Mix'
    master_url = 'https://www.discogs.com/master/17710-Depeche-Mode-Just-Cant-Get-Enough'

    release_date_object = None
    master_date_object = None

    # scrape the release first
    print(release_url.split('/')[-1])
    df = scrape_table(release_url)
    if df is not None:
        # Save to CSV if needed
        df.to_csv(f"output_{release_url.split('/')[-1]}.csv", index=False)

        try:
            release_date_str = df.at[0, 'Released:']
            release_datetime_object = parser.parse(release_date_str)
            release_date_object = release_datetime_object.date()
        except Exception as e:
            print(f"No release date on release")
            release_date_object = None
        finally:
            if release_date_object:
                print(f'release date: {release_date_object.strftime('%A %d %B %Y')}')

    # scrape the master release next
    print(master_url.split('/')[-1])
    df = scrape_table(master_url)
    if df is not None:
        # Save to CSV if needed
        df.to_csv(f"output_{master_url.split('/')[-1]}.csv", index=False)

        try:
            master_date_str = df.at[0, 'Released:']
            master_datetime_object = parser.parse(master_date_str)
            master_date_object = master_datetime_object.date()
        except Exception as e:
            print(f"no release date found on master release")
            master_date_object = None
        finally:
            if master_date_object:
                print(f'master release date: {master_date_object.strftime('%A %d %B %Y')}')


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    main()
