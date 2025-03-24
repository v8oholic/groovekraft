#!/usr/bin/env python3

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import pandas as pd
import sys
import signal


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
        print(table_headers)

        # Extract rows
        rows = []
        rows_elements = table.find_elements(By.TAG_NAME, 'tr')  # [1:]  # Skip header row
        for row in rows_elements:
            cells = row.find_elements(By.TAG_NAME, 'td')
            row_data = [cell.text.strip() for cell in cells]
            if row_data:
                rows.append(row_data[0])

        # find the release date, if present
        for i in range(len(table_headers)):
            if table_headers[i] == 'Released:':
                release_date = rows[i]
                print(f'release date: {release_date}')

        print(rows)

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
    for url in urls:
        df = scrape_table(url)
        if df is not None:
            print(f"Data from {url}:")
            print(df.head())
            # Save to CSV if needed
            df.to_csv(f"output_{url.split('/')[-1]}.csv", index=False)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    main()
