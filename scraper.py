from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import pandas as pd

# Set up headless Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run headless (without UI)
chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration (optional)
chrome_options.add_argument("--no-sandbox")  # Avoid issues on some systems
chrome_options.add_argument("--disable-software-rasterizer")  # Disable software rasterizer
chrome_options.add_argument("--remote-debugging-port=9222")  # Open debugging port (optional)
# Disable /dev/shm usage to reduce memory consumption
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1280x1024")  # Set a fixed window size for headless mode

# Create the service object for ChromeDriver
service = Service(ChromeDriverManager().install())

# Initialize the WebDriver with the correct service and options
driver = webdriver.Chrome(service=service, options=chrome_options)


def scrape_table(url):
    try:
        # Open the URL using Selenium
        driver.get(url)

        # Wait for page to load (adjust the timeout as needed)
        driver.implicitly_wait(10)  # Wait for up to 10 seconds

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
                rows.append(row_data)

        # find the release date, if present
        for i in range(len(table_headers)):
            if table_headers[i] == 'Released:':
                release_date = rows[i]
                print(f'release date: {release_date}')

        # Create DataFrame
        return pd.DataFrame(rows, columns=table_headers) if table_headers else pd.DataFrame(rows)

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

# Loop through URLs and scrape tables
for url in urls:
    df = scrape_table(url)
    if df is not None:
        print(f"Data from {url}:")
        print(df.head())
        # Save to CSV if needed
        df.to_csv(f"output_{url.split('/')[-1]}.csv", index=False)
