#!/usr/bin/env python3

# CLI entry point

import discogs_client
from discogs_client.exceptions import HTTPError
import musicbrainzngs
from musicbrainzngs import musicbrainz
import sys
import argparse
import signal
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import pandas as pd
from dateutil import parser
import dateparser
import logging
import configparser

from modules import db_discogs
from modules.db_discogs import set_release_date
from modules.db import db_ops, fetch_row_by_discogs_id, fetch_discogs_release_rows
from modules.db import initialize_db
from modules.db import db_summarise_row
from modules.discogs_importer import import_from_discogs
from modules.discogs_importer import connect_to_discogs
from modules.mb_matcher import match_discogs_against_mb
from modules.db_discogs import fetch_row, set_release_date
from modules.config import AppConfig
from modules.utils import earliest_date
from modules.utils import parse_and_humanize_date
from modules.utils import humanize_date_delta
from modules.utils import is_today_anniversary


logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)


def scrape_row(discogs_client, discogs_id=0):

    row = db_discogs.fetch_row(discogs_id)

    discogs_release = discogs_client.release(discogs_id)

    release_url = discogs_release.url

    # scrape the release first
    logger.debug(release_url.split('/')[-1])
    df = scrape_table(release_url)
    if df is not None:
        # Save to CSV if needed
        # df.to_csv(f"output_{release_url.split('/')[-1]}.csv", index=False)

        try:
            release_date_str = df.at[0, 'Released:']
            if release_date_str:
                # attempt day, month and year match first
                settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first',
                            'DATE_ORDER': 'DMY', 'PREFER_LOCALE_DATE_ORDER': False, 'REQUIRE_PARTS': ['day', 'month', 'year']}

                date_object = dateparser.parse(release_date_str, settings=settings)

                if date_object:
                    release_date_str = date_object.strftime("%Y-%m-%d")

                else:
                    # attempt month and year match
                    settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first',
                                'DATE_ORDER': 'DMY', 'PREFER_LOCALE_DATE_ORDER': False, 'REQUIRE_PARTS': ['month', 'year']}
                    date_object = dateparser.parse(release_date_str, settings=settings)

                    if date_object:
                        release_date_str = date_object.strftime("%Y-%m")

                    else:
                        # attempt year match
                        settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first',
                                    'DATE_ORDER': 'DMY', 'PREFER_LOCALE_DATE_ORDER': False, 'REQUIRE_PARTS': ['year']}
                        date_object = dateparser.parse(release_date_str, settings=settings)

                        if date_object:
                            release_date_str = date_object.strftime("%Y")
                        else:
                            release_date_str = ''

        except Exception as e:
            logger.warning(f"No release date on release")
            release_date_str = ''
        finally:

            # only use the date if it's earlier than the existing one
            release_date = earliest_date(row.release_date, release_date_str)
            db_discogs.set_release_date(row.discogs_id, release_date)


def scrape_discogs(config):

    discogs_client, discogs_access_token, discogs_access_secret = connect_to_discogs(config)

    with db_ops() as cur:

        cur.execute("""
            SELECT *
            FROM discogs_releases
            ORDER BY sort_name, discogs_id
        """)

        rows = cur.fetchall()

        print(f'{len(rows)} rows')

        for idx, row in enumerate(rows):
            print(f'scrape {db_summarise_row(row.discogs_id)}')
            scrape_row(discogs_client=discogs_client, discogs_id=row.discogs_id)


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
            logger.warning(f"No table found at {url}")
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
        logger.error(f"Error scraping {url}: {e}")
        return None
    finally:
        driver.quit()  # Close the browser session after scraping


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


if False:
    # With an active auth token, we're able to reuse the client object and request
    # additional discogs authenticated endpoints, such as database search.
    search_results = discogs_client.search(
        "House For All", type="release", artist="Blunted Dummies"
    )

    print("\n== Search results for release_title=House For All ==")
    for release in search_results:
        print(f"\n\t== discogs-id {release.id} ==")
        print(f'\tArtist\t: {", ".join(artist.name for artist in release.artists)}')
        print(f"\tTitle\t: {release.title}")
        print(f"\tYear\t: {release.year}")
        print(f'\tLabels\t: {", ".join(label.name for label in release.labels)}')

    # You can reach into the Fetcher lib if you wish to used the wrapped requests
    # library to download an image. The following example demonstrates this.
    image = search_results[0].images[0]["uri"]
    content, resp = discogs_client._fetcher.fetch(
        None, "GET", image, headers={"User-agent": discogs_client.user_agent}
    )

    print(" == API image request ==")
    print(f"    * response status      = {resp}")
    print(f'    * saving image to disk = {image.split("/")[-1]}')

    with open(image.split("/")[-1], "wb") as fh:
        fh.write(content)

# x = dir(discogs_client.release('9459125'))
# print(x)


def fls(data_str, length):
    if len(data_str) > length:
        return data_str[:length-3]+'...'
    else:
        return data_str.ljust(length)


def match(config=None):

    set_date = None

    with db_ops() as cur:

        cur.execute(f"""
            SELECT *
            FROM discogs_releases
            WHERE artist LIKE '%{config.match}%'
            OR title LIKE '%{config.match}%'
            OR sort_name LIKE '%{config.match}%'
            ORDER BY artist, release_date, title, discogs_id
        """)

        rows = cur.fetchall()
        if len(rows) == 0:
            print('no matching items')
            return

        if config.release_date:

            if len(config.release_date) > 10:
                # try day, month and year match first
                formats = ['%-d %b %Y']
                settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first',
                            'DATE_ORDER': 'DMY', 'PREFER_LOCALE_DATE_ORDER': False, 'REQUIRE_PARTS': ['day', 'month', 'year']}
                date_object = dateparser.parse(
                    config.release_date, date_formats=formats, settings=settings)

                if date_object:
                    set_date = date_object.strftime('%Y-%m-%d')

            elif len(config.release_date) == 10:
                # try day, month and year match first
                formats = ["%Y-%m-%d", '%-d %B %Y']
                settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first',
                            'DATE_ORDER': 'DMY', 'PREFER_LOCALE_DATE_ORDER': False, 'REQUIRE_PARTS': ['day', 'month', 'year']}
                date_object = dateparser.parse(
                    config.release_date, date_formats=formats, settings=settings)

                if date_object:
                    set_date = date_object.strftime('%Y-%m-%d')

            if len(config.release_date) == 8:
                # try month and year
                formats = ["'b %Y"]
                settings = {'PREFER_DAY_OF_MONTH': 'first', 'REQUIRE_PARTS': ['month', 'year']}
                # settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first',
                #             'DATE_ORDER': 'DMY', 'PREFER_LOCALE_DATE_ORDER': False, 'REQUIRE_PARTS': ['month', 'year']}

                date_object = dateparser.parse(
                    config.release_date, date_formats=formats, settings=settings)

                if date_object:
                    set_date = date_object.strftime("%Y-%m")

            if len(config.release_date) == 7:
                # try month and year
                formats = ["%Y-%m"]
                settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first',
                            'DATE_ORDER': 'DMY', 'PREFER_LOCALE_DATE_ORDER': False, 'REQUIRE_PARTS': ['month', 'year']}

                date_object = dateparser.parse(
                    config.release_date, date_formats=formats, settings=settings)

                if date_object:
                    set_date = date_object.strftime("%Y-%m")

            if len(config.release_date) == 4:
                # try for just a year
                formats = ["%Y"]
                settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first',
                            'DATE_ORDER': 'DMY', 'PREFER_LOCALE_DATE_ORDER': False, 'REQUIRE_PARTS': ['year']}

                date_object = dateparser.parse(
                    config.release_date, date_formats=formats, settings=settings)

                if date_object:
                    set_date = date_object.strftime("%Y")

            if not set_date:
                print(f'invalid date {config.release_date}')
                return

        if set_date:
            if set_date and len(rows) > 1:
                print('more than one item matched, release date not set')
                return
            else:
                cur.execute(f"""
                    SELECT *
                    FROM discogs_releases
                    WHERE artist LIKE '%{config.match}%'
                    OR title LIKE '%{config.match}%'
                    OR sort_name LIKE '%{config.match}%'
                    ORDER BY artist, release_date, title, discogs_id
                """)

                row = cur.fetchone()

                set_release_date(row.discogs_id, set_date, row.release_date, config)
                return

        max_title_len = 45

        artist_len = 0
        title_len = 0
        format_len = 0
        release_date_len = 0

        for p in range(2):
            if p == 0:
                artist_len = 0
                title_len = 0
                format_len = 0
                release_date_len = 0

                last_artist = None
            else:
                title_len = min(title_len, max_title_len)

            for row in rows:

                artist = row.artist
                title = row.title

                format = row.format if row.format else ''
                release_date = row.release_date if row.release_date else ''

                if config.release_date:
                    delta = humanize_date_delta(row.release_date)

                if p == 0:
                    artist_len = max(artist_len, len(artist))
                    title_len = max(title_len, len(title))

                    format_len = max(format_len, len(format))
                    release_date_len = max(release_date_len, len(release_date))
                else:
                    # logging.debug(
                    #     f"{row.release_id:>8} {artist:20} {title:20} {format:10} {release_date:10}")
                    # logging.debug(
                    #     f'{row.release_id:>8} {fls(artist, artist_len)} {fls(title, title_len)} {fls(format, format_len)} {fls(release_date, release_date_len)}')
                    if artist != last_artist:
                        last_artist = artist
                        print(artist)
                        print('='*len(artist))

                    print(
                        f'{row.discogs_id:>8} {fls(title, title_len)} {fls(format, format_len)} {fls(release_date, release_date_len)}')


def status(config):

    def output_nvp(label, value):
        print(f'{fls(label, 45)}: {value}')

    discogs_client, discogs_access_token, discogs_access_secret = connect_to_discogs(config)

    # fetch the identity object for the current logged in user.
    discogs_user = discogs_client.identity()

    output_nvp("Discogs username", discogs_user.username)
    output_nvp("MusicBrainz username", config.username)

    folder = discogs_user.collection_folders[0]

    output_nvp('releases on Discogs', len(folder.releases))

    with db_ops() as cur:

        cur.execute("""
            SELECT COUNT(*) as count
            FROM discogs_releases
        """)
        row = cur.fetchone()

        output_nvp('releasees in local database', row.count)

        cur.execute("""
            SELECT COUNT(*) as count
            FROM mb_matches
            WHERE mbid IS NOT NULL
        """)
        row = cur.fetchone()

        output_nvp('releases matched in MusicBrainz', row.count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM discogs_releases
            WHERE release_date IS NULL
        """)
        row = cur.fetchone()

        output_nvp('releases with no release date', row.count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM discogs_releases
            WHERE LENGTH(release_date) = 4
        """)
        row = cur.fetchone()

        output_nvp('releases with just release year', row.count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM discogs_releases
            WHERE LENGTH(release_date) = 7
        """)
        row = cur.fetchone()

        output_nvp('releases with just release month and year', row.count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM discogs_releases
            WHERE LENGTH(release_date) = 10
        """)
        row = cur.fetchone()

        output_nvp('releases with full release date', row.count)


def random_selection():

    with db_ops() as cur:

        cur.execute("""
            SELECT *
            FROM discogs_releases
            ORDER BY RANDOM()
            LIMIT 1
        """)

        row = cur.fetchone()

        print()
        print('random selection:')
        print()
        output_row(row)
        print()


def output_row(row):
    if row.release_date:
        delta = humanize_date_delta(row.release_date)

    print(f'released        : {delta}')
    print(f'artist          : {row.artist}')
    print(f'title           : {row.title}')
    print(f'format          : {row.format}')
    print(f'discogs_id      : {row.discogs_id}')
    if row.release_date:
        # print(f'release_date    : {row.release_date}')
        print(f'release_date    : {parse_and_humanize_date(row.release_date)}')


def on_this_day(today_str='', config=None):

    with db_ops() as cur:

        cur.execute("""
            SELECT *
            FROM discogs_releases
            WHERE release_date IS NOT NULL
            ORDER BY artist, release_date, title, discogs_id
        """)

        rows = cur.fetchall()

        # print(f'{len(rows)} rows')

        for row in rows:

            if row.release_date and len(row.release_date) == 10:
                if today_str:
                    if not is_today_anniversary(row.release_date, today_str):
                        continue

                else:
                    if not is_today_anniversary(row.release_date):
                        continue

                print()
                output_row(row)
                print()


def import_old_release_dates(config=None):

    rows = fetch_discogs_release_rows()

    for index, row in enumerate(rows):

        print(f'⚙️ {index+1}/{len(rows)} {db_summarise_row(row.discogs_id)}')

        # fetch the old row
        old_item = fetch_row_by_discogs_id(row.discogs_id)
        if old_item:
            # release_date = earliest_date(old_item.release_date, row.release_date)
            # set_release_date(row.discogs_id, release_date)

            set_release_date(row.discogs_id, old_item.release_date, True)


def main(config):

    initialize_db()

    # discogs_id = 5084926
    # update_table(discogs_id=2635834)
    # return

    if args.import_items:
        import_from_discogs(config=config)
        match_discogs_against_mb(config=config)
        import_old_release_dates(config=config)

    elif args.update_items:
        match_discogs_against_mb(config=config)

    elif args.scrape:
        # scrape_discogs(config)
        import_old_release_dates(config=config)

    elif args.match:
        match(config=config)

    elif args.random:
        random_selection()

    elif args.onthisday:
        on_this_day(config=config)

    elif args.status:
        status(config=config)


if __name__ == "__main__":

    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(
        description="Music Collection Importer",
        epilog="Import from Discogs skips previously imported releases. Update from MusicBrainz processes only previously imported releases."
    )

    # autopep8: off
    parser.add_argument('--init', required=False, action='store_true', help='initialise database')

    main_group = parser.add_mutually_exclusive_group(required=False)
    main_group.add_argument('--import', required=False, action='store_true', dest='import_items', help='import items from Discogs')
    main_group.add_argument('--update', required=False, action='store_true', dest='update_items', help='update items using MusicBrainz')
    main_group.add_argument('--scrape', required=False, action='store_true', help='update release dates from Discogs website')
    main_group.add_argument('--onthisday', '--on-this-day', required=False, action='store_true', help='display any release anniversaries')
    main_group.add_argument('--random', required=False, action='store_true', help='generate random selection')
    main_group.add_argument('--match', required=False, help='find matching text in database')
    main_group.add_argument('--status', required=False, action='store_true', help='report status of database')

    # scope_group = parser.add_mutually_exclusive_group(required=False)
    # scope_group.add_argument('--new', required=False, action='store_true', dest="new_items", help='new items')
    # scope_group.add_argument('--all', required=False, action='store_true', dest="all_items", help='all items')
    # scope_group.add_argument('--discogs_id', required=False, help='restrict init or update to a specific Discogs id')
    # scope_group.add_argument('--mbid', required=False, help='restrict init or update to a specific MusicBrainz id')

    parser.add_argument('--date', required=False, dest='release_date', help='set release date')

    id_group = parser.add_mutually_exclusive_group(required=False)
    id_group.add_argument('--reset', required=False, action='store_true', help='reset version number to force full update')

    parser.add_argument('--dry-run', '--read-only', required=False, action="store_true", help='dry run to test filtering')
    parser.add_argument("--database", type=str, help="Path to the SQLite database file")
    parser.add_argument('--verbose', required=False, action='store_true', help='verbose messages')
    # autopep8: on

    args = parser.parse_args()

    config_parser = configparser.ConfigParser()
    config_parser.read("discogs.ini")

    config = AppConfig(args)
    config.load_from_config_parser(config_parser)

    # args2 = vars(args)
    # print(args2)

    main(config)
