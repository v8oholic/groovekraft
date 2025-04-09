#!/usr/bin/env python3

# CLI entry point

import discogs_client
from discogs_client.exceptions import HTTPError
import musicbrainzngs
from musicbrainzngs import musicbrainz
import sys
import argparse
import sqlite3
import signal
from collections import namedtuple
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import pandas as pd
from dateutil import parser
import datetime
import dateparser
import dateutil
from contextlib import contextmanager
import json
import logging
from modules.utils import normalize_country_name

USER_AGENT = 'v8oholic_collection_application/1.0'

DISCOGS_CONSUMER_KEY = "yEJrrZEZrExGHEPjNQca"
DISCOGS_CONSUMER_SECRET = "isFjruJTfmmXFXiaywRqCUSkIGwHlHKn"

MUSICBRAINZ_USERNAME = 'v8oholic'
MUSICBRAINZ_PASSWORD = 'copdEs-3mezto-horvox'

NAMED_TUPLES = True
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DATABASE_NAME = 'discogs.db'

# Discogs -> MusicBrainz mapping

SCORE_WEIGHTS = {
    "artist": 2,
    "title": 2,
    "country": 2,
    "format": 2,
    "barcode": 2,
    "no_barcode": 1,
    "catno": 2,
    "no_catno": 1,
}

MAXIMUM_SCORE = 12


with open("config/countries.json", "r", encoding="utf-8") as f:
    COUNTRIES = json.load(f)

with open("config/mediatypes.json", "r", encoding="utf-8") as f:
    MEDIATYPES = json.load(f)


def signal_handler(sig, frame):
    logger.info('You pressed Ctrl+C!')
    sys.exit(0)


@contextmanager
def db_ops(db_name):
    """Wrapper to take care of committing and closing a database

    See https://stackoverflow.com/questions/67436362/decorator-for-sqlite3/67436763
    """
    global args

    if args.dry_run:
        conn = sqlite3.connect(f'file:{db_name}?mode=ro', uri=True)
    else:
        conn = sqlite3.connect(db_name)

    if NAMED_TUPLES:
        conn.row_factory = namedtuple_factory
    else:
        conn.row_factory = sqlite3.Row

    try:
        cur = conn.cursor()
        yield cur
    except Exception as e:
        # do something with exception
        conn.rollback()
        raise e
    else:
        conn.commit()
    finally:
        conn.close()


def disambiguator_score(discogs_release=None, discogs_id=None, mb_release=None, mb_id=None, discogs_client=None):
    """Score a MusicBrainz release against a Discogs release

    Returns a rounded percentage score, with 100 being a perfect match."""

    if not discogs_id and discogs_release:
        discogs_id = discogs_release.id

    if not discogs_release and discogs_id:
        discogs_release = discogs_client.release(discogs_id)

    load_mbid = None

    if mb_release:
        if not all([
            mb_release.get('label-info-count'),
            mb_release.get('artist-credit-phrase'),
            mb_release.get('medium-list')
        ]):
            load_mbid = mb_release.get('id')
    else:
        load_mbid = mb_id

    if mb_release is None and load_mbid == 0:
        raise Exception("Unable to load MusicBrainz release")

    if load_mbid:
        mb_release = musicbrainzngs.get_release_by_id(
            load_mbid, includes=['artists', 'artist-credits', 'labels', 'media'])
        if mb_release:
            mb_release = mb_release.get('release')
        else:
            raise Exception("Unable to load MusicBrainz release")

    artist_score = 0
    title_score = 0
    country_score = 0
    format_score = 0
    barcode_score = 0
    catno_score = 0

    artist = trim_if_ends_with_number_in_brackets(discogs_release.artists[0].name)
    if artist == 'Various':
        artist = 'Various Artists'
    artist = artist.casefold()
    if artist == mb_release.get('artist-credit-phrase').casefold():
        artist_score = SCORE_WEIGHTS['artist']

    title = discogs_release.title.casefold()
    if title == mb_release.get('title').casefold():
        artist_score = SCORE_WEIGHTS['title']

    country = COUNTRIES.get(normalize_country_name(discogs_release.country))
    if not country:
        logger.warning(f"⚠️ Unknown country mapping for: {discogs_release.country}")
    elif country == mb_release.get('country'):
        country_score = SCORE_WEIGHTS['country']

    _, _, mb_format = convert_format(discogs_release.formats[0])
    media = mb_release.get('medium-list')
    if media and len(media):
        first_media = media[0]
        format = first_media.get('format')
        if format in ['Enhanced CD', 'Copy Control CD', 'HDCD']:
            format = 'CD'
        if format == mb_format:
            format_score = SCORE_WEIGHTS['format']

    barcodes = []
    discogs_identifiers = discogs_release.fetch('identifiers')
    for identifier in discogs_identifiers:
        if identifier['type'] == 'Barcode':
            barcodes.append(sanitise_identifier(identifier['value']))
    barcodes = list(set(barcodes))
    release_barcode = mb_release.get('barcode')
    if barcodes and release_barcode:
        release_barcode = sanitise_identifier(release_barcode)
        for barcode in barcodes:
            if barcode == release_barcode:
                barcode_score = SCORE_WEIGHTS['barcode']
                break
    else:
        # if barcode is missing from either or both, assume still possible match
        barcode_score = SCORE_WEIGHTS['no_barcode']

    catnos = list(set([sanitise_identifier(x.data['catno']) for x in discogs_release.labels]))
    mb_catnos = []
    for label_info in mb_release.get('label-info-list'):
        find_catno = label_info.get('catalog-number')
        mb_catnos.append(find_catno)
    mb_catnos = list(set(mb_catnos))
    if catnos and mb_catnos:
        for catno in catnos:
            for label_info in mb_release.get('label-info-list'):
                find_catno = label_info.get('catalog-number')
                if find_catno and catno == sanitise_identifier(find_catno):
                    catno_score = SCORE_WEIGHTS['catno']
                    break
    else:
        catno_score = SCORE_WEIGHTS['no_catno']

    total_points = artist_score+title_score+country_score+format_score+catno_score+barcode_score

    return round(100 * total_points/MAXIMUM_SCORE)


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


def get_earliest_release_date(artist, title):
    try:
        # Search for the recording by artist and title
        result = musicbrainzngs.search_recordings(artist=artist, recording=title, limit=10)

        if not result['recording-list']:
            return f"No recordings found for {artist} - {title}"

        earliest_date = None

        # Iterate through recordings
        for recording in result['recording-list']:
            # Check for associated releases
            if 'release-list' in recording:
                for release in recording['release-list']:
                    # Get the release date if available
                    date = release.get('date')
                    if date:
                        # Compare and store the earliest date
                        if not earliest_date or date < earliest_date:
                            earliest_date = date

        return earliest_date if earliest_date else "No release date available"

    except musicbrainzngs.WebServiceError as e:
        return f"Error: {e}"


def get_earliest_release_date2(artist, title):
    try:
        # Search for the recording (limit increased for better matching)
        result = musicbrainzngs.search_recordings(artist=artist, recording=title, limit=50)

        if not result['recording-list']:
            return f"No recordings found for {artist} - {title}"

        title_words = set(title.lower().split())
        earliest_date = None

        # Iterate through recordings
        for recording in result['recording-list']:
            # Ensure both artist and title match strictly
            if 'release-list' in recording:
                # Check for strict title match by ensuring all title words are present
                recording_title = recording['title'].lower()
                if not title_words.issubset(recording_title.split()):
                    continue

                # Ensure artist name appears in one of the associated artist credits
                if not any(artist.lower() in a['artist']['name'].lower() for a in recording['artist-credit']):
                    continue

                # Compare release dates
                for release in recording['release-list']:
                    date = release.get('date')
                    if date and (not earliest_date or date < earliest_date):
                        earliest_date = date

        return earliest_date if earliest_date else "No release date available"

    except musicbrainzngs.WebServiceError as e:
        return f"Error: {e}"


def trim_if_ends_with_number_in_brackets(s):
    pattern = r' \(([1-9]\d*)\)$'
    return re.sub(pattern, '', s)
    # return bool(re.search(pattern, s))


def humanize_date_delta(dt1, dt2=datetime.datetime.today().date()):
    date_formats = ['%Y-%m-%d', '%Y-%m', '%Y']
    settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first'}

    dt2 = datetime.date.today().strftime('%Y-%m-%d')
    dt2_object = dateparser.parse(dt2, date_formats=date_formats, settings=settings)
    dt1_object = dateparser.parse(dt1, date_formats=date_formats, settings=settings)

    rd = dateutil.relativedelta.relativedelta(dt2_object, dt1_object)

    if len(dt1) == 4:
        # just the year
        x = []

        if rd.years == 1:
            x.append(f'{rd.years} year')
        elif rd.years > 1:
            x.append(f'{rd.years} years')

        xl = len(x)
        if xl == 1:
            xd = x[0]
        else:
            x2 = x.pop()
            xd = ', '.join(x)
            xd += ' and ' + x2

        xd += ' ago this year'

    elif len(dt1) == 7:
        # just the year and month

        x = []

        if rd.years == 1:
            x.append(f'{rd.years} year')
        elif rd.years > 1:
            x.append(f'{rd.years} years')

        if rd.months == 1:
            x.append(f'{rd.months} month')
        elif rd.months > 1:
            x.append(f'{rd.months} months')

        xl = len(x)
        if xl == 1:
            xd = x[0]
        else:
            x2 = x.pop()
            xd = ', '.join(x)
            xd += ' and ' + x2

        xd += ' ago this month'

    else:

        x = []

        if rd.years == 1:
            x.append(f'{rd.years} year')
        elif rd.years > 1:
            x.append(f'{rd.years} years')

        if rd.months == 1:
            x.append(f'{rd.months} month')
        elif rd.months > 1:
            x.append(f'{rd.months} months')

        if rd.days == 1:
            x.append(f'{rd.days} day')
        elif rd.days > 1:
            x.append(f'{rd.days} days')

        xl = len(x)
        if xl == 1:
            xd = x[0]
        else:
            x2 = x.pop()
            xd = ', '.join(x)
            xd += ' and ' + x2

        xd += ' ago today'

    return xd


def is_today_anniversary(date_str):
    # Parse the input date (expects format YYYY-MM-DD)
    dt1 = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    dt2 = datetime.datetime.today().date()

    # Compare month and day (ignore year)
    return dt1.month == dt2.month and dt1.day == dt2.day


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


def namedtuple_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    cls = namedtuple("Row", fields)
    return cls._make(row)


def connect_to_discogs(oauth_token=None, ouath_token_secret=None):
    authenticated = False

    if oauth_token and ouath_token_secret:
        try:
            client = discogs_client.Client(
                USER_AGENT,
                consumer_key=DISCOGS_CONSUMER_KEY,
                consumer_secret=DISCOGS_CONSUMER_SECRET,
                token=oauth_token,
                secret=ouath_token_secret
            )
            access_token = oauth_token
            access_secret = ouath_token_secret
            authenticated = True

        except HTTPError:
            logger.error("Unable to authenticate.")
            access_token = None
            access_secret = None

    if not authenticated:

        # instantiate discogs_client object
        client = discogs_client.Client(USER_AGENT)

        # prepare the client with our API consumer data
        client.set_consumer_key(DISCOGS_CONSUMER_KEY, DISCOGS_CONSUMER_SECRET)
        token, secret, url = client.get_authorize_url()

        logger.debug(" == Request Token == ")
        logger.debug(f"    * oauth_token        = {token}")
        logger.debug(f"    * oauth_token_secret = {secret}")

        # visit the URL in auth_url to allow the app to connect

        print(f"Please browse to the following URL {url}")

        accepted = "n"
        while accepted.lower() == "n":
            print()
            accepted = input(f"Have you authorized me at {url} [y/n] :")

        # note the token

        # Waiting for user input. Here they must enter the verifier key that was
        # provided at the unqiue URL generated above.
        oauth_verifier = input("Verification code : ")

        try:
            access_token, access_secret = client.get_access_token(oauth_verifier)
            authenticated = True

        except HTTPError:
            print("Unable to authenticate to Discogs.")
            sys.exit(1)

    return client, access_token, access_secret


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


def db_update_artist(release_id, new_value, old_value):
    global args

    if old_value == new_value:
        return

    if args.dry_run:
        logger.warning(row_ignore_change(release_id, 'artist', new_value, old_value, 'read-only'))
        return

    logger.debug(row_change(release_id, 'artist', new_value, old_value))
    with db_ops(DATABASE_NAME) as cur:
        cur.execute('UPDATE items SET artist = ? WHERE release_id = ?', (new_value, release_id))


def db_update_title(release_id, new_value, old_value):
    global args

    if old_value == new_value:
        return

    if args.dry_run:
        logger.warning(row_ignore_change(release_id, 'artist', new_value, old_value, 'read-only'))
        return

    logger.debug(row_change(release_id, 'title', new_value, old_value))
    with db_ops(DATABASE_NAME) as cur:
        cur.execute('UPDATE items SET title = ? WHERE release_id = ?', (new_value, release_id))


def db_update_mb_id(release_id, new_value, old_value):
    global args

    if old_value == new_value:
        return

    if args.dry_run:
        logger.warning(row_ignore_change(release_id, 'mb_id', new_value, old_value, 'read-only'))
        return

    logger.debug(row_change(release_id, 'mb_id', new_value, old_value))
    with db_ops(DATABASE_NAME) as cur:
        cur.execute('UPDATE items SET mb_id = ? WHERE release_id = ?', (new_value, release_id))


def db_update_mb_artist(release_id, new_value, old_value):
    global args

    if old_value == new_value:
        return

    if args.dry_run:
        logger.warning(row_ignore_change(release_id, 'mb_artist',
                       new_value, old_value, 'read-only'))
        return

    logger.debug(row_change(release_id, 'mb_artist', new_value, old_value))
    with db_ops(DATABASE_NAME) as cur:
        cur.execute('UPDATE items SET mb_artist = ? WHERE release_id = ?', (new_value, release_id))


def db_update_mb_title(release_id, new_value, old_value):
    global args

    if old_value == new_value:
        return

    if args.dry_run:
        logger.warning(row_change(release_id, 'mb_title', new_value, old_value, 'read-only'))
        return

    logger.debug(row_change(release_id, 'mb_title', new_value, old_value))
    with db_ops(DATABASE_NAME) as cur:
        cur.execute('UPDATE items SET mb_title = ? WHERE release_id = ?', (new_value, release_id))


def db_update_sort_name(release_id, new_value, old_value):
    global args

    if old_value == new_value:
        return

    if args.dry_run:
        logger.warning(row_change(release_id, 'sort_name', new_value, old_value, 'read-only'))
        return

    logger.debug(row_change(release_id, 'sort_name', new_value, old_value))
    with db_ops(DATABASE_NAME) as cur:
        cur.execute('UPDATE items SET sort_name = ? WHERE release_id = ?', (new_value, release_id))


def db_update_release_date(release_id, new_value, old_value):
    global args

    if old_value == new_value:
        return

    if args.dry_run:
        logger.warning(row_change(release_id, 'release_date', new_value, old_value, 'read-only'))
        return

    if old_value is not None and len(new_value) < len(old_value):
        logger.warning(row_change(release_id, 'release_date',
                       new_value, old_value, "ignored shorter"))
        return

    if old_value is not None and old_value < new_value:
        logger.warning(row_change(release_id, 'release_date',
                       new_value, old_value, "ignored newer"))
        return

    logger.debug(row_change(release_id, 'release_date', new_value, old_value))
    with db_ops(DATABASE_NAME) as cur:
        cur.execute('UPDATE items SET release_date = ? WHERE release_id = ?',
                    (new_value, release_id))


def db_update_country(release_id, new_value, old_value):
    global args

    if old_value == new_value:
        return

    if args.dry_run:
        logger.warning(row_change(release_id, 'country', new_value, old_value, 'read-only'))
        return

    logger.debug(row_change(release_id, 'country', new_value, old_value))
    with db_ops(DATABASE_NAME) as cur:
        cur.execute('UPDATE items SET country = ? WHERE release_id = ?', (new_value, release_id))


def db_update_format(release_id, new_value, old_value):
    global args

    if old_value == new_value:
        return

    if args.dry_run:
        logger.warning(row_change(release_id, 'format', new_value, old_value, 'read-only'))
        return

    logger.debug(row_change(release_id, 'format', new_value, old_value))
    with db_ops(DATABASE_NAME) as cur:
        cur.execute('UPDATE items SET format = ? WHERE release_id = ?', (new_value, release_id))


def db_update_mb_primary_type(release_id, new_value, old_value):
    global args

    if old_value == new_value:
        return

    if args.dry_run:
        logger.warning(row_change(release_id, 'mb_primary_type', new_value, old_value, 'read-only'))
        return

    logger.debug(row_change(release_id, 'mb_primary_type', new_value, old_value))
    with db_ops(DATABASE_NAME) as cur:
        cur.execute('UPDATE items SET mb_primary_type = ? WHERE release_id = ?',
                    (new_value, release_id))


def db_update_version_id(release_id, new_value, old_value):
    global args

    if old_value == new_value:
        return

    if args.dry_run:
        logger.warning(row_change(release_id, 'version_id', new_value, old_value, 'read-only'))
        return

    logger.debug(row_change(release_id, 'version_id', new_value, old_value))
    with db_ops(DATABASE_NAME) as cur:
        cur.execute('UPDATE items SET version_id = ? WHERE release_id = ?',
                    (new_value, release_id))


def db_fetch_row_by_discogs_id(discogs_id):

    with db_ops(DATABASE_NAME) as cur:
        cur.execute('SELECT * FROM items WHERE release_id = ?', (int(discogs_id),))
        row = cur.fetchone()

    return row


def db_insert_row(artist=None, title=None, format=None, mb_primary_type=None, release_date=None, release_id=0, country=None, mb_id=None, mb_artist=None, mb_title=None, sort_name=None, version_id=0):
    global args

    if args.dry_run:
        logger.warning('dry run - not inserted')
        return

    with db_ops(DATABASE_NAME) as cur:
        cur.execute("""
            INSERT INTO items (artist, title, format, mb_primary_type, release_date, release_id, country, mb_id, mb_artist, mb_title, sort_name, version_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
                    (artist, title, format, mb_primary_type, release_date, release_id, country, mb_id, mb_artist, mb_title, sort_name, version_id))


def db_get_release_date_by_discogs_id(discogs_id):

    with db_ops(DATABASE_NAME) as cur:
        cur.execute('SELECT * FROM items WHERE release_id = ?', (int(discogs_id),))
        row = cur.fetchone()

    if row is None:
        return row

    return row.release_date


def db_fetch_row_by_mb_id(mb_id):

    with db_ops(DATABASE_NAME) as cur:
        cur.execute('SELECT * FROM items WHERE mb_id = ?', (mb_id,))
        row = cur.fetchone()

    return row


def scrape_row(discogs_client, discogs_release=None, row=None, discogs_id=0):

    if row is None and discogs_id:
        db_fetch_row_by_discogs_id(discogs_id=discogs_id)

    if row is not None and discogs_id == 0:
        discogs_id = row.release_id

    if not discogs_release and discogs_id:
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
            db_update_release_date(row.release_id, release_date)


def convert_format(discogs_format):

    mb_format = discogs_format.get('name')  # for example CD
    primary_type = discogs_format.get('name')  # for example CD
    quantity = discogs_format.get('qty')
    secondary_types = discogs_format.get('descriptions')  # for example album

    if primary_type == 'Vinyl':

        if 'EP' in secondary_types and '7"' in secondary_types:
            format = '7" EP'
            mb_primary_type = 'single'
        elif 'EP' in secondary_types and '12"' in secondary_types:
            format = '12" EP'
            mb_primary_type = 'single'
        elif '12"' in secondary_types:
            format = '12" Single'
            mb_primary_type = 'single'
        elif '7"' in secondary_types:
            format = '7" Single'
            mb_primary_type = 'single'
        elif 'Compilation' in secondary_types:
            format = 'LP Compilation'
            mb_primary_type = 'album'
        elif 'LP' in secondary_types:
            format = 'LP'
            mb_primary_type = 'album'
        else:
            format = '?'
            mb_primary_type = "?"

    elif primary_type == 'CD':

        if 'Mini' in secondary_types:
            format = 'CD 3" Mini Single'
            mb_primary_type = 'single'
        elif 'Single' in secondary_types:
            format = 'CD Single'
            mb_primary_type = 'single'
        elif 'Maxi-Single' in secondary_types:
            format = 'CD Single'
            mb_primary_type = 'single'
        elif 'EP' in secondary_types:
            format = 'CD EP'
            mb_primary_type = 'single'
        elif 'LP' in secondary_types:
            format = 'CD Album'
            mb_primary_type = 'album'
        elif 'Album' in secondary_types:
            format = 'CD Album'
            mb_primary_type = 'album'
        elif 'Mini-Album' in secondary_types:
            format = 'CD Mini-Album'
            mb_primary_type = 'album'
        elif 'Compilation' in secondary_types:
            format = 'CD Compilation'
            mb_primary_type = 'album'
        else:
            format = "CD Single"
            mb_primary_type = "single"

    elif primary_type == 'Flexi-disc':

        if '7"' in secondary_types:
            format = '7" flexi-disc'
            mb_primary_type = 'single'
        else:
            format = '?'
            mb_primary_type = "?"

    elif primary_type == 'Box Set':

        if '12"' in secondary_types:
            format = '12" Singles Box Set'
            mb_primary_type = 'single'
        elif '7"' in secondary_types:
            format = '7" Singles Box Set'
            mb_primary_type = 'single'
        elif 'LP' in secondary_types:
            format = "LP Box Set"
            mb_primary_type = 'album'
        elif 'EP' in secondary_types:
            format = "EP Box Set"
            mb_primary_type = 'single'
        elif 'Single' in secondary_types:
            mb_primary_type = 'single'
        elif 'Maxi-Single' in secondary_types:
            mb_primary_type = 'single'
        else:
            format = 'Box Set'
            mb_primary_type = "Other"
    else:
        format = '?'
        mb_primary_type = "?"

    return format, mb_primary_type, mb_format


def row_change(release_id, data_name, data_to, data_from):
    return f'💾 {data_name} set to {data_to} (was {data_from}) for release {release_id}'


def row_ignore_change(release_id, data_name, data_to, data_from, reason):
    return f'⛔️ {reason} {data_name} not set to {data_to} (still {data_from}) for release {release_id}'


def mb_get_releases_for_release_group(mb_id):

    batch_offset = 0
    batch_size = 25
    max_results = 200
    release_list = []

    try:
        while len(release_list) < max_results:
            rels = musicbrainzngs.browse_releases(
                release_group=mb_id,
                includes=['artist-credits', 'labels', 'media', 'release-groups'],
                limit=batch_size,
                offset=batch_offset
            )

            # Add new results
            batch_release_list = rels.get('release-list', [])
            release_list.extend(batch_release_list)

            # Check if we've reached the end
            if len(batch_release_list) < batch_size:
                break

            # Move to the next page
            batch_offset += batch_size

    except Exception as e:
        logger.error(e)
        release_list = []

    finally:
        return release_list


def find_match_by_discogs_link(discogs_release=None):
    """Search MusicBrainz by Discogs release URL

    If the search fails, the master URL is also tried.

    Returns the release, release group and release date if successful."""

    mb_release = None
    mb_release_group = None

    releases = mb_browse_releases_by_discogs_release_link(discogs_id=discogs_release.id)
    if not releases:
        # no match found
        mb_release = None
    elif len(releases) == 1:
        mb_release = releases[0]
        # we are only expecting 1 match
        logger.debug(f'🎯 matched {mb_summarise_release(id=mb_release.get('id'))}')
    else:
        logger.warning(f'❗️ unexpected number of matches for Discogs URL ({len(releases)})')

        best_match_score = 0
        best_match_release = None

        for release in releases:
            disambiguation_score = disambiguator_score(
                discogs_release=discogs_release, mb_release=release)

            # print(f'{disambiguation_score:3}% {mb_summarise_release(release)}')

            if disambiguation_score == 100:
                # pefect match
                best_match_score = disambiguation_score
                best_match_release = release
                break
            elif disambiguation_score > best_match_score:
                best_match_score = disambiguation_score
                best_match_release = release

        if best_match_score == 100:
            mb_release = best_match_release
            logger.debug(
                f'💯 {best_match_score}% disambiguation {mb_summarise_release(id=mb_release.get('id'))}')
        elif best_match_score > 0:
            mb_release = best_match_release
            logger.debug(
                f'📈 {best_match_score}% disambiguation {mb_summarise_release(id=mb_release.get('id'))}')
        else:
            mb_release = None

    if mb_release:
        # success - load the release group too
        mb_release = musicbrainzngs.get_release_by_id(
            mb_release['id'], includes=["release-groups", 'artists', 'artist-credits'])
        mb_release = mb_release.get('release')

        release_group_id = mb_release.get('release-group').get('id')
        mb_release_group = musicbrainzngs.get_release_group_by_id(
            release_group_id, includes=['artists', 'releases', 'artist-credits'])
        mb_release_group = mb_release_group.get('release-group')

        first_release_date = earliest_date(mb_release_group.get(
            'first-release-date'), mb_release.get('date'))

        return mb_release_group, mb_release, first_release_date

    if discogs_release.master is None:
        # no master specified for this Discogs release, cannot continue
        return None, None, None

    # try to find the release group link instead
    release_groups = mb_browse_release_groups_by_discogs_master_link(
        discogs_master_id=discogs_release.master.id)
    if not release_groups:
        # no match found
        mb_release_group = None
    elif len(release_groups) == 1:
        mb_release_group = release_groups[0]
        # we are only expecting 1 match
    else:
        # more than 1 release group match
        all_releases = []
        for release_group in release_groups:
            releases = mb_get_releases_for_release_group(mb_id=release_group.get('id'))
            all_releases.extend(releases)

        best_match_score = 0
        best_match_release = None

        for release in all_releases:
            disambiguation_score = disambiguator_score(
                discogs_release=discogs_release, mb_release=release)

            # logger.debug(f'{disambiguation_score:3}% {mb_summarise_release(release)}')

            if disambiguation_score == 100:
                # pefect match
                best_match_score = disambiguation_score
                best_match_release = release
                break
            elif disambiguation_score > best_match_score:
                best_match_score = disambiguation_score
                best_match_release = release

        if best_match_score == 100:
            mb_release = best_match_release
            print(f'💯 {best_match_score}% disambiguation {mb_summarise_release(id=mb_release.get('id'))}')
        elif best_match_score > 0:
            mb_release = best_match_release
            print(f'📈 {best_match_score}% disambiguation {mb_summarise_release(id=mb_release.get('id'))}')
        else:
            # nothing matched
            return None, None, None

        # get the group
        release_group_id = mb_release.get('release-group').get('id')
        mb_release_group = musicbrainzngs.get_release_group_by_id(
            release_group_id, includes=['artists', 'releases', 'artist-credits'])
        mb_release_group = mb_release_group.get('release-group')

        first_release_date = earliest_date(mb_release_group.get(
            'first-release-date'), mb_release.get('date'))

        return mb_release_group, mb_release, first_release_date

    if not mb_release_group:
        # failed to find the group
        return None, None, None

    batch_offset = 0
    batch_size = 25
    max_results = 200
    release_list = []

    try:
        while len(release_list) < max_results:
            rels = musicbrainzngs.browse_releases(
                release_group=mb_release_group.get('id'),
                includes=['artist-credits', 'labels', 'media'],
                limit=batch_size,
                offset=batch_offset
            )

            # Add new results
            batch_release_list = rels.get('release-list', [])
            release_list.extend(batch_release_list)

            # Check if we've reached the end
            if len(batch_release_list) < batch_size:
                break

            # Move to the next page
            batch_offset += batch_size

    except Exception as e:
        logging.error(e)
        rels = []

    if not release_list:
        # no releases, shouldn't happen
        return None, None, None

    best_match_score = 0
    best_match_release = None

    for idx, mb_release in enumerate(release_list):
        disambiguation_score = disambiguator_score(
            discogs_release=discogs_release, mb_release=mb_release)

        # print(f'{disambiguation_score:3}% {mb_summarise_release(mb_release)}')

        if disambiguation_score == 100:
            # pefect match
            best_match_score = disambiguation_score
            best_match_release = mb_release
            break
        elif disambiguation_score > best_match_score:
            best_match_score = disambiguation_score
            best_match_release = mb_release

    if best_match_score == 100:
        mb_release = best_match_release
        print(f'💯 {best_match_score}% disambiguation {mb_summarise_release(id=mb_release.get('id'))}')
    elif best_match_score > 0:
        mb_release = best_match_release
        print(f'📈 {best_match_score}% disambiguation {mb_summarise_release(id=mb_release.get('id'))}')
    else:
        # nothing matched
        return None, None, None

    first_release_date = earliest_date(
        mb_release_group.get('first-release-date'),
        mb_release.get('date')
    )

    return mb_release_group, mb_release, first_release_date


def update_row(discogs_client, discogs_release=None, discogs_id=None, mb_id=None, version_id=0):

    if not discogs_id and discogs_release:
        discogs_id = discogs_release.id

    if not discogs_release and discogs_id:
        discogs_release = discogs_client.release(discogs_id)

    print(f'⚙️ {discogs_summarise_release(discogs_release=discogs_release)}')

    country = discogs_release.country
    mb_country = COUNTRIES.get(normalize_country_name(country))

    label_names = [x.data['name'] for x in discogs_release.labels]
    label_catnos = list(set([sanitise_identifier(x.data['catno']) for x in discogs_release.labels]))

    for format in discogs_release.formats:
        format_desc = ','.join(format.get('descriptions'))
    # formats = [x.data['name'] for x in release.formats]

    format, mb_primary_type, mb_format = convert_format(discogs_release.formats[0])

    # get the barcode
    barcode = None
    barcodes = []
    discogs_identifiers = discogs_release.fetch('identifiers')
    for identifier in discogs_identifiers:
        if identifier['type'] == 'Barcode':
            barcode = sanitise_identifier(identifier['value'])
            barcodes.append(sanitise_identifier(identifier['value']))
    barcodes = list(set(barcodes))

    artist_name = trim_if_ends_with_number_in_brackets(discogs_release.artists[0].name)
    if artist_name == 'Various':
        artist_name = 'Various Artists'

    release_title = discogs_release.title

    # try the Discogs link(s) first
    mb_release_group, mb_release, release_date = find_match_by_discogs_link(
        discogs_release=discogs_release)

    if not mb_release:
        # attempt to match release group and release, and derive release date
        mb_release_group, mb_release, release_date = mb_find_release_group_and_release(
            artist=artist_name,
            title=release_title,
            primary_type=mb_primary_type,
            country=mb_country,
            catnos=label_catnos,
            discogs_id=discogs_release.id)

    if not mb_release:
        candidates = []

        for catno in label_catnos:
            candidates.extend(mb_find_releases(
                artist=artist_name,
                catno=catno,
                country=mb_country,
                format=mb_format,
            ))

        for barcode in barcodes:
            candidates.extend(mb_find_releases(
                artist=artist_name,
                barcode=barcode,
                country=mb_country,
                format=mb_format,
            ))

        for catno in label_catnos:
            candidates.extend(mb_find_releases(
                artist=artist_name,
                catno=catno,
                format=mb_format,
            ))

        for catno in label_catnos:
            candidates.extend(mb_find_releases(
                title=release_title,
                catno=catno
            ))

        for catno in label_catnos:
            candidates.extend(mb_find_releases(
                title=release_title,
                catno=catno,
                barcode=barcode
            ))

        for catno in label_catnos:
            candidates.extend(mb_find_releases(
                artist=artist_name,
                title=release_title,
                catno=catno
            ))

        for catno in label_catnos:
            candidates.extend(mb_find_releases(
                artist=artist_name,
                title=release_title,
                catno=catno,
                primary_type=mb_primary_type,
                country=mb_country
            ))

        best_match_score = 0
        best_match_release = None

        for release in candidates:
            disambiguation_score = disambiguator_score(
                discogs_release=discogs_release, mb_release=release)

            if disambiguation_score == 100:
                best_match_score = disambiguation_score
                best_match_release = release
                break
            elif disambiguation_score > best_match_score:
                best_match_score = disambiguation_score
                best_match_release = release

        if best_match_score == 100:
            mb_release = best_match_release
            print(f'💯 {best_match_score}% disambiguation {mb_summarise_release(id=mb_release.get('id'))}')
        elif best_match_score > 0:
            mb_release = best_match_release
            print(
                f'📈 {best_match_score}% disambiguation {mb_summarise_release(id=mb_release.get('id'))}')
        else:
            mb_release = None

    if mb_release:
        if not release_date:
            release_date = mb_release.get('date')
    else:
        logging.warning(
            f'❌ no match for {discogs_summarise_release(discogs_release=discogs_release)}')
        version_id = 0

    row = db_fetch_row_by_discogs_id(discogs_release.id)
    if row:
        # update any changed fields in the minimal part of the row
        if not release_date:
            release_date = earliest_date(row.release_date, discogs_release.year)

        db_update_artist(discogs_release.id, artist_name, row.artist)
        db_update_title(discogs_release.id, release_title, row.title)
        db_update_format(discogs_release.id, format, row.format)
        db_update_mb_primary_type(discogs_release.id, mb_primary_type, row.mb_primary_type)
        db_update_country(discogs_release.id, mb_country, row.country)
        db_update_release_date(discogs_release.id, release_date, row.release_date)
        db_update_version_id(discogs_release.id, version_id, row.version_id)

    else:

        # no row exists yet - create a minimal row
        print(f'💾 {discogs_summarise_release(discogs_release=discogs_release)}')

        db_insert_row(release_id=discogs_release.id,
                      artist=artist_name,
                      title=release_title,
                      format=format,
                      mb_primary_type=mb_primary_type,
                      release_date=discogs_release.year,
                      country=mb_country,
                      version_id=version_id)

        row = db_fetch_row_by_discogs_id(discogs_release.id)

    # at this point a number of scenarios are possible:
    #   we definitely have a discogs release
    #   we definitely have a row, either minimal or full
    #   we might have a matched MusicBrainz release group, release and release date
    #   we might just have a mtched MusicBrainz release
    #   we might have no matched MusicBrainz release at all

    if mb_release is None:
        # nothing further can be done with this release at this time, but certain fields
        # should be unset if present
        mb_id = None
        mb_artist = None
        mb_title = None

        db_update_mb_id(row.release_id, mb_id, row.mb_id)
        db_update_mb_artist(row.release_id, mb_artist, row.mb_artist)
        db_update_mb_title(row.release_id, mb_title, row.mb_title)

        return

    if not release_date:
        release_date = earliest_date(mb_release.get('date'), release_date)

    if not mb_release_group:
        # reload the release, including the release group information, artists etc
        mb_release_details = musicbrainzngs.get_release_by_id(
            mb_release['id'], includes=["release-groups", 'artists', 'artist-credits'])

        mb_release = mb_release_details.get('release')
        if mb_release:
            mb_release_group = mb_release.get('release-group')
            if mb_release_group:
                release_date = earliest_date(
                    release_date, mb_release_group.get('first-release-date'))

    # row already exists, just update any out of date or missing items
    row = db_fetch_row_by_discogs_id(discogs_release.id)

    release_id = row.release_id

    # mb_release = mb_release_details.get('release')
    mb_id = mb_release.get('id')
    mb_artist = mb_release.get('artist-credit-phrase')
    mb_title = mb_release.get('title')
    mb_artist_credit = mb_release.get('artist-credit')
    mb_artist_first = mb_artist_credit[0].get('artist')
    mb_sort_name = mb_artist_first.get('sort-name')
    if not mb_sort_name:
        mb_sort_name = mb_artist

    db_update_mb_id(release_id, mb_id, row.mb_id)
    db_update_mb_artist(release_id, mb_artist, row.mb_artist)
    db_update_mb_title(release_id, mb_title, row.mb_title)
    db_update_sort_name(release_id, mb_sort_name, row.sort_name)
    db_update_release_date(release_id, release_date, row.release_date)
    db_update_format(discogs_release.id, format, row.format)
    db_update_mb_primary_type(discogs_release.id, mb_primary_type, row.mb_primary_type)


def import_from_discogs(discogs_id=None, all_rows=False, new_rows=False):

    if discogs_id == None and all_rows == False and new_rows == False:
        new_rows = True

    musicbrainz.set_useragent(USER_AGENT, '0.1', 'steve.powell@outlook.com')

    try:
        musicbrainz.auth(MUSICBRAINZ_USERNAME, MUSICBRAINZ_PASSWORD)

    except HTTPError:
        logging.error("Unable to authenticate to Discogs.")
        sys.exit(1)

    except Exception as e:
        logging.error(f'MusicBrainz authentication error {e}')
        sys.exit(1)

    musicbrainzngs.set_rate_limit(1, 1)
    # musicbrainzngs.set_format(fmt='json')

    oauth_token = 'bTNnyxNgaHvEarRXVjBiRAoJZgTBPuUXosDEdiEG'
    ouath_token_secret = 'YJfCvMXmaxJgfroTnSjtSDKWZpsLbEYPEwUwuSyK'

    discogs_client, discogs_access_token, discogs_access_secret = connect_to_discogs(
        oauth_token, ouath_token_secret)

    # fetch the identity object for the current logged in user.
    discogs_user = discogs_client.identity()

    logging.debug(" == User ==")
    logging.debug(f"    * username           = {discogs_user.username}")
    logging.debug(f"    * name               = {discogs_user.name}")
    logging.debug(" == Access Token ==")
    logging.debug(f"    * oauth_token        = {discogs_access_token}")
    logging.debug(f"    * oauth_token_secret = {discogs_access_secret}")
    logging.debug(" Authentication complete. Future requests will be signed with the above tokens.")

    # get the highest version number, which will be used for all updates
    max_version_id = 0
    min_version_id = 0

    with db_ops(DATABASE_NAME) as cur:

        # get the highest version number
        cur.execute("""
                SELECT MAX(version_id) as max_version_id, MIN(version_id) as min_version_id FROM items
            """)
        row = cur.fetchone()

        max_version_id = row.max_version_id if row.max_version_id else 0
        min_version_id = row.min_version_id if row.min_version_id else 0

    if discogs_id:
        if all_rows:
            update_row(discogs_client=discogs_client,
                       discogs_release=discogs_item.release, version_id=max_version_id)
            return

        row = db_fetch_row_by_discogs_id(discogs_item.id)
        if row is not None:
            print(f'⏭️ {db_summarise_row(row=row)}')
            return

        update_row(discogs_client=discogs_client,
                   discogs_release=discogs_item.release, version_id=max_version_id)

    else:
        folder = discogs_user.collection_folders[0]

        print(f'number of items in all collections: {len(folder.releases)}')

        for discogs_item in discogs_user.collection_folders[0].releases:

            if all_rows:
                update_row(discogs_client=discogs_client,
                           discogs_release=discogs_item.release, version_id=max_version_id)
                continue

            row = db_fetch_row_by_discogs_id(discogs_item.id)
            if row is not None:
                print(f'⏭️ {db_summarise_row(row=row)}')
                continue

            update_row(discogs_client=discogs_client,
                       discogs_release=discogs_item.release, version_id=max_version_id)


def open_db():
    """Create database"""

    with db_ops(DATABASE_NAME) as cur:
        res = cur.execute("SELECT name FROM sqlite_master WHERE name='items'")
        if res.fetchone() is None:
            logging.debug("creating table")
            cur.execute(
                "CREATE TABLE items(artist, title, format, release_id, release_date, country, mb_id, mb_artist, mb_title, sort_name, version_id)")
            cur.execute("CREATE UNIQUE INDEX idx_items_release_id ON items(release_id)")
            cur.execute(
                "CREATE UNIQUE INDEX idx_items_sort ON items(sort_name, artist, release_date, title, release_id)")
            cur.execute("CREATE UNIQUE INDEX idx_items_version ON items(version_id, release_id)")
    # else:
    #   cur.execute("ALTER TABLE status ADD locked NOT NULL DEFAULT 0")
        # word count, word list, words
        # cur.execute("ALTER TABLE status ADD word_count DEFAULT 0")
        # cur.execute("ALTER TABLE status DROP words_found")
        # cur.execute("ALTER TABLE status ADD transcription")
        # cur.execute("CREATE UNIQUE INDEX idx_status_primary ON status(girl, number)")


def fls(data_str, length):
    if len(data_str) > length:
        return data_str[:length-3]+'...'
    else:
        return data_str.ljust(length)


def match(text, release_date=None):
    set_date = None

    with db_ops(DATABASE_NAME) as cur:

        cur.execute(f"""
            SELECT *
            FROM items
            WHERE artist LIKE '%{text}%'
            OR mb_artist LIKE '%{text}%'
            OR title LIKE '%{text}%'
            OR mb_title LIKE '%{text}%'
            OR sort_name LIKE '%{text}%'
            ORDER BY artist, release_date, title, release_id
        """)

        rows = cur.fetchall()
        if len(rows) == 0:
            logging.warning('no matching items')
            return

        if release_date:

            if len(release_date) > 10:
                # try day, month and year match first
                formats = ['%-d %b %Y']
                settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first',
                            'DATE_ORDER': 'DMY', 'PREFER_LOCALE_DATE_ORDER': False, 'REQUIRE_PARTS': ['day', 'month', 'year']}
                date_object = dateparser.parse(
                    release_date, date_formats=formats, settings=settings)

                if date_object:
                    set_date = date_object.strftime('%Y-%m-%d')

            elif len(release_date) == 10:
                # try day, month and year match first
                formats = ["%Y-%m-%d", '%-d %B %Y']
                settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first',
                            'DATE_ORDER': 'DMY', 'PREFER_LOCALE_DATE_ORDER': False, 'REQUIRE_PARTS': ['day', 'month', 'year']}
                date_object = dateparser.parse(
                    release_date, date_formats=formats, settings=settings)

                if date_object:
                    set_date = date_object.strftime('%Y-%m-%d')

            if len(release_date) == 8:
                # try month and year
                formats = ["'b %Y"]
                settings = {'PREFER_DAY_OF_MONTH': 'first', 'REQUIRE_PARTS': ['month', 'year']}
                # settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first',
                #             'DATE_ORDER': 'DMY', 'PREFER_LOCALE_DATE_ORDER': False, 'REQUIRE_PARTS': ['month', 'year']}

                date_object = dateparser.parse(
                    release_date, date_formats=formats, settings=settings)

                if date_object:
                    set_date = date_object.strftime("%Y-%m")

            if len(release_date) == 7:
                # try month and year
                formats = ["%Y-%m"]
                settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first',
                            'DATE_ORDER': 'DMY', 'PREFER_LOCALE_DATE_ORDER': False, 'REQUIRE_PARTS': ['month', 'year']}

                date_object = dateparser.parse(
                    release_date, date_formats=formats, settings=settings)

                if date_object:
                    set_date = date_object.strftime("%Y-%m")

            if len(release_date) == 4:
                # try for just a year
                formats = ["%Y"]
                settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first',
                            'DATE_ORDER': 'DMY', 'PREFER_LOCALE_DATE_ORDER': False, 'REQUIRE_PARTS': ['year']}

                date_object = dateparser.parse(
                    release_date, date_formats=formats, settings=settings)

                if date_object:
                    set_date = date_object.strftime("%Y")

            if not set_date:
                logging.warning(f'invalid date {release_date}')
                return

        if set_date:
            if set_date and len(rows) > 1:
                print('more than one item matched, release date not set')
                return
            else:
                cur.execute(f"""
                    SELECT *
                    FROM items
                    WHERE artist LIKE '%{text}%'
                    OR mb_artist LIKE '%{text}%'
                    OR title LIKE '%{text}%'
                    OR mb_title LIKE '%{text}%'
                    OR sort_name LIKE '%{text}%'
                    ORDER BY artist, release_date, title, release_id
                """)

                row = cur.fetchone()

                db_update_release_date(row.release_id, set_date, row.release_date)
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

                artist = row.mb_artist if row.mb_artist else row.artist if row.artist else 'Unknown artist'
                artist = row.artist
                title = row.mb_title if row.mb_title else row.title if row.title else 'Unknown title'

                format = row.format if row.format else ''
                release_date = row.release_date if row.release_date else ''

                if release_date:
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
                        f'{row.release_id:>8} {fls(title, title_len)} {fls(format, format_len)} {fls(release_date, release_date_len)}')


def status():

    def output_nvp(label, value):
        print(f'{fls(label, 45)}: {value}')

    oauth_token = 'bTNnyxNgaHvEarRXVjBiRAoJZgTBPuUXosDEdiEG'
    ouath_token_secret = 'YJfCvMXmaxJgfroTnSjtSDKWZpsLbEYPEwUwuSyK'

    discogs_client, discogs_access_token, discogs_access_secret = connect_to_discogs(
        oauth_token, ouath_token_secret)

    # fetch the identity object for the current logged in user.
    discogs_user = discogs_client.identity()

    output_nvp("Discogs username", discogs_user.username)
    output_nvp("MusicBrainz username", MUSICBRAINZ_USERNAME)

    folder = discogs_user.collection_folders[0]

    output_nvp('releases on Discogs', len(folder.releases))

    with db_ops(DATABASE_NAME) as cur:

        cur.execute("""
            SELECT COUNT(*) as count
            FROM items
        """)
        row = cur.fetchone()

        output_nvp('releasees in local database', row.count)

        cur.execute("""
            SELECT COUNT(*) as count
            FROM items
            WHERE mb_id IS NOT NULL
        """)
        row = cur.fetchone()

        output_nvp('releases matched in MusicBrainz', row.count)

        cur.execute("""
            SELECT COUNT(*) as count
            FROM items
            WHERE mb_id IS NULL
        """)
        row = cur.fetchone()

        output_nvp('releases not matched in MusicBrainz', row.count)

        # get the highest version number, which will be used for all updates
        max_version_id = 0
        min_version_id = 0

        # get the highest version number
        cur.execute("""
                SELECT MAX(version_id) as max_version_id, MIN(version_id) as min_version_id FROM items
            """)
        row = cur.fetchone()

        max_version_id = row.max_version_id if row.max_version_id else 0
        min_version_id = row.min_version_id if row.min_version_id else 0

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM items
            WHERE version_id IS NULL OR version_id < {max_version_id}
        """)
        row = cur.fetchone()

        output_nvp('local releases pending update', row.count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM items
            WHERE release_date IS NULL OR release_date = ''
        """)
        row = cur.fetchone()

        output_nvp('releases with no release date', row.count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM items
            WHERE LENGTH(release_date) = 4
        """)
        row = cur.fetchone()

        output_nvp('releases with just release year', row.count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM items
            WHERE LENGTH(release_date) = 7
        """)
        row = cur.fetchone()

        output_nvp('releases with just release month and year', row.count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM items
            WHERE LENGTH(release_date) = 10
        """)
        row = cur.fetchone()

        output_nvp('releases with full release date', row.count)


def random_selection():

    with db_ops(DATABASE_NAME) as cur:

        cur.execute("""
            SELECT *
            FROM items
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
    print(f'release_id      : {row.release_id}')
    if row.release_date:
        # print(f'release_date    : {row.release_date}')
        print(f'release_date    : {parse_and_humanize_date(row.release_date)}')


def update_mb_id(release_id, mb_id):
    global args

    if args.dry_run:
        logging.warning('dry run - not updated')
        return

    with db_ops(DATABASE_NAME) as cur:
        cur.execute('UPDATE items SET mb_id = ? WHERE release_id = ?', (mb_id, release_id))


def parse_date(date_str):
    if date_str and not isinstance(date_str, str):
        date_str = str(date_str)

    date_formats = ['%Y-%m-%d', '%Y-%m', '%Y']
    settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first'}

    if date_str is None or date_str == '':
        return None

    date_object = dateparser.parse(date_str, date_formats=date_formats, settings=settings)

    return date_object.date()


def parse_and_humanize_date(ymd_date):

    if not ymd_date:
        return ''

    if len(ymd_date) == 10:
        # try day, month and year match first
        formats = ["%Y-%m-%d"]
        settings = {'REQUIRE_PARTS': ['day', 'month', 'year']}
        date_object = dateparser.parse(ymd_date, date_formats=formats, settings=settings)

        if date_object:
            return date_object.strftime('%A %-d %B %Y')

    if len(ymd_date) == 7:
        # try month and year
        formats = ["%Y-%m"]
        settings = {'REQUIRE_PARTS': ['month', 'year']}

        date_object = dateparser.parse(ymd_date, date_formats=formats, settings=settings)

        if date_object:
            return date_object.strftime("%B %Y")

    if len(ymd_date) == 4:
        # try for just a year
        formats = ["%Y"]
        settings = {'REQUIRE_PARTS': ['year']}

        date_object = dateparser.parse(ymd_date, date_formats=formats, settings=settings)

        if date_object:
            return date_object.strftime("%Y")

    return ''


def update_table(discogs_id=None, mb_id=None, all_rows=False, new_rows=False, reset_versions=False):

    if discogs_id == None and mb_id == None and all_rows == False and new_rows == False:
        new_rows = True

    musicbrainz.set_useragent(USER_AGENT, '0.1', 'steve.powell@outlook.com')

    try:
        musicbrainz.auth(MUSICBRAINZ_USERNAME, MUSICBRAINZ_PASSWORD)

    except HTTPError:
        logging.error("Unable to authenticate to Discogs.")
        sys.exit(1)

    except Exception as e:
        logging.error(f'MusicBrainz authentication error {e}')
        sys.exit(1)

    musicbrainzngs.set_rate_limit(1, 1)

    oauth_token = 'bTNnyxNgaHvEarRXVjBiRAoJZgTBPuUXosDEdiEG'
    ouath_token_secret = 'YJfCvMXmaxJgfroTnSjtSDKWZpsLbEYPEwUwuSyK'

    discogs_client, discogs_access_token, discogs_access_secret = connect_to_discogs(
        oauth_token, ouath_token_secret)

    # get the highest version number, which will be used for all updates
    max_version_id = 0
    min_version_id = 0

    if reset_versions:
        # reset the row versions, so that all will be processed
        with db_ops(DATABASE_NAME) as cur:
            cur.execute("""
                    UPDATE items
                    SET version_id = 0
                """)
    else:
        with db_ops(DATABASE_NAME) as cur:

            # get the highest version number
            cur.execute("""
                    SELECT MAX(version_id) as max_version_id, MIN(version_id) as min_version_id FROM items
                """)
            row = cur.fetchone()

            max_version_id = row.max_version_id if row.max_version_id else 0
            min_version_id = row.min_version_id if row.min_version_id else 0

    if discogs_id:
        row = db_fetch_row_by_discogs_id(discogs_id)
        if row is not None:
            update_row(discogs_client, discogs_id=int(discogs_id), version_id=max_version_id)
        else:
            logging.error(f'discogs_id {discogs_id} not found')

    elif mb_id:
        row = db_fetch_row_by_mb_id(mb_id)
        if row is not None:
            update_row(discogs_client, discogs_id=row.release_id,
                       mb_id=mb_id, max_version_id=max_version_id)
        else:
            logging.error(f'MBID {mb_id} not found')

    else:

        with db_ops(DATABASE_NAME) as cur:

            if all_rows == False and max_version_id > 0 and max_version_id > min_version_id:
                # only process items below the maximum version
                cur.execute(f"""
                        SELECT * FROM items WHERE version_id IS NULL OR version_id < {max_version_id}
                    """)
                rows = cur.fetchall()

                for row in rows:
                    update_row(discogs_client, discogs_id=row.release_id, version_id=max_version_id)
            else:
                # process all rows
                cur.execute("""
                        SELECT * FROM items
                    """)

                rows = cur.fetchall()

                for row in rows:
                    update_row(discogs_client, discogs_id=row.release_id,
                               version_id=max_version_id+1)


def scrape_discogs(discogs_id=0, mb_id=None):

    oauth_token = 'bTNnyxNgaHvEarRXVjBiRAoJZgTBPuUXosDEdiEG'
    ouath_token_secret = 'YJfCvMXmaxJgfroTnSjtSDKWZpsLbEYPEwUwuSyK'

    discogs_client, discogs_access_token, discogs_access_secret = connect_to_discogs(
        oauth_token, ouath_token_secret)

    if discogs_id:
        row = db_fetch_row_by_discogs_id(discogs_id)
        if row is not None:
            print(f'update {db_summarise_row(row=row)}')
            scrape_row(discogs_client=discogs_client, row=row, discogs_id=int(discogs_id))
        else:
            logging.error(f'discogs_id {discogs_id} not found')

    elif mb_id:
        row = db_fetch_row_by_mb_id(mb_id)
        if row is not None:
            print(f'update {db_summarise_row(row=row)}')
            scrape_row(discogs_client=discogs_client, row=row)
        else:
            logging.error(f'MBID {mb_id} not found')

    else:

        with db_ops(DATABASE_NAME) as cur:

            cur.execute("""
                SELECT *
                FROM items
                WHERE mb_id IS NULL or mb_artist IS NULL or mb_title IS NULL or sort_name IS NULL or country IS NULL
                ORDER BY artist, release_date, title, release_id
            """)

            rows = cur.fetchall()

            print(f'{len(rows)} rows')

            for row in rows:
                print(f'update {db_summarise_row(row=row)}')
                scrape_row(discogs_client=discogs_client, row=row)


def mb_get_artist(artist):
    """
        Search for artist by name, returning a single match

        artist name can have more than one exact match

        alias	        (part of) any alias attached to the artist (diacritics are ignored)
        primary_alias	(part of) any primary alias attached to the artist (diacritics are ignored)
        area	        (part of) the name of the artist's main associated area
        arid	        the artist's MBID
        artist	        (part of) the artist's name (diacritics are ignored)
        artistaccent	(part of) the artist's name (with the specified diacritics)
        begin	        the artist's begin date (e.g. "1980-01-22")
        beginarea	    (part of) the name of the artist's begin area
        comment	        (part of) the artist's disambiguation comment
        country	        the 2-letter code (ISO 3166-1 alpha-2) for the artist's main associated country
        end	            the artist's end date (e.g. "1980-01-22")
        endarea	        (part of) the name of the artist's end area
        ended	        a boolean flag (true/false) indicating whether or not the artist has ended (is dissolved/deceased)
        gender	        the artist's gender (“male”, “female”, “other” or “not applicable”)
        ipi	            an IPI code associated with the artist
        isni	        an ISNI code associated with the artist
        sortname	    (part of) the artist's sort name
        tag	            (part of) a tag attached to the artist
        type	        the artist's type (“person”, “group”, etc.)
    """

    print(f'searching for artist {artist}')
    result = musicbrainzngs.search_artists(query=f'artist:"{artist}"')

    if result is None:
        return None

    artist_list = result['artist-list']

    if not artist_list:
        return None

    print(f'found {len(artist_list)} matches')

    # for arow in artist_list:
    #     print(f'    id  : {arow['id']}')
    #     print(f'    name: {arow['name']}')

    # match a single
    artist_index = -1
    for index, value in enumerate(artist_list):
        if value['name'].casefold() == artist.casefold():
            artist_index = index
            break

    if artist_index < 0:
        return None

    # arow = artist_list[artist_index]
    # print(arow)
    # print(f'id  : {arow['id']}')
    # print(f'name: {arow['name']}')

    newlist = [x for x in artist_list if artist.casefold() == x['name'].casefold()]

    return newlist[0]


def earliest_date(dt1_str, dt2_str):
    """ Return the earlier of two dates

        Given two date strings, return the earlier of the two. Dates
        can be in the form YYYY-MM-DD, YYYY-MM or simply YY. The
        comparison takes into account partial dates, so that 1982-04
        is not considered earlier than 1982-04-10, but would be
        considered earlier than 1982-05
    """

    if dt1_str and not isinstance(dt1_str, str):
        dt1_str = str(dt1_str)

    if dt2_str and not isinstance(dt2_str, str):
        dt2_str = str(dt2_str)

    if dt1_str == dt2_str:
        return dt1_str

    dt1_obj = parse_date(dt1_str) if dt1_str else None
    dt2_obj = parse_date(dt2_str) if dt2_str else None

    if dt1_obj is None and dt2_obj is None:
        return

    if dt1_obj is not None and dt2_obj is None:
        return dt1_str

    if dt1_obj is None and dt2_obj is not None:
        return dt2_str

    if dt1_obj.year < dt2_obj.year:
        return dt1_str

    if dt1_obj.year > dt2_obj.year:
        return dt2_str

    # year is the same
    # if one has a month and the other doesn't, it wins
    if len(dt1_str) == 4 and len(dt2_str) >= 7:
        return dt2_str

    if len(dt1_str) >= 7 and len(dt2_str) == 4:
        return dt1_str

    if dt1_obj.month < dt2_obj.month:
        return dt1_str

    if dt1_obj.month > dt2_obj.month:
        return dt2_str

    # year and month are the same
    # if one has a day and the other doesn't, it wins
    if len(dt1_str) == 7 and len(dt2_str) == 10:
        return dt2_str

    if len(dt1_str) == 10 and len(dt2_str) == 7:
        return dt1_str

    if dt1_obj.day < dt2_obj.day:
        return dt1_str

    if dt1_obj.day > dt2_obj.day:
        return dt2_str

    return dt1_str


def mb_find_release_group_and_release(artist='', title='', primary_type=None, alias='', country='', catnos=[], discogs_id=0):
    """ Search for a release group and release (if discogs_id is specified)

        All these fields are searchable, only some are implemented.

        alias	            (part of) any alias attached to the release group (diacritics are ignored)
        arid	            the MBID of any of the release group artists
        artist	            (part of) the combined credited artist name for the release group, including join phrases (e.g. "Artist X feat.")
        artistname	        (part of) the name of any of the release group artists
        comment	            (part of) the release group's disambiguation comment
        creditname	        (part of) the credited name of any of the release group artists on this particular release group
        firstreleasedate	the release date of the earliest release in this release group (e.g. "1980-01-22")
        primarytype	        the primary type of the release group
        reid	            the MBID of any of the releases in the release group
        release	            (part of) the title of any of the releases in the release group
        releasegroup	    (part of) the release group's title (diacritics are ignored)
        releasegroupaccent	(part of) the release group's title (with the specified diacritics)
        releases	        the number of releases in the release group
        rgid	            the release group's MBID
        secondarytype	    any of the secondary types of the release group
        status	            the status of any of the releases in the release group
        tag	                (part of) a tag attached to the release group
        type	            legacy release group type field that predates the ability to set multiple types (see calculation code)
    """

    query = []

    if discogs_id > 0:
        discogs_url = f"https://www.discogs.com/release/{discogs_id}"
    else:
        discogs_url = None

    if artist:
        query.append(f'artist:"{artist}"')

    if title:
        # query.append(f'release:"{title}"')
        query.append(f'releasegroup:"{title}"')

    if primary_type:
        query.append(f'primarytype:{primary_type}')

    if alias:
        query.append(f'alias:"{alias}"')

    query_string = ' AND '.join(query)

    all_results = []
    offset = 0
    batch_size = 25
    max_results = 100

    while len(all_results) < max_results:

        result = musicbrainzngs.search_release_groups(
            query=query_string, limit=batch_size, offset=offset)

        # Add new results
        release_group_list = result.get('release-group-list', [])
        all_results.extend(release_group_list)

        if release_group_list:
            # search this batch of release groups
            for group in release_group_list:
                # fetch the release group including releases - note
                gr = musicbrainzngs.get_release_group_by_id(group.get('id'), includes=['releases'])
                gr_group = gr.get('release-group')
                gr_release_list = gr_group.get('release-list')

                # print(f'checking {mb_summarise_release_group(id=gr_group.get('id'))}')

                release_date = group.get('first-release-date')

                for index, gr_release in enumerate(gr_release_list):
                    if mb_match_discogs_release(gr_release.get('id'), discogs_url=discogs_url):
                        print(f'✅ matched {mb_summarise_release(id=gr_release.get('id'))}')

                        release_date = earliest_date(
                            group.get('first-release-date'), gr_release.get('date'))
                        return gr_group, gr_release, release_date

                    gr_release = musicbrainzngs.get_release_by_id(gr_release.get(
                        'id'), includes=["url-rels", 'artists', 'artist-credits', 'labels']).get('release')

                    if artist and gr_release.get('artist-credit-phrase') != artist:
                        continue

                    if title and gr_release.get('title') != title:
                        continue

                    # if primary_type and gr_release.get('primarytype') != primary_type:
                    #     continue

                    if country and gr_release.get('country') != country:
                        continue

                    # catnos = []
                    if catnos and len(catnos):
                        match = False
                        for catno in catnos:
                            for label_info in gr_release.get('label-info-list'):
                                find_catno = label_info.get('catalog-number')
                                if catno is not None and catno == sanitise_identifier(find_catno):
                                    match = True
                                    break

                            if match:
                                break

                        if not match:
                            continue

                    print(f'? maybe {mb_summarise_release(id=gr_release.get('id'))}')

        # Check if we've reached the end
        if len(release_group_list) < batch_size:
            break

        # Move to the next page
        offset += batch_size

    # print(f'{len(all_results)} result(s) from query {query_string}')

    return None, None, None


def get_artist_mbid(discogs_id):
    if discogs_id > 0:
        discogs_url = f"https://www.discogs.com/release/{discogs_id}"
    else:
        discogs_url = None

    if not discogs_url:
        return None

    try:
        mb_url = musicbrainzngs.browse_urls(discogs_url, includes=["artist-rels"])

    except musicbrainzngs.musicbrainz.ResponseError:
        # TODO: Raise appropriate exception. (URL doesn't exist.)
        return None

    if "artist-relation-list" in mb_url["url"]:
        artists = mb_url["url"]["artist-relation-list"]
        if len(artists) == 1:
            return artists[0]["artist"]["id"]
        elif len(artists) > 1:
            # TODO: Raise appropriate exception. (More than one associated artist.)
            return None
    else:
        # TODO: Raise appropriate exception. (No associated artists.)
        return None


def mb_browse_releases_by_discogs_release_link(discogs_id=0):
    """Browse releases for a given Discogs URL

    The expected result is either no matches or a single match. However
    it is possible for more than one match to be found. Returns a list
    of releases."""

    discogs_url = f"https://www.discogs.com/release/{discogs_id}"
    offset = 0
    batch_size = 25
    max_releases = 200
    releases = []
    all_results = 0

    try:
        while all_results < max_releases:

            mb_url = musicbrainzngs.browse_urls(
                discogs_url,
                includes=["release-rels"],
                limit=batch_size,
                offset=offset
            )

            url = mb_url.get('url')
            release_relation_list = url.get('release-relation-list')

            all_results += len(release_relation_list)

            for release_relation in release_relation_list:
                if release_relation.get('type') == 'discogs':
                    release = release_relation.get('release')
                    releases.append(release)

            # Check if we've reached the end
            if len(release_relation_list) < batch_size:
                break

            # Move to the next page
            offset += batch_size

    except musicbrainzngs.musicbrainz.ResponseError:
        # probably 404
        pass

    except Exception as e:
        print(e)

    finally:
        return releases


def mb_browse_release_groups_by_discogs_master_link(discogs_master_id=0):
    """Browse release groups for a given Discogs master URL

    The expected result is either no matches or a single match. However
    it is possible for more than one match to be found. Returns a list
    of release groups."""

    discogs_url = f"https://www.discogs.com/master/{discogs_master_id}"
    offset = 0
    batch_size = 25
    max_release_groups = 200
    release_groups = []
    all_results = 0

    try:
        while all_results < max_release_groups:

            mb_url = musicbrainzngs.browse_urls(
                discogs_url,
                includes=["release-group-rels"],
                limit=batch_size,
                offset=offset
            )

            url = mb_url.get('url')
            release_group_relation_list = url.get('release_group-relation-list')

            all_results += len(release_group_relation_list)

            for release_relation in release_group_relation_list:
                if release_relation.get('type') == 'discogs':
                    release_group = release_relation.get('release-group')
                    release_groups.append(release_group)

            # Check if we've reached the end
            if len(release_group_relation_list) < batch_size:
                break

            # Move to the next page
            offset += batch_size

    except musicbrainzngs.musicbrainz.ResponseError:
        # probably 404
        pass

    except Exception as e:
        print(e)

    finally:
        return release_groups


def make_discogs_url(release_id=0, master_release_id=0):
    if release_id:
        return f"https://www.discogs.com/release/{release_id}"
    elif master_release_id:
        return f"https://www.discogs.com/master/{master_release_id}"
    else:
        return ''


def mb_match_discogs_release(mb_release_id, discogs_url):
    """ search release relationships for a Discogs link """

    mb_release = musicbrainzngs.get_release_by_id(
        mb_release_id, includes=["url-rels", 'artists', 'artist-credits'])

    local_release = mb_release.get('release', {})

    for rel in local_release.get('url-relation-list', []):
        # print(f'    type {rel['type']} target {rel['target']}')
        if rel['type'] == 'discogs':
            # print(f'    type {rel['type']} target {rel['target']}')
            if rel['type'] == 'discogs' and rel['target'] == discogs_url:
                # print(f"✅ matched {mb_summarise_release(mb_release=local_release)}")
                return True

    return False


def sanitise_identifier(catno):
    catno_string = ''.join(chr for chr in catno if chr.isalnum()).casefold()
    return catno_string


def mb_match_catno(mb_release, catno):
    # match catalog number(s) against a release (because search by catno is broken)

    if catno is None:
        return False

    lil = mb_release.get('label-info-list')
    if not lil:
        return False

    label_catnos = []
    for x in lil:

        label_catno = x.get('catalog-number')
        if label_catno:
            label_catnos.append(sanitise_identifier(label_catno))

    catno_list = list(set(catno))
    catno_list_str = ','.join(catno_list)

    for full_catno in catno_list:
        catnostring = sanitise_identifier(full_catno)

        for label_catno in label_catnos:
            if catnostring == label_catno:
                return True

    return False


def mb_summarise_release_group(mb_release_group=None, mb_id=None):
    load_mbid = None

    if mb_release_group:
        x = mb_release_group.get('release-list')
        if not x:
            load_mbid = mb_release_group.get('id')
        # x = mb_release_group.get('artists')
        # if not x:
        #     load_mbid = mb_release_group.get('id')
        x = mb_release_group.get('artist-credit-phrase')
        if not x:
            load_mbid = mb_release_group.get('id')
    else:
        load_mbid = mb_id

    if mb_release_group is None and load_mbid == 0:
        raise Exception("Unable to load MusicBrainz release group")

    if load_mbid:
        mb_release_group = musicbrainzngs.get_release_group_by_id(
            load_mbid, includes=['releases', 'artists', 'artist-credits'])
        if mb_release_group:
            mb_release_group = mb_release_group.get('release-group')
        else:
            raise Exception("Unable to load MusicBrainz release group")

    output = []

    mbid = mb_release_group.get('id')
    output.append(f'MB group {mbid}')

    artist = mb_release_group.get('artist-credit-phrase')
    if not artist:
        artist = mb_release_group.get('artist')
    if artist:
        output.append(artist)

    title = mb_release_group.get('title')
    if title:
        output.append(title)

    # release_list = mb_release_group.get('release-list')
    # if release_list:
    #     output.append(f'({len(release_list)} rel)')
    release_count = mb_release_group.get('release-count')
    if release_count:
        output.append(f'({release_count} {'release' if release_count == 1 else 'releases'})')

    first_release_date = mb_release_group.get('first-release-date')
    if first_release_date:
        output.append(first_release_date)

    return ' '.join(output)


def mb_summarise_release(mb_release=None, mb_id=None):

    load_mbid = None

    if mb_release:
        x = mb_release.get('label-info-count')
        if not x:
            load_mbid = mb_release.get('id')
        x = mb_release.get('artist-credit-phrase')
        if not x:
            load_mbid = mb_release.get('id')
        x = mb_release.get('medium-list')
        if not x:
            load_mbid = mb_release.get('id')
    else:
        load_mbid = mb_id

    if mb_release is None and load_mbid == 0:
        raise Exception("Unable to load MusicBrainz release")

    if load_mbid:
        mb_release = musicbrainzngs.get_release_by_id(
            load_mbid, includes=['artists', 'artist-credits', 'labels', 'media'])
        if mb_release:
            mb_release = mb_release.get('release')
        else:
            raise Exception("Unable to load MusicBrainz release")

    output = []

    mbid = mb_release.get('id')
    output.append(f'MB release {mbid}')

    artist = mb_release.get('artist-credit-phrase')
    if not artist:
        artist = mb_release.get('artist')
    if artist:
        output.append(artist)

    title = mb_release.get('title')
    if title:
        output.append(title)

    date = mb_release.get('date')
    if date:
        output.append(date)

    country = mb_release.get('country')
    if country:
        output.append(country)

    lil = mb_release.get('label-info-list')
    if lil:
        label_catnos = []
        for x in lil:

            label_catno = x.get('catalog-number')
            if label_catno:
                label_catnos.append(label_catno)
        catnos = ','.join(label_catnos)
        output.append(catnos)

    media = mb_release.get('medium-list')
    if media and len(media):
        first_media = media[0]
        format = first_media.get('format')
        if format:
            output.append(format)

    return ' '.join(output)


def discogs_summarise_release(discogs_release=None, id=None, discogs_client=None):
    if not discogs_release and id and discogs_client:
        discogs_release = discogs_client.release(id)

    output = []

    id = discogs_release.id
    output.append(f'Discogs {id}')

    artist = trim_if_ends_with_number_in_brackets(discogs_release.artists[0].name)
    if artist:
        output.append(artist)

    title = discogs_release.title
    if title:
        output.append(title)

    year = discogs_release.year
    if year:
        output.append(str(year))

    country = discogs_release.country
    if country:
        country = COUNTRIES.get(country)
        if country:
            output.append(country)
        else:
            print(f'{discogs_release.country} not found!')

    first_format = discogs_release.formats[0]
    format = first_format.get('name') + ' ' + ' '.join(first_format.get('descriptions'))

    if format:
        output.append(format)

    return ' '.join(output)


def db_formatted_row(row=None, id=None):
    if not row and id:
        row = db_fetch_row_by_discogs_id(id)

    output = []

    id = row.release_id
    output.append(f'release {id}')

    artist = row.artist
    if artist:
        output.append(artist)

    title = row.title
    if title:
        output.append(title)

    format = row.format
    if format:
        output.append(format)

    release_date = row.release_date
    if release_date:
        output.append(str(release_date))

    country = row.country
    if country:
        output.append(country)

    return ' '.join(output)


def db_summarise_row(row=None, id=None):
    if not row and id:
        row = db_fetch_row_by_discogs_id(id)

    output = []

    id = row.release_id
    output.append(f'release {id}')

    artist = row.artist
    if artist:
        output.append(artist)

    title = row.title
    if title:
        output.append(title)

    release_date = row.release_date
    if release_date:
        output.append(str(release_date))

    # country = row.country
    # if country:
    #     output.append(country)

    return ' '.join(output)


def mb_find_release(artist='', title='', discogs_id=0, catno=None, primary_type=None, country=None, barcode=None, format=None):
    """
        Search for a release

        All these fields are searchable.

        alias	        (part of) any alias attached to the release (diacritics are ignored)
        arid	        the MBID of any of the release artists
        artist	        (part of) the combined credited artist name for the release, including join phrases (e.g. "Artist X feat.")
        artistname	    (part of) the name of any of the release artists
        asin	        an Amazon ASIN for the release
        barcode	        the barcode for the release
        catno	        any catalog number for this release (insensitive to case, spaces, and separators)
        comment	        (part of) the release's disambiguation comment
        country	        the 2-letter code (ISO 3166-1 alpha-2) for any country the release was released in
        creditname	    (part of) the credited name of any of the release artists on this particular release
        date	        a release date for the release (e.g. "1980-01-22")
        discids	        the total number of disc IDs attached to all mediums on the release
        discidsmedium	the number of disc IDs attached to any one medium on the release
        format	        the format of any medium in the release (insensitive to case, spaces, and separators)
        laid	        the MBID of any of the release labels
        label	        (part of) the name of any of the release labels
        lang	        the ISO 639-3 code for the release language
        mediums	        the number of mediums on the release
        packaging	    the format of the release (insensitive to case, spaces, and separators)
        primarytype	    the primary type of the release group for this release
        quality	        the listed quality of the data for the release (2 for “high”, 1 for “normal”; cannot search for “low” at the moment; see the related bug report)
        reid	        the release's MBID
        release	        (part of) the release's title (diacritics are ignored)
        releaseaccent	(part of) the release's title (with the specified diacritics)
        rgid	        the MBID of the release group for this release
        script	        the ISO 15924 code for the release script
        secondarytype	any of the secondary types of the release group for this release
        status	        the status of the release
        tag	            (part of) a tag attached to the release
        tracks	        the total number of tracks on the release
        tracksmedium	the number of tracks on any one medium on the release
        type	        legacy release group type field that predates the ability to set multiple types (see calculation code)
    """
    if discogs_id > 0:
        discogs_url = f"https://www.discogs.com/release/{discogs_id}"
    else:
        discogs_url = None

    query = []

    if artist:
        query.append(f'artist:"{artist}"')

    if title:
        query.append(f'release:"{title}"')

    if catno:
        query.append(f'catno:"{catno}"')

    if primary_type:
        query.append(f'primarytype:"{primary_type}"')

    if country:
        query.append(f'country:{country}')

    if barcode:
        query.append(f'barcode:{barcode}')

    if format:
        query.append(f'format:{barcode}')

    all_results = []
    offset = 0
    batch_size = 25
    max_results = 200

    query_string = ' AND '.join(query)
    # print(f'query_string {query_string}')

    try:
        while len(all_results) < max_results:
            # Fetch results with pagination
            result = musicbrainzngs.search_releases(
                query=query_string, limit=batch_size, offset=offset)

            # Add new results
            releases = result.get('release-list', [])
            all_results.extend(releases)

            if releases:
                # search this batch
                # print(f'{len(releases)} release(s) from query {query_string}')

                for release in releases:
                    # print(mb_summarise_release(release))

                    if mb_match_discogs_release(release.get('id'), discogs_url=discogs_url):
                        print(f'✅ matched {mb_summarise_release(release)}')
                        return release

                    if len(releases) == 1:
                        # didn't match Discogs release URL, but there's only one result, so return it anyway
                        print(f'✅ matched {mb_summarise_release(release)}')
                        return release

                    # if catno and mb_match_catno(release, catno):
                    #     return release

                    # if len(releases) == 1:
                    #     # didn't match Discogs release URL, but there's only one result, so return it anyway
                    #     return release

            # Check if we've reached the end
            if len(releases) < batch_size:
                break

            # Move to the next page
            offset += batch_size

        # print(f'{len(all_results)} result(s) from query {query_string}')

        # for release in all_results:
        #     print(mb_summarise_release(release))

        return None

    except Exception as e:
        print(f"Error: {e}")
        # return []
        return None


def mb_find_releases(artist='', title='', catno=None, primary_type=None, country=None, barcode=None, format=None):
    """
        Search for any releases matching the query

        All these fields are searchable.

        alias	        (part of) any alias attached to the release (diacritics are ignored)
        arid	        the MBID of any of the release artists
        artist	        (part of) the combined credited artist name for the release, including join phrases (e.g. "Artist X feat.")
        artistname	    (part of) the name of any of the release artists
        asin	        an Amazon ASIN for the release
        barcode	        the barcode for the release
        catno	        any catalog number for this release (insensitive to case, spaces, and separators)
        comment	        (part of) the release's disambiguation comment
        country	        the 2-letter code (ISO 3166-1 alpha-2) for any country the release was released in
        creditname	    (part of) the credited name of any of the release artists on this particular release
        date	        a release date for the release (e.g. "1980-01-22")
        discids	        the total number of disc IDs attached to all mediums on the release
        discidsmedium	the number of disc IDs attached to any one medium on the release
        format	        the format of any medium in the release (insensitive to case, spaces, and separators)
        laid	        the MBID of any of the release labels
        label	        (part of) the name of any of the release labels
        lang	        the ISO 639-3 code for the release language
        mediums	        the number of mediums on the release
        packaging	    the format of the release (insensitive to case, spaces, and separators)
        primarytype	    the primary type of the release group for this release
        quality	        the listed quality of the data for the release (2 for “high”, 1 for “normal”; cannot search for “low” at the moment; see the related bug report)
        reid	        the release's MBID
        release	        (part of) the release's title (diacritics are ignored)
        releaseaccent	(part of) the release's title (with the specified diacritics)
        rgid	        the MBID of the release group for this release
        script	        the ISO 15924 code for the release script
        secondarytype	any of the secondary types of the release group for this release
        status	        the status of the release
        tag	            (part of) a tag attached to the release
        tracks	        the total number of tracks on the release
        tracksmedium	the number of tracks on any one medium on the release
        type	        legacy release group type field that predates the ability to set multiple types (see calculation code)
    """
    query = []

    if artist:
        query.append(f'artist:"{artist}"')

    if title:
        query.append(f'release:"{title}"')

    if catno:
        query.append(f'catno:"{catno}"')

    if primary_type:
        query.append(f'primarytype:"{primary_type}"')

    if country:
        query.append(f'country:{country}')

    if barcode:
        query.append(f'barcode:{barcode}')

    if format:
        query.append(f'format:{barcode}')

    all_results = []
    offset = 0
    batch_size = 25
    max_results = 200

    query_string = ' AND '.join(query)
    # print(f'query_string {query_string}')

    try:
        while len(all_results) < max_results:
            # Fetch results with pagination
            batch_releases = musicbrainzngs.search_releases(
                query=query_string,
                limit=batch_size,
                offset=offset)

            # Add new results
            releases = batch_releases.get('release-list', [])
            all_results.extend(releases)

            # Check if we've reached the end
            if len(releases) < batch_size:
                break

            # Move to the next page
            offset += batch_size

    except Exception as e:
        print(f"Error: {e}")
        all_results = []

    finally:
        return all_results


def on_this_day(today_str=''):

    with db_ops(DATABASE_NAME) as cur:

        cur.execute("""
            SELECT *
            FROM items
            WHERE release_date IS NOT NULL
            ORDER BY artist, release_date, title, release_id
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


def main():
    global args
    discogs_id = None
    mb_id = None

    if args.id:
        discogs_id = args.id
    elif args.mbid:
        mb_id = args.mbid

    open_db()

    # discogs_id = 5084926
    # update_table(discogs_id=2635834)
    # return

    if args.import_items:
        import_from_discogs(discogs_id=discogs_id, all_rows=args.all_items, new_rows=args.new_items)

    elif args.update_items:
        update_table(discogs_id=discogs_id, mb_id=mb_id,
                     all_rows=args.all_items, new_rows=args.new_items, reset_versions=args.reset)

    elif args.scrape:
        scrape_discogs(discogs_id=discogs_id, mb_id=mb_id,
                       all_rows=args.all_items, new_rows=args.new_items)

    elif args.match:
        match(args.match, args.release_date)

    elif args.random:
        random_selection()

    elif args.onthisday:
        on_this_day()

    elif args.status:
        status()


if __name__ == "__main__":
    global args

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

    scope_group = parser.add_mutually_exclusive_group(required=False)
    scope_group.add_argument('--new', required=False, action='store_true', dest="new_items", help='new items')
    scope_group.add_argument('--all', required=False, action='store_true', dest="all_items", help='all items')
    scope_group.add_argument('--id', required=False, help='restrict init or update to a specific Discogs id')
    scope_group.add_argument('--mbid', required=False, help='restrict init or update to a specific MusicBrainz id')

    parser.add_argument('--date', required=False, dest='release_date', help='set release date')

    id_group = parser.add_mutually_exclusive_group(required=False)
    id_group.add_argument('--reset', required=False, action='store_true', help='reset version number to force full update')

    parser.add_argument('--dry-run', '--read-only', required=False, action="store_true", help='dry run to test filtering')
    parser.add_argument('--verbose', required=False, action='store_true', help='verbose messages')
    # autopep8: on

    args = parser.parse_args()

    # args2 = vars(args)
    # print(args2)

    main()
