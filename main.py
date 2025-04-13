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

from modules.db import db_ops, fetch_discogs_release, update_discogs_artist, update_discogs_barcodes, update_discogs_catnos
from modules.db import update_discogs_country, update_discogs_format, update_mb_artist, update_discogs_title, update_mb_mbid, update_mb_primary_type, update_mb_title
from modules.db import update_discogs_release_date, insert_row, update_mb_sort_name, fetch_row_by_mb_id, initialize_db
from modules.db import db_summarise_row
from modules.discogs_importer import import_from_discogs_v2
from modules.discogs_importer import connect_to_discogs
from modules.discogs_importer import discogs_summarise_release
from modules.mb_matcher import match_discogs_against_mb
from modules.mb_matcher import disambiguator_score
from modules.mb_matcher import find_match_by_discogs_link
from modules.mb_matcher import mb_find_release_group_releases
from modules.mb_matcher import mb_find_releases
from modules.mb_matcher import mb_summarise_release
from modules.utils import convert_country_from_discogs_to_musicbrainz
from modules.config import AppConfig
from modules.utils import convert_format
from modules.utils import sanitise_identifier
from modules.utils import trim_if_ends_with_number_in_brackets
from modules.utils import earliest_date
from modules.utils import parse_and_humanize_date
from modules.utils import humanize_date_delta
from modules.utils import is_today_anniversary


logger = logging.getLogger(__name__)

V2 = True


def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)


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


# def get_earliest_release_date(artist, title):
#     try:
#         # Search for the recording by artist and title
#         result = musicbrainzngs.search_recordings(artist=artist, recording=title, limit=10)

#         if not result['recording-list']:
#             return f"No recordings found for {artist} - {title}"

#         earliest_date = None

#         # Iterate through recordings
#         for recording in result['recording-list']:
#             # Check for associated releases
#             if 'release-list' in recording:
#                 for release in recording['release-list']:
#                     # Get the release date if available
#                     date = release.get('date')
#                     if date:
#                         # Compare and store the earliest date
#                         if not earliest_date or date < earliest_date:
#                             earliest_date = date

#         return earliest_date if earliest_date else "No release date available"

#     except musicbrainzngs.WebServiceError as e:
#         return f"Error: {e}"


# def get_earliest_release_date2(artist, title):
#     try:
#         # Search for the recording (limit increased for better matching)
#         result = musicbrainzngs.search_recordings(artist=artist, recording=title, limit=50)

#         if not result['recording-list']:
#             return f"No recordings found for {artist} - {title}"

#         title_words = set(title.lower().split())
#         earliest_date = None

#         # Iterate through recordings
#         for recording in result['recording-list']:
#             # Ensure both artist and title match strictly
#             if 'release-list' in recording:
#                 # Check for strict title match by ensuring all title words are present
#                 recording_title = recording['title'].lower()
#                 if not title_words.issubset(recording_title.split()):
#                     continue

#                 # Ensure artist name appears in one of the associated artist credits
#                 if not any(artist.lower() in a['artist']['name'].lower() for a in recording['artist-credit']):
#                     continue

#                 # Compare release dates
#                 for release in recording['release-list']:
#                     date = release.get('date')
#                     if date and (not earliest_date or date < earliest_date):
#                         earliest_date = date

#         return earliest_date if earliest_date else "No release date available"

#     except musicbrainzngs.WebServiceError as e:
#         return f"Error: {e}"

#     # return bool(re.search(pattern, s))


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


def scrape_row(discogs_client, discogs_release=None, row=None, discogs_id=0):

    if row is None and discogs_id:
        fetch_discogs_release(discogs_id)

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
            update_discogs_release_date(row.release_id, release_date, config)


def update_row(discogs_client, discogs_release=None, discogs_id=None, mb_id=None, version_id=0, config=None):

    if not discogs_id and discogs_release:
        discogs_id = discogs_release.id

    if not discogs_release and discogs_id:
        discogs_release = discogs_client.release(discogs_id)

    print(f'⚙️ {discogs_summarise_release(release=discogs_release)}')

    mb_country = convert_country_from_discogs_to_musicbrainz(discogs_release.country)

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
        mb_release_group, mb_release, release_date = mb_find_release_group_releases(
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
        print(f'❌ no match for {discogs_summarise_release(release=discogs_release)}')
        version_id = 0

    row = fetch_discogs_release(discogs_id)
    if row:
        # update any changed fields in the minimal part of the row
        if not release_date:
            release_date = earliest_date(row.release_date, discogs_release.year)

        update_discogs_artist(discogs_release.id, artist_name, row.artist, config=config)
        update_discogs_title(discogs_release.id, release_title, row.title, config=config)
        update_discogs_format(discogs_release.id, format, row.format, config=config)
        update_mb_primary_type(discogs_release.id, mb_primary_type,
                               row.mb_primary_type, config=config)
        update_discogs_country(discogs_release.id, mb_country, row.country, config=config)
        update_discogs_release_date(discogs_release.id, release_date,
                                    row.release_date, config=config)

    else:

        # no row exists yet - create a minimal row
        print(f'💾 {discogs_summarise_release(release=discogs_release)}')

        insert_row(release_id=discogs_release.id,
                   artist=artist_name,
                   title=release_title,
                   format=format,
                   mb_primary_type=mb_primary_type,
                   release_date=discogs_release.year,
                   country=mb_country,
                   version_id=version_id,
                   config=config)

        row = fetch_discogs_release(discogs_id)

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

        update_mb_mbid(row.release_id, mb_id, row.mb_id, config=config)
        update_mb_artist(row.release_id, mb_artist, row.mb_artist, config=config)
        update_mb_title(row.release_id, mb_title, row.mb_title, config=config)

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
    row = fetch_discogs_release(discogs_id)

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

    update_mb_mbid(release_id, mb_id, row.mb_id, config=config)
    update_mb_artist(release_id, mb_artist, row.mb_artist, config=config)
    update_mb_title(release_id, mb_title, row.mb_title, config=config)
    update_mb_sort_name(release_id, mb_sort_name, row.sort_name, config=config)
    update_discogs_release_date(release_id, release_date, row.release_date, config=config)
    update_discogs_format(discogs_release.id, format, row.format, config=config)
    update_mb_primary_type(discogs_release.id, mb_primary_type,
                           row.mb_primary_type, config=config)


def import_from_discogs(config=None):

    if config.discogs_id == None and config.all_items == False and config.new_items == False:
        config.new_items = True

    musicbrainz.set_useragent(config.user_agent, '0.1', 'steve.powell@outlook.com')

    try:
        musicbrainz.auth(config.username, config.password)

    except HTTPError:
        logging.error("Unable to authenticate to Discogs.")
        sys.exit(1)

    except Exception as e:
        logging.error(f'MusicBrainz authentication error {e}')
        sys.exit(1)

    musicbrainzngs.set_rate_limit(1, 1)
    # musicbrainzngs.set_format(fmt='json')

    discogs_client, discogs_access_token, discogs_access_secret = connect_to_discogs(config)

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

    with db_ops() as cur:

        # get the highest version number
        cur.execute("""
                SELECT MAX(version_id) as max_version_id, MIN(version_id) as min_version_id FROM items
            """)
        row = cur.fetchone()

        max_version_id = row.max_version_id if row.max_version_id else 0
        min_version_id = row.min_version_id if row.min_version_id else 0

    if config.discogs_id:
        if config.all_items:
            update_row(discogs_client=discogs_client,
                       discogs_release=discogs_item.release, version_id=max_version_id)
            return

        row = fetch_discogs_release(config.discogs_id)
        if row is not None:
            print(f'⏭️ {db_summarise_row(row=row)}')
            return

        update_row(discogs_client=discogs_client,
                   discogs_release=discogs_item.release, version_id=max_version_id)

    else:
        folder = discogs_user.collection_folders[0]

        print(f'number of items in all collections: {len(folder.releases)}')

        for discogs_item in discogs_user.collection_folders[0].releases:

            if config.all_items:
                update_row(discogs_client=discogs_client,
                           discogs_release=discogs_item.release, version_id=max_version_id)
                continue

            row = fetch_discogs_release(discogs_item.id)
            if row is not None:
                print(f'⏭️ {db_summarise_row(row=row)}')
                continue

            update_row(discogs_client=discogs_client,
                       discogs_release=discogs_item.release, version_id=max_version_id)


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
            FROM items
            WHERE artist LIKE '%{config.match}%'
            OR mb_artist LIKE '%{config.match}%'
            OR title LIKE '%{config.match}%'
            OR mb_title LIKE '%{config.match}%'
            OR sort_name LIKE '%{config.match}%'
            ORDER BY artist, release_date, title, release_id
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
                    FROM items
                    WHERE artist LIKE '%{config.match}%'
                    OR mb_artist LIKE '%{config.match}%'
                    OR title LIKE '%{config.match}%'
                    OR mb_title LIKE '%{config.match}%'
                    OR sort_name LIKE '%{config.match}%'
                    ORDER BY artist, release_date, title, release_id
                """)

                row = cur.fetchone()

                update_discogs_release_date(row.release_id, set_date, row.release_date, config)
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
                        f'{row.release_id:>8} {fls(title, title_len)} {fls(format, format_len)} {fls(release_date, release_date_len)}')


def match_v2(config=None):

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

                update_discogs_release_date(row.discogs_id, set_date, row.release_date, config)
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
                        f'{row.release_id:>8} {fls(title, title_len)} {fls(format, format_len)} {fls(release_date, release_date_len)}')


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


def random_selection(config):

    with db_ops() as cur:

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


def output_row_v2(row):
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


def update_table(config=None):

    if config.discogs_id == None and config.mbid == None and config.all_items == False and config.new_items == False:
        config.new_items = True

    # TODO set these from settings file
    musicbrainz.set_useragent(config.user_agent, '0.1', 'steve.powell@outlook.com')

    try:
        musicbrainz.auth(config.username, config.password)

    except HTTPError:
        logging.error("Unable to authenticate to Discogs.")
        sys.exit(1)

    except Exception as e:
        logging.error(f'MusicBrainz authentication error {e}')
        sys.exit(1)

    musicbrainzngs.set_rate_limit(1, 1)

    discogs_client, discogs_access_token, discogs_access_secret = connect_to_discogs(config)

    # get the highest version number, which will be used for all updates
    max_version_id = 0
    min_version_id = 0

    if config.reset:
        # reset the row versions, so that all will be processed
        with db_ops() as cur:
            cur.execute("""
                    UPDATE items
                    SET version_id = 0
                """)
    else:
        with db_ops() as cur:

            # get the highest version number
            cur.execute("""
                    SELECT MAX(version_id) as max_version_id, MIN(version_id) as min_version_id FROM items
                """)
            row = cur.fetchone()

            max_version_id = row.max_version_id if row.max_version_id else 0
            min_version_id = row.min_version_id if row.min_version_id else 0

    if config.discogs_id:
        row = fetch_discogs_release(config.discogs_id)
        if row is not None:
            update_row(discogs_client, discogs_id=int(config.discogs_id),
                       version_id=max_version_id, config=config)
        else:
            print(f'discogs_id {config.discogs_id} not found')

    else:

        with db_ops() as cur:

            if config.all_items == False and max_version_id > 0 and max_version_id > min_version_id:
                # only process items below the maximum version
                cur.execute(f"""
                        SELECT * FROM items WHERE version_id IS NULL OR version_id < {max_version_id}
                    """)
                rows = cur.fetchall()

                for row in rows:
                    update_row(discogs_client, discogs_id=row.release_id,
                               version_id=max_version_id, config=config)
            else:
                # process all rows
                cur.execute("""
                        SELECT * FROM items
                    """)

                rows = cur.fetchall()

                for row in rows:
                    update_row(discogs_client, discogs_id=row.release_id,
                               version_id=max_version_id+1, config=config)


def scrape_discogs(discogs_id, mb_id=None, config=None):

    discogs_client, discogs_access_token, discogs_access_secret = connect_to_discogs(config)

    if discogs_id:
        row = fetch_discogs_release(discogs_id)
        if row is not None:
            print(f'update {db_summarise_row(row=row)}')
            scrape_row(discogs_client=discogs_client, row=row, discogs_id=int(discogs_id))
        else:
            print(f'discogs_id {discogs_id} not found')

    else:

        with db_ops() as cur:

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
                scrape_row(discogs_client=discogs_client, row=row, config=config)


def db_formatted_row(discogs_id):
    row = fetch_discogs_release(discogs_id)

    output = []

    id = row.discogs_id
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


def on_this_day_v2(today_str='', config=None):

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
                output_row_v2(row)
                print()


def on_this_day(today_str='', config=None):

    with db_ops() as cur:

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


def main(config):

    initialize_db()

    # discogs_id = 5084926
    # update_table(discogs_id=2635834)
    # return

    if V2:

        if args.import_items:
            import_from_discogs_v2(config=config)
            match_discogs_against_mb(config=config)

        elif args.update_items:
            match_discogs_against_mb(config=config)

        elif args.scrape:
            scrape_discogs(config=config)

        elif args.match:
            match_v2(config=config)

        elif args.random:
            random_selection(config=config)

        elif args.onthisday:
            on_this_day_v2(config=config)

        elif args.status:
            status(config=config)

    else:

        if args.import_items:
            import_from_discogs(config=config)

        elif args.update_items:
            update_table(config=config)

        elif args.scrape:
            scrape_discogs(config=config)

        elif args.match:
            match(config=config)

        elif args.random:
            random_selection(config=config)

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
