#!/usr/bin/env python3

import os
import requests
# Discogs API logic
import discogs_client
from discogs_client.exceptions import HTTPError

from discogs import db_discogs
from modules import utils
from discogs.discogs_oauth_gui import prompt_oauth_verifier_gui

import logging

logger = logging.getLogger(__name__)

DISCOGS_CONSUMER_KEY = 'yEJrrZEZrExGHEPjNQca'
DISCOGS_CONSUMER_SECRET = 'isFjruJTfmmXFXiaywRqCUSkIGwHlHKn'


def connect_to_discogs(config):

    token_row = db_discogs.get_oauth_tokens()
    if token_row:
        oauth_token, oauth_token_secret = token_row
    else:
        oauth_token = oauth_token_secret = None

    authenticated = False

    if oauth_token and oauth_token_secret:
        try:
            client = discogs_client.Client(
                config.user_agent,
                consumer_key=config.consumer_key,
                consumer_secret=config.consumer_secret,
                token=oauth_token,
                secret=oauth_token_secret
            )
            access_token = oauth_token
            access_secret = oauth_token_secret
            authenticated = True

        except HTTPError:
            logger.error("Unable to authenticate.")
            access_token = None
            access_secret = None
            authenticated = False

    if not authenticated:

        # instantiate discogs_client object
        client = discogs_client.Client(user_agent=config.user_agent)

        # prepare the client with our API consumer data
        client.set_consumer_key(config.consumer_key, config.consumer_secret)
        token, secret, url = client.get_authorize_url()

        logger.debug(" == Request Token == ")
        logger.debug(f"    * oauth_token        = {token}")
        logger.debug(f"    * oauth_token_secret = {secret}")
        logger.debug(f"    * authorization URL  = {url}")

        try:
            oauth_verifier = prompt_oauth_verifier_gui(url)
            access_token, access_secret = client.get_access_token(oauth_verifier)
            db_discogs.set_oauth_tokens(access_token, access_secret)
            authenticated = True
        except Exception as e:
            raise Exception(f"Unable to authenticate to Discogs: {e}")

    return client, access_token, access_secret


def normalize_artist(name):
    artist = utils.trim_if_ends_with_number_in_brackets(name)
    if artist == 'Various':
        artist = 'Various Artists'
    return artist


def normalize_title(title):
    return title


def normalize_format(format0):
    return format0['name'] + ': ' + ', '.join(format0['descriptions'])


def normalize_country(country):
    return utils.normalize_country_name(country)


def normalize_barcodes(identifiers):
    barcodes = []
    for identifier in identifiers:
        if identifier['type'].casefold() == 'barcode':
            barcodes.append(utils.sanitise_identifier(identifier['value']))
    return ', '.join(sorted(set(barcodes)))


def normalize_catnos(labels):
    catnos_set = set([x.data['catno'] for x in labels])
    return ', '.join(sorted(catnos_set))


def discogs_summarise_release(release=None, id=None, discogs_client=None):
    if not release and id and discogs_client:
        release = discogs_client.release(id)

    output = []

    id = release.id
    output.append(f'Discogs {id}')

    artist = utils.trim_if_ends_with_number_in_brackets(release.artists[0].name)
    if artist:
        output.append(artist)

    title = release.title
    if title:
        output.append(title)

    year = release.year
    if year:
        output.append(str(year))

    country = release.country
    if country:
        output.append(country)

    first_format = release.formats[0]
    format = first_format.get('name') + ' ' + ' '.join(first_format.get('descriptions'))

    if format:
        output.append(format)

    return ' '.join(output)


def import_from_discogs(discogs_client, images_folder, callback=print, should_cancel=lambda: False, progress_callback=lambda pct: None):
    os.makedirs(images_folder, exist_ok=True)
    discogs_user = discogs_client.identity()
    releases = discogs_user.collection_folders[0].releases

    if not releases:
        callback("No releases found in the Discogs collection.")
        progress_callback(100)
        return

    callback(f'Number of items in all collections: {len(releases)}')

    imported = 0
    updated = 0
    total_releases = len(releases)

    for index, release_summary in enumerate(releases, start=1):
        if should_cancel():
            callback("Import cancelled.")
            return

        percent = int((index / total_releases) * 100)
        progress_callback(percent)

        release = discogs_client.release(release_summary.id)
        callback(f'⚙️ {index}/{total_releases} {discogs_summarise_release(release=release)}')

        artist = normalize_artist(release.artists[0].name)
        title = normalize_title(release.title)
        format = normalize_format(release.formats[0])
        country = normalize_country(release.country)
        barcodes = normalize_barcodes(release.fetch('identifiers'))
        catnos = normalize_catnos(release.labels)
        year = release.year or None
        master_id = release.master.id if release.master else 0

        row = db_discogs.fetch_row(release.id)
        release_date = utils.earliest_date(row.release_date if row else None, year)

        if row:
            db_discogs.set_artist(release.id, artist, callback=callback)
            db_discogs.set_title(release.id, title, callback=callback)
            db_discogs.set_format(release.id, format, callback=callback)
            db_discogs.set_country(release.id, country, callback=callback)
            db_discogs.set_barcodes(release.id, barcodes or None, callback=callback)
            db_discogs.set_catnos(release.id, catnos or None, callback=callback)
            db_discogs.set_year(release.id, year, callback=callback)
            db_discogs.set_master_id(release.id, master_id, callback=callback)
            db_discogs.set_release_date(release.id, release_date, callback=callback)

            if not row.sort_name:
                db_discogs.set_sort_name(release.id, artist, callback=callback)

            updated += 1
        else:
            db_discogs.insert_row(
                discogs_id=release.id,
                artist=artist,
                title=title,
                format=format,
                country=country,
                year=year,
                barcodes=barcodes or None,
                catnos=catnos or None,
                master_id=master_id,
                sort_name=artist,
                release_date=release_date
            )
            imported += 1

        primary_image_url = None
        if hasattr(release, 'images') and release.images and release.images[0].get('type', '').lower() == 'primary':
            primary_image_url = release.images[0]['uri']

        existing_uri = getattr(row, 'primary_image_uri', None) if row else None

        if primary_image_url and primary_image_url != existing_uri:
            headers = {"User-Agent": discogs_client.user_agent}
            image_path = os.path.join(images_folder, f"{release.id}.jpg")

            for attempt in range(2):  # Try up to 2 times
                try:
                    response = requests.get(primary_image_url, headers=headers, timeout=10)
                    response.raise_for_status()
                    with open(image_path, "wb") as f:
                        f.write(response.content)
                    if row:
                        db_discogs.set_primary_image_uri(
                            release.id, primary_image_url, callback=callback)
                    break  # Success, break out of retry loop
                except Exception as e:
                    if attempt == 1:
                        callback(f"Warning: Failed to download image for release {release.id}: {e}")

    callback(f'🏁 {imported} new items imported, {updated} items updated.')
