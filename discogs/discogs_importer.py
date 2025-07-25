#!/usr/bin/env python3

import os
import requests
# Discogs API logic
import discogs_client
from discogs_client.exceptions import HTTPError
import json


from discogs.db_discogs import (
    get_oauth_tokens, set_oauth_tokens, fetch_row, set_artist, set_title, set_format,
    set_country, set_barcodes, set_catnos, set_year, set_master_id, set_release_date,
    set_sort_name, insert_row, set_primary_image_uri, get_all_discogs_ids, fetch_all_rows, delete_discogs_release_row
)
from modules import utils
from discogs.discogs_oauth_gui import prompt_oauth_verifier_gui
from modules.config import DISCOGS_CONSUMER_KEY, DISCOGS_CONSUMER_SECRET, GROOVEKRAFT_USER_AGENT
import logging
from modules.config import AppConfig

logger = logging.getLogger(__name__)


def connect_to_discogs(db_path):

    token_row = get_oauth_tokens(db_path)
    if token_row:
        oauth_token, oauth_token_secret = token_row
    else:
        oauth_token = oauth_token_secret = None

    authenticated = False

    if oauth_token and oauth_token_secret:
        try:
            client = discogs_client.Client(
                GROOVEKRAFT_USER_AGENT,
                consumer_key=DISCOGS_CONSUMER_KEY,
                consumer_secret=DISCOGS_CONSUMER_SECRET,
                token=oauth_token,
                secret=oauth_token_secret
            )
            # Attempt to validate the token immediately
            client.identity()
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
        client = discogs_client.Client(user_agent=GROOVEKRAFT_USER_AGENT)

        # prepare the client with our API consumer data
        client.set_consumer_key(DISCOGS_CONSUMER_KEY, DISCOGS_CONSUMER_SECRET)
        token, secret, url = client.get_authorize_url()

        logger.debug(" == Request Token == ")
        logger.debug(f"    * oauth_token        = {token}")
        logger.debug(f"    * oauth_token_secret = {secret}")
        logger.debug(f"    * authorization URL  = {url}")

        try:
            oauth_verifier = prompt_oauth_verifier_gui(url)
            access_token, access_secret = client.get_access_token(oauth_verifier)
            set_oauth_tokens(db_path, access_token, access_secret)
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


def likely_match(orphan, candidate):
    # Simple heuristic: match if catnos or barcodes overlap, or artist/title/year match strongly
    if orphan.catnos and candidate.catnos and set(orphan.catnos.split(', ')) & set(candidate.catnos.split(', ')):
        return True
    if orphan.barcodes and candidate.barcodes and set(orphan.barcodes.split(', ')) & set(candidate.barcodes.split(', ')):
        return True
    if orphan.artist == candidate.artist and orphan.title == candidate.title and orphan.year == candidate.year:
        return True
    return False


def import_from_discogs(discogs_client, cfg: AppConfig, callback=print, should_cancel=lambda: False, progress_callback=lambda pct: None):

    if __debug__:
        import debugpy
        debugpy.breakpoint()

    db_path = cfg.db_path
    images_folder = cfg.images_folder

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
    failed = 0
    total_releases = len(releases)

    imported_ids = set()

    for index, release_summary in enumerate(releases, start=1):
        if should_cancel():
            callback("Import cancelled.")
            return

        percent = int((index / total_releases) * 100)
        progress_callback(percent)

        try:
            release = discogs_client.release(release_summary.id)
        except json.decoder.JSONDecodeError as e:
            callback(f"❌ JSON error fetching release {release_summary.id}: {e}")
            failed += 1
            continue
        except Exception as e:
            callback(f"❌ Error fetching release {release_summary.id}: {e}")
            failed += 1
            continue

        release_date = release.fetch('released') or None

        if isinstance(release_date, str) and release_date.endswith('-00'):
            release_date = release_date[:len(release_date)-3]

        callback(f'⚙️ {index}/{total_releases} {discogs_summarise_release(release=release)}')

        artist = normalize_artist(release.artists[0].name)
        title = normalize_title(release.title)
        format = normalize_format(release.formats[0])
        country = normalize_country(release.country)
        barcodes = normalize_barcodes(release.fetch('identifiers'))
        catnos = normalize_catnos(release.labels)
        year = release.year or None
        master_id = release.master.id if release.master else 0

        row = fetch_row(db_path, release.id)
        release_date = utils.earliest_date(row.release_date if row else None, release_date)

        if row:

            set_artist(db_path, release.id, artist, callback=callback)
            set_title(db_path, release.id, title, callback=callback)
            set_format(db_path, release.id, format, callback=callback)
            set_country(db_path, release.id, country, callback=callback)
            set_barcodes(db_path, release.id, barcodes or None, callback=callback)
            set_catnos(db_path, release.id, catnos or None, callback=callback)
            set_year(db_path, release.id, year, callback=callback)
            set_master_id(db_path, release.id, master_id, callback=callback)
            # Only update the release date if the release_date_locked flag is not set
            if getattr(row, "release_date_locked", None) in (None, 0):
                set_release_date(db_path, release.id, release_date, callback=callback)

            if not row.sort_name:
                set_sort_name(db_path, release.id, artist, callback=callback)

            updated += 1
        else:
            insert_row(
                db_path,
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

        imported_ids.add(release.id)

        primary_image_url = None
        if hasattr(release, 'images') and release.images:
            for img in release.images:
                if img.get('type', '').lower() == 'primary':
                    primary_image_url = img['uri']
                    break
            if not primary_image_url:
                primary_image_url = release.images[0]['uri']
                callback(
                    f"⚠️ No primary image marked for release {release.id}, using first available image.")

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
                        set_primary_image_uri(
                            db_path,
                            release.id,
                            primary_image_url,
                            callback=callback)
                    break  # Success, break out of retry loop
                except Exception as e:
                    if attempt == 1:
                        callback(f"Warning: Failed to download image for release {release.id}: {e}")

    # Identify orphans and possible replacements
    all_db_ids = get_all_discogs_ids(db_path)
    orphans = set(all_db_ids) - imported_ids

    if orphans:
        callback(f"Found {len(orphans)} orphaned releases in database not in latest import.")
        all_rows = fetch_all_rows(db_path)
        id_to_row = {row.discogs_id: row for row in all_rows}

        for orphan_id in orphans:
            orphan = id_to_row.get(orphan_id)
            if not orphan:
                continue
            replacements = []
            for candidate_id in imported_ids:
                candidate = id_to_row.get(candidate_id)
                if not candidate:
                    continue
                if likely_match(orphan, candidate):
                    replacements.append(candidate)

            if replacements:
                locked_date_copied = False
                for replacement in replacements:
                    # Carry over locked release date if orphan has it locked and replacement doesn't
                    if getattr(orphan, "release_date_locked", None) in (1, True) and getattr(replacement, "release_date_locked", None) in (None, 0):
                        set_release_date(db_path, replacement.discogs_id,
                                         orphan.release_date, callback=callback)
                        callback(
                            f"Carried over locked release date from orphan {orphan_id} to replacement {replacement.discogs_id}")
                        locked_date_copied = True
                callback(
                    f"Orphan release {orphan_id} has {len(replacements)} possible replacements.")
                # Delete the orphan if replacements were found and (date not locked or locked date copied)
                if getattr(orphan, "release_date_locked", None) not in (1, True) or locked_date_copied:
                    delete_discogs_release_row(db_path, orphan_id)
                    callback(f"Deleted orphan release {orphan_id} from database.")
            else:
                callback(f"No replacements found for orphan release {orphan_id}.")

    callback(f'🏁 {imported} new items imported, {updated} items updated, {failed} releases failed.')
