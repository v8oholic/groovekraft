#!/usr/bin/env python3

# Discogs API logic
import discogs_client
from discogs_client.exceptions import HTTPError

import modules.db_discogs as db_discogs

from .utils import trim_if_ends_with_number_in_brackets, sanitise_identifier, normalize_country_name, earliest_date

import logging

logger = logging.getLogger(__name__)


def connect_to_discogs(config):

    authenticated = False

    if config.oauth_token and config.oauth_token_secret:
        try:
            client = discogs_client.Client(
                config.user_agent,
                consumer_key=config.consumer_key,
                consumer_secret=config.consumer_secret,
                token=config.oauth_token,
                secret=config.oauth_token_secret
            )
            access_token = config.oauth_token
            access_secret = config.oauth_token_secret
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
            raise Exception("Unable to authenticate to Discogs.")

    return client, access_token, access_secret


def normalize_artist(name):
    artist = trim_if_ends_with_number_in_brackets(name)
    if artist == 'Various':
        artist = 'Various Artists'
    return artist


def normalize_title(title):
    return title


def normalize_format(format0):
    return format0['name'] + ': ' + ', '.join(format0['descriptions'])


def normalize_country(country):
    return normalize_country_name(country)


def normalize_barcodes(identifiers):
    barcodes = []
    for identifier in identifiers:
        if identifier['type'].casefold() == 'barcode':
            barcodes.append(sanitise_identifier(identifier['value']))
    return ', '.join(sorted(set(barcodes)))


def normalize_catnos(labels):
    catnos_set = set([sanitise_identifier(x.data['catno']) for x in labels])
    return ', '.join(sorted(catnos_set))


def discogs_summarise_release(release=None, id=None, discogs_client=None):
    if not release and id and discogs_client:
        release = discogs_client.release(id)

    output = []

    id = release.id
    output.append(f'Discogs {id}')

    artist = trim_if_ends_with_number_in_brackets(release.artists[0].name)
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


def import_from_discogs(config=None):

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

    releases = discogs_user.collection_folders[0].releases
    print(f'number of items in all collections: {len(releases)}')

    for index, release in enumerate(releases):

        release = discogs_client.release(release.id)
        print(f'⚙️ {index+1}/{len(releases)} {discogs_summarise_release(release=release)}')

        artist = normalize_artist(release.artists[0].name)
        title = normalize_title(release.title)
        format = normalize_format(release.formats[0])
        country = normalize_country(release.country)
        barcodes = normalize_barcodes(release.fetch('identifiers'))
        catnos = normalize_catnos(release.labels)
        year = release.year if release.year else None
        master_id = release.master.id if release.master is not None else 0

        row = db_discogs.fetch_row(release.id)

        if row:
            release_date = earliest_date(row.release_date, year)

            db_discogs.set_artist(release.id, artist)
            db_discogs.set_title(release.id, title)
            db_discogs.set_format(release.id, format)
            db_discogs.set_country(release.id, country)
            db_discogs.set_barcodes(release.id, barcodes if barcodes else None)
            db_discogs.set_catnos(release.id, catnos if catnos else None)
            db_discogs.set_year(release.id, year)
            db_discogs.set_master_id(release.id, master_id)
            db_discogs.set_release_date(release.id, release_date)
            if not row.sort_name:
                db_discogs.set_sort_name(release.id, artist)

        else:

            db_discogs.insert_row(
                discogs_id=release.id,
                artist=artist,
                title=title,
                format=format,
                country=country,
                year=year,
                barcodes=barcodes if barcodes else None,
                catnos=catnos if catnos else None,
                master_id=master_id,
                sort_name=artist,
                release_date=earliest_date(None, year))
