#!/usr/bin/env python3

# CLI entry point

import discogs_client
import sys
import argparse
import signal
from dateutil import parser
import logging
import configparser

from modules import db_discogs
from modules import utils
from modules import mb_matcher
from modules import discogs_importer
from modules import db
from modules import scraper
from modules.config import AppConfig

logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)


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


def missing(config):

    with db.context_manager() as cur:

        # LEFT JOIN mb_matches m ON d.discogs_id = m.discogs_id

        # construct query
        query = []

        find_string = config.find

        query.append('SELECT d.artist, d.title, d.format, d.release_date, d.discogs_id')
        query.append('FROM discogs_releases d')
        query.append('LEFT JOIN mb_matches m USING(discogs_id)')
        query.append('WHERE m.mbid IS NULL')
        if find_string:
            query.append(f'AND (d.artist LIKE "%{find_string}%"')
            query.append(f'OR d.title LIKE "%{find_string}%"')
            query.append(f'OR d.sort_name LIKE "%{find_string}%")')
        query.append('ORDER BY d.artist, d.release_date, d.title, d.discogs_id')

        query_string = ' '.join(query)

        cur.execute(query_string)

        rows = cur.fetchall()
        if len(rows) == 0:
            print('no matching items')
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

                format = row.format.split(',')[0] if row.format else ''
                release_date = row.release_date if row.release_date else ''

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
                        print()
                        print(artist)
                        print('='*len(artist))

                    print(
                        f'{row.discogs_id:>8} {fls(title, title_len)} {fls(format, format_len)} {fls(release_date, release_date_len)}')

        print()
        print(f'{len(rows)} rows')
        print()


def match(config):

    with db.context_manager() as cur:

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

                format = row.format.split(',')[0] if row.format else ''
                release_date = row.release_date if row.release_date else ''

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
                        print()
                        print(artist)
                        print('='*len(artist))

                    print(
                        f'{row.discogs_id:>8} {fls(title, title_len)} {fls(format, format_len)} {fls(release_date, release_date_len)}')


def status(config):

    def output_nvp(label, value):
        print(f'{fls(label, 45)}: {value}')

    def output_nvp_percentage(label, count, total):
        value = f'{count} ({round(count * 100 / total, 1)}%)'
        print(f'{fls(label, 45)}: {value}')

    discogs_client, discogs_access_token, discogs_access_secret = discogs_importer.connect_to_discogs(
        config)

    # fetch the identity object for the current logged in user.
    discogs_user = discogs_client.identity()

    output_nvp("Discogs username", discogs_user.username)
    output_nvp("MusicBrainz username", config.username)

    folder = discogs_user.collection_folders[0]

    output_nvp('releases on Discogs', len(folder.releases))
    discogs_count = len(folder.releases)

    with db.context_manager() as cur:

        cur.execute("""
            SELECT COUNT(*) as count
            FROM discogs_releases
        """)
        row = cur.fetchone()

        localdb_count = row.count
        output_nvp_percentage('releasees in local database', localdb_count, discogs_count)

        query = []

        query.append('SELECT COUNT(d.discogs_id) as count')
        query.append('FROM discogs_releases d')
        query.append('LEFT JOIN mb_matches m USING(discogs_id)')
        query.append('WHERE m.mbid IS NOT NULL')

        cur.execute(' '.join(query))
        row = cur.fetchone()

        output_nvp_percentage('releases matched in MusicBrainz', row.count, localdb_count)

        query = []

        query.append('SELECT COUNT(d.discogs_id) as count')
        query.append('FROM discogs_releases d')
        query.append('LEFT JOIN mb_matches m USING(discogs_id)')
        query.append('WHERE m.mbid IS NULL')

        cur.execute(' '.join(query))
        row = cur.fetchone()

        output_nvp_percentage('releases not matched in MusicBrainz', row.count, localdb_count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM discogs_releases
            WHERE release_date IS NULL
        """)
        row = cur.fetchone()

        output_nvp_percentage('no release date', row.count, localdb_count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM discogs_releases
            WHERE LENGTH(release_date) = 4
        """)
        row = cur.fetchone()

        output_nvp_percentage('just release year', row.count, localdb_count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM discogs_releases
            WHERE LENGTH(release_date) = 7
        """)
        row = cur.fetchone()

        output_nvp_percentage('just month and release year', row.count, localdb_count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM discogs_releases
            WHERE LENGTH(release_date) = 10
        """)
        row = cur.fetchone()

        output_nvp_percentage('full release date', row.count, localdb_count)


def random_selection():

    with db.context_manager() as cur:

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
        delta = utils.humanize_date_delta(row.release_date)

    print(f'released        : {delta}')
    print(f'artist          : {row.artist}')
    print(f'title           : {row.title}')
    print(f'format          : {row.format}')
    print(f'discogs_id      : {row.discogs_id}')
    if row.release_date:
        # print(f'release_date    : {row.release_date}')
        print(f'release_date    : {utils.parse_and_humanize_date(row.release_date)}')


def on_this_day(today_str='', config=None):

    with db.context_manager() as cur:

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
                    if not utils.is_today_anniversary(row.release_date, today_str):
                        continue

                else:
                    if not utils.is_today_anniversary(row.release_date):
                        continue

                print()
                output_row(row)
                print()


def import_old_release_dates(config=None):

    rows = db_discogs.fetch_discogs_release_rows()

    for index, row in enumerate(rows):

        print(f'⚙️ {index+1}/{len(rows)} {db.db_summarise_row(row.discogs_id)}')

        # fetch the old row
        old_item = db_discogs.fetch_row_by_discogs_id(row.discogs_id)
        if old_item:
            # release_date = earliest_date(old_item.release_date, row.release_date)
            # set_release_date(row.discogs_id, release_date)

            db_discogs.set_release_date(row.discogs_id, old_item.release_date, True)


def main(config):

    db.initialize_db()

    # discogs_id = 5084926
    # update_table(discogs_id=2635834)
    # return

    if args.import_items:
        discogs_importer.import_from_discogs(config=config)
        mb_matcher.match_discogs_against_mb(config=config)
        # import_old_release_dates(config=config)

    elif args.update_items:
        mb_matcher.match_discogs_against_mb(config=config)

    elif args.scrape:
        scraper.scrape_discogs(config)

    elif args.missing:
        missing(config=config)

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
    main_group.add_argument('--missing', required=False, action='store_true', help='show unmatched releases')
    main_group.add_argument('--match', required=False, help='find matching text in database')
    main_group.add_argument('--status', required=False, action='store_true', help='report status of database')

    scope_group = parser.add_mutually_exclusive_group(required=False)
    scope_group.add_argument('--find', required=False, help='find matching text in database')
    scope_group.add_argument('--begin', type=int, required=False, default=0, help='begin at discogs_id')
    scope_group.add_argument('--id', type=int, required=False, default=0, help='only a specific Discogs id')
    scope_group.add_argument('--unmatched', required=False, action='store_true', help='only unmatched items')
    # scope_group.add_argument('--all', required=False, action='store_true', dest="all_items", help='all items')
    # scope_group.add_argument('--discogs_id', required=False, help='restrict init or update to a specific Discogs id')
    # scope_group.add_argument('--mbid', required=False, help='restrict init or update to a specific MusicBrainz id')

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
