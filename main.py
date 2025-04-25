#!/usr/bin/env python3

# CLI entry point

import discogs_client
import sys
import argparse
import signal
from dateutil import parser
import logging
import configparser

from discogs import db_discogs
from mb_modules import mb_matcher
from discogs import discogs_importer
from modules import db
from modules import scraper
from modules import reporting
from modules.config import AppConfig
from modules.gui import run_gui

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


def main(config: AppConfig):

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
        reporting.missing(config=config)

    elif args.list_by_date:
        reporting.list(config=config, order_by_date=False)

    elif args.list_by_name:
        reporting.list(config=config, order_by_date=True)

    elif args.random:
        reporting.random_selection(config=config)

    elif args.onthisday:
        reporting.on_this_day(config=config)

    elif args.status:
        reporting.status(config=config)


if __name__ == "__main__":

    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(
        description="Music Collection Importer",
        epilog="Import from Discogs skips previously imported releases. Update from MusicBrainz processes only previously imported releases."
    )

    # autopep8: off
    parser.add_argument('--init', required=False, action='store_true', help='initialise database')
    parser.add_argument('--gui', required=False, action='store_true', help='launch experimental PyQt6 GUI')

    main_group = parser.add_mutually_exclusive_group(required=False)
    main_group.add_argument('--import', required=False, action='store_true', dest='import_items', help='import items from Discogs')
    main_group.add_argument('--update', required=False, action='store_true', dest='update_items', help='update items using MusicBrainz')
    main_group.add_argument('--scrape', required=False, action='store_true', help='update release dates from Discogs website')
    main_group.add_argument('--onthisday', '--on-this-day', required=False, action='store_true', help='display any release anniversaries')
    main_group.add_argument('--random', required=False, action='store_true', help='generate random selection')
    main_group.add_argument('--missing', required=False, action='store_true', help='show unmatched releases')
    main_group.add_argument('--list-by-name', required=False, action='store_true', help='list items by artist name')
    main_group.add_argument('--list-by-date', required=False, action='store_true', help='list items by release date')
    main_group.add_argument('--status', required=False, action='store_true', help='report status of database')

    scope_group = parser.add_mutually_exclusive_group(required=False)
    scope_group.add_argument('--find', required=False, help='find matching text in database')
    scope_group.add_argument('--begin', type=int, required=False, default=0, help='begin at discogs_id')
    scope_group.add_argument('--id', type=int, required=False, default=0, help='only a specific Discogs id')
    scope_group.add_argument('--unmatched', required=False, action='store_true', help='only unmatched items')
    scope_group.add_argument('--undated', required=False, action='store_true', help='only items without a full release date')
    scope_group.add_argument('--format', required=False, help='find matching format')
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

    db.initialize_db()

    if args.gui:
        run_gui(config)

    # args2 = vars(args)
    # print(args2)
    else:

        main(config)
