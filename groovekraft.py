#!/usr/bin/env python3

# CLI entry point

import discogs_client
import sys
import argparse
import signal
from dateutil import parser
import logging
import configparser
import os

from modules import db
from modules.config import AppConfig
from modules.gui import run_gui

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
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

if __name__ == "__main__":

    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(
        description="Music Collection Importer",
        epilog="Import from Discogs skips previously imported releases. Update from MusicBrainz processes only previously imported releases."
    )

    # autopep8: off
    parser.add_argument('--init', required=False, action='store_true', help='initialise database')
    parser.add_argument('--gui', required=False, action='store_true', help='launch experimental PyQt6 GUI')
    parser.add_argument("--database", type=str, help="Path to the SQLite database file")
    parser.add_argument('--verbose', required=False, action='store_true', help='verbose messages')
    # autopep8: on

    args = parser.parse_args()

    config_parser = configparser.ConfigParser()
    if not config_parser.read("discogs.ini"):
        print("Error: Could not find discogs.ini configuration file. Are you running from the application directory?")
        sys.exit(1)

    config = AppConfig(args, os.path.dirname(os.path.abspath(__file__)))
    config.load_from_config_parser(config_parser)

    db.initialize_db()

    run_gui(config)
