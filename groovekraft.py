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


if __name__ == "__main__":

    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(
        description="GrooveKraft",
        epilog="Import from Discogs, match against MusicBrainz, view collection locally."
    )

    # autopep8: off
    parser.add_argument("--database", type=str, help="Path to the SQLite database file")
    parser.add_argument('--verbose', required=False, action='store_true', help='verbose messages')
    # autopep8: on

    args = parser.parse_args()

    config_parser = configparser.ConfigParser()
    # if not config_parser.read("groovekraft.ini"):
    #     print("Error: Could not find groovekraft.ini configuration file. Are you running from the application directory?")
    #     sys.exit(1)

    config = AppConfig(args, os.path.dirname(os.path.abspath(__file__)))
    config.load_from_config_parser(config_parser)

    db.initialize_db()

    run_gui(config)
