#!/usr/bin/env python3

# CLI entry point

import sys
import argparse
import signal
from dateutil import parser
import logging
import configparser
import os
from pathlib import Path

from shared.config import AppConfig, APP_NAME
from shared.gui import run_gui
from shared.db import initialize_db

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)


def get_user_data_dir(app_name: str) -> Path:
    if sys.platform == "darwin":
        base_dir = Path.home() / "Library" / "Application Support"
    elif sys.platform.startswith("win"):
        base_dir = Path(os.getenv("APPDATA"))
    else:  # Assume Linux
        base_dir = Path.home() / ".local" / "share"
    app_dir = base_dir / app_name
    app_dir.mkdir(parents=True, exist_ok=True)
    return app_dir


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
    config.root_folder = get_user_data_dir(APP_NAME)
    config.db_path = os.path.join(config.root_folder, "database", "GrooveKraft.db")
    config.images_folder = os.path.join(config.root_folder, "images")

    initialize_db(config.db_path)

    run_gui(config)
