import argparse
import os
import sys
from pathlib import Path

from shared.config import APP_NAME, AppConfig
from shared.db import initialize_db


def get_user_data_dir(app_name: str) -> Path:
    if sys.platform == "darwin":
        base_dir = Path.home() / "Library" / "Application Support"
    elif sys.platform.startswith("win"):
        base_dir = Path(os.getenv("APPDATA"))
    else:
        base_dir = Path.home() / ".local" / "share"
    app_dir = base_dir / app_name
    app_dir.mkdir(parents=True, exist_ok=True)
    return app_dir


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="GrooveKraft",
        epilog="Import from Discogs, match against MusicBrainz, view collection locally.",
    )
    parser.add_argument("--database", type=str, help="Path to the SQLite database file")
    parser.add_argument("--verbose", required=False, action="store_true", help="verbose messages")
    parser.add_argument("--server", action="store_true", help="run the web server instead of the desktop UI")
    parser.add_argument("--host", default="127.0.0.1", help="host/interface for server mode")
    parser.add_argument("--port", type=int, default=8000, help="port for server mode")
    return parser


def build_config(args, app_root: str) -> AppConfig:
    config = AppConfig(args, app_root)
    config.root_folder = str(get_user_data_dir(APP_NAME))
    config.db_path = args.database or os.path.join(config.root_folder, "database", "GrooveKraft.db")
    config.images_folder = os.path.join(config.root_folder, "images")
    initialize_db(config.db_path)
    return config
