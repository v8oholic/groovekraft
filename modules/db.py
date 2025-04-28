#!/usr/bin/env python3
import logging
from collections import namedtuple
import sqlite3
from contextlib import contextmanager
import os


logger = logging.getLogger(__name__)

CREATE_DISCOGS_RELEASES_TABLE = """
    CREATE TABLE IF NOT EXISTS discogs_releases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        discogs_id INTEGER UNIQUE,
        artist TEXT NOT NULL,
        title TEXT NOT NULL,
        year INTEGER,
        barcodes TEXT,
        catnos TEXT,
        country TEXT,
        format TEXT,
        master_id INTEGER,
        release_date TEXT,
        sort_name TEXT COLLATE NOCASE,
        primary_image_uri TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
"""

CREATE_DISCOGS_RELEASES_TRIGGER = """
    CREATE TRIGGER IF NOT EXISTS update_discogs_releases_updatetime
    BEFORE UPDATE
        ON discogs_releases
    BEGIN
        UPDATE discogs_releases
        SET updated_at = CURRENT_TIMESTAMP
        WHERE id = OLD.id;
    END;
"""

CREATE_MB_MATCHES_TABLE = """
    CREATE TABLE IF NOT EXISTS mb_matches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        discogs_id INTEGER UNIQUE,
        mbid TEXT,
        artist TEXT,
        title TEXT,
        country TEXT,
        format TEXT,
        primary_type TEXT,
        score INTEGER,
        matched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (discogs_id) REFERENCES discogs_releases (discogs_id)
    );
"""

CREATE_MB_MATCHES_TRIGGER = """
    CREATE TRIGGER IF NOT EXISTS update_mb_matches_updatetime
    BEFORE UPDATE
        ON mb_matches
    BEGIN
        UPDATE mb_matches
        SET matched_at = CURRENT_TIMESTAMP
        WHERE id = OLD.id;
    END;
"""

CREATE_DISCOGS_OAUTH_TABLE = """
    CREATE TABLE IF NOT EXISTS discogs_oauth (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        oauth_token TEXT,
        oauth_token_secret TEXT
    );
"""

CREATE_MB_CREDENTIALS_TABLE = """
    CREATE TABLE IF NOT EXISTS mb_credentials (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        password TEXT NOT NULL
    );
"""


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = namedtuple_factory
    return conn


def initialize_db(db_path: str) -> None:
    if not db_path:
        logger.critical('Missing db_path')
        exit()

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = get_connection(db_path)
    cursor = conn.cursor()

    logger.info("Initializing the database...")

    cursor.execute(CREATE_DISCOGS_RELEASES_TABLE)
    cursor.execute(CREATE_DISCOGS_RELEASES_TRIGGER)
    cursor.execute(CREATE_MB_MATCHES_TABLE)
    cursor.execute(CREATE_MB_MATCHES_TRIGGER)
    cursor.execute(CREATE_DISCOGS_OAUTH_TABLE)
    cursor.execute(CREATE_MB_CREDENTIALS_TABLE)

    conn.commit()
    conn.close()


@contextmanager
def context_manager(db_path: str, read_only: bool = False):
    """Wrapper to take care of committing and closing a database

    See https://stackoverflow.com/questions/67436362/decorator-for-sqlite3/67436763
    """
    try:
        conn = sqlite3.connect(f'file:{db_path}?mode=ro',
                               uri=True) if read_only else sqlite3.connect(db_path)
        conn.row_factory = namedtuple_factory
        cur = conn.cursor()
        yield cur
    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        raise e
    else:
        conn.commit()
    finally:
        conn.close()


def namedtuple_factory(cursor, row) -> namedtuple:
    return namedtuple("Row", [column[0] for column in cursor.description])(*row)


def row_change(release_id: int, data_name: str, data_to: str, data_from: str) -> str:
    return f'💾 {data_name} set to {data_to} (was {data_from}) for release {release_id}'


def row_ignore_change(release_id: int, data_name: str, data_to: str, data_from: str, reason: str) -> str:
    return f'⛔️ {reason} {data_name} not set to {data_to} (still {data_from}) for release {release_id}'


def get_release_date_by_discogs_id(db_path: str, discogs_id: int) -> str:
    with context_manager(db_path) as cur:
        cur.execute('SELECT * FROM items WHERE release_id = ?', (discogs_id,))
        row = cur.fetchone()

    return row.release_date if row else None


def fetch_row_by_mb_id(db_path: str, mb_id: str) -> namedtuple:
    with context_manager(db_path) as cur:
        cur.execute('SELECT * FROM items WHERE mb_id = ?', (mb_id,))
        return cur.fetchone()


def db_summarise_row(db_path, id: int) -> str:
    with context_manager(db_path) as cur:
        cur.execute("""
            SELECT *
            FROM discogs_releases
            WHERE discogs_id = ?""", (id,))
        row = cur.fetchone()

    output = [f'release {id}']
    if row.artist:
        output.append(row.artist)
    if row.title:
        output.append(row.title)
    if row.release_date:
        output.append(str(row.release_date))
    if row.country:
        output.append(row.country)

    return ' '.join(output)
