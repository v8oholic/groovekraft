#!/usr/bin/env python3

import os
from contextlib import contextmanager
import sqlite3
from collections import namedtuple
import logging

logger = logging.getLogger(__name__)

DB_PATH = os.path.join("database", "discogs.db")


def get_connection(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = namedtuple_factory
    return conn


def initialize_db(db_path=DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
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
            sort_name TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # SET updated_at = strftime('%Y-%m-%d %H:%M:%S:%s', 'now', 'localtime')

    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS update_discogs_releases_updatetime
        BEFORE UPDATE
            ON discogs_releases
        BEGIN
            UPDATE discogs_releases
            SET updated_at = CURRENT_TIMESTAMP
            WHERE id = OLD.id;
        END;
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mb_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            discogs_id INTEGER,
            mbid TEXT,
            artist TEXT,
            title TEXT,
            country TEXT,
            format TEXT,
            primary_type TEXT,
            score INTEGER,
            release_date TEXT,
            sort_name SORT_NAME,
            matched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (discogs_id) REFERENCES discogs_releases (discogs_id)
        );
    """)

    # cursor.execute("""
    #     CREATE TABLE IF NOT EXISTS release_dates (
    #         id INTEGER PRIMARY KEY AUTOINCREMENT,
    #         discogs_id INTEGER,
    #         release_date TEXT,
    #         sort_name TEXT,
    #         score REAL,
    #         matched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    #         FOREIGN KEY (discogs_id) REFERENCES discogs_releases (discogs_id)
    #     );
    # """)

    conn.commit()
    conn.close()


@contextmanager
def context_manager(db_path=DB_PATH, read_only=False):
    """Wrapper to take care of committing and closing a database

    See https://stackoverflow.com/questions/67436362/decorator-for-sqlite3/67436763
    """
    if read_only:
        conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    else:
        conn = sqlite3.connect(db_path)
    conn.row_factory = namedtuple_factory

    try:
        cur = conn.cursor()
        yield cur
    except Exception as e:
        # do something with exception
        conn.rollback()
        raise e
    else:
        conn.commit()
    finally:
        conn.close()


def namedtuple_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    cls = namedtuple("Row", fields)
    return cls._make(row)


def row_change(release_id, data_name, data_to, data_from):
    return f'💾 {data_name} set to {data_to} (was {data_from}) for release {release_id}'


def row_ignore_change(release_id, data_name, data_to, data_from, reason):
    return f'⛔️ {reason} {data_name} not set to {data_to} (still {data_from}) for release {release_id}'


def get_release_date_by_discogs_id(discogs_id, config):

    with context_manager(config) as cur:
        cur.execute('SELECT * FROM items WHERE release_id = ?', (int(discogs_id),))
        row = cur.fetchone()

    if row is None:
        return row

    return row.release_date


def fetch_row_by_mb_id(mb_id, config):

    with context_manager(config) as cur:
        cur.execute('SELECT * FROM items WHERE mb_id = ?', (mb_id,))
        row = cur.fetchone()

    return row


def db_summarise_row(id, config=None):

    with context_manager() as cur:
        cur.execute("""
            SELECT *
            FROM discogs_releases
            WHERE discogs_id = ?""", (id,))
        row = cur.fetchone()

    output = []

    output.append(f'release {id}')

    artist = row.artist
    if artist:
        output.append(artist)

    title = row.title
    if title:
        output.append(title)

    release_date = row.release_date
    if release_date:
        output.append(str(release_date))

    country = row.country
    if country:
        output.append(country)

    return ' '.join(output)
