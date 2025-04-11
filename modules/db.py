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
            release_date TEXT,
            imported_at TEXT DEFAULT CURRENT_TIMESTAMP,
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
            sort_name TEXT,
            country,
            score REAL,
            matched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (discogs_id) REFERENCES discogs_releases (discogs_id)
        );
    """)

    conn.commit()
    conn.close()


@contextmanager
def db_ops(db_path=DB_PATH, read_only=False):
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


def update_artist(release_id, new_value, old_value, config):

    if old_value == new_value:
        return

    if config.dry_run:
        print(row_ignore_change(release_id, 'artist', new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'artist', new_value, old_value))
    with db_ops(config) as cur:
        cur.execute('UPDATE items SET artist = ? WHERE release_id = ?', (new_value, release_id))


def update_artist(release_id, new_value, old_value, read_only=False):

    if old_value == new_value:
        return

    if read_only:
        print(row_ignore_change(release_id, 'artist', new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'artist', new_value, old_value))
    with db_ops(read_only=read_only) as cur:
        cur.execute("""
            UPDATE discogs_releases
            SET artist = ?
            WHERE discogs_id = ?""", (new_value, release_id))


def update_title(release_id, new_value, old_value, config):
    if old_value == new_value:
        return

    if config.dry_run:
        print(row_ignore_change(release_id, 'artist', new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'title', new_value, old_value))
    with db_ops(config) as cur:
        cur.execute('UPDATE items SET title = ? WHERE release_id = ?', (new_value, release_id))


def update_title(release_id, new_value, old_value, read_only=False):
    if old_value == new_value:
        return

    if read_only:
        print(row_ignore_change(release_id, 'artist', new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'title', new_value, old_value))
    with db_ops(read_only=read_only) as cur:
        cur.execute("""
            UPDATE discogs_releases
            SET title = ?
            WHERE discogs_id = ?""", (new_value, release_id))


def update_mb_id(release_id, new_value, old_value, config):

    if old_value == new_value:
        return

    if config.dry_run:
        print(row_ignore_change(release_id, 'mb_id', new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'mb_id', new_value, old_value))
    with db_ops(config) as cur:
        cur.execute('UPDATE items SET mb_id = ? WHERE release_id = ?', (new_value, release_id))


def update_mb_artist(release_id, new_value, old_value, config):

    if old_value == new_value:
        return

    if config.dry_run:
        print(row_ignore_change(release_id, 'mb_artist',
                                new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'mb_artist', new_value, old_value))
    with db_ops(config) as cur:
        cur.execute('UPDATE items SET mb_artist = ? WHERE release_id = ?', (new_value, release_id))


def update_mb_title(release_id, new_value, old_value, config):

    if old_value == new_value:
        return

    if config.dry_run:
        print(row_change(release_id, 'mb_title', new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'mb_title', new_value, old_value))
    with db_ops(config) as cur:
        cur.execute('UPDATE items SET mb_title = ? WHERE release_id = ?', (new_value, release_id))


def update_sort_name(release_id, new_value, old_value, config):

    if old_value == new_value:
        return

    if config.dry_run:
        print(row_change(release_id, 'sort_name', new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'sort_name', new_value, old_value))
    with db_ops(config) as cur:
        cur.execute('UPDATE items SET sort_name = ? WHERE release_id = ?', (new_value, release_id))


def update_release_date(release_id, new_value, old_value, config):

    if old_value == new_value:
        return

    if config.dry_run:
        print(row_ignore_change(release_id, 'release_date',
                                new_value, old_value, 'read-only'))
        return

    if old_value is not None and len(new_value) < len(old_value):
        print(row_ignore_change(release_id, 'release_date',
                                new_value, old_value, "ignored shorter"))
        return

    if old_value is not None and old_value < new_value:
        print(row_ignore_change(release_id, 'release_date',
                                new_value, old_value, "ignored newer"))
        return

    print(row_change(release_id, 'release_date', new_value, old_value))
    with db_ops(config) as cur:
        cur.execute('UPDATE items SET release_date = ? WHERE release_id = ?',
                    (new_value, release_id))


def update_country(release_id, new_value, old_value, config):

    if old_value == new_value:
        return

    if config.dry_run:
        print(row_ignore_change(release_id, 'country', new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'country', new_value, old_value))
    with db_ops(config) as cur:
        cur.execute('UPDATE items SET country = ? WHERE release_id = ?', (new_value, release_id))


def update_country(release_id, new_value, old_value, read_only):

    if old_value == new_value:
        return

    if read_only:
        print(row_ignore_change(release_id, 'country', new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'country', new_value, old_value))
    with db_ops(read_only=read_only) as cur:
        cur.execute("""
            UPDATE discogs_releases
            SET country = ?
            WHERE discogs_id = ?""", (new_value, release_id))


def update_barcodes(release_id, new_value, old_value, read_only):

    if old_value == new_value:
        return

    if read_only:
        print(row_ignore_change(release_id, 'barcodes', new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'barcodes', new_value, old_value))
    with db_ops(read_only=read_only) as cur:
        cur.execute("""
            UPDATE discogs_releases
            SET barcodes = ?
            WHERE discogs_id = ?""", (new_value, release_id))


def update_catnos(release_id, new_value, old_value, read_only):

    if old_value == new_value:
        return

    if read_only:
        print(row_ignore_change(release_id, 'catnos', new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'catnos', new_value, old_value))
    with db_ops(read_only=read_only) as cur:
        cur.execute("""
            UPDATE discogs_releases
            SET catnos = ?
            WHERE discogs_id = ?""", (new_value, release_id))


def update_format(release_id, new_value, old_value, config):

    if old_value == new_value:
        return

    if config.dry_run:
        print(row_ignore_change(release_id, 'format', new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'format', new_value, old_value))
    with db_ops(config) as cur:
        cur.execute('UPDATE items SET format = ? WHERE release_id = ?', (new_value, release_id))


def update_format(release_id, new_value, old_value, read_only=False):

    if old_value == new_value:
        return

    if read_only:
        print(row_ignore_change(release_id, 'format', new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'format', new_value, old_value))
    with db_ops(read_only=read_only) as cur:
        cur.execute('UPDATE discogs_releases SET format = ? WHERE discogs_id = ?',
                    (new_value, release_id))


def update_mb_primary_type(release_id, new_value, old_value, config):

    if old_value == new_value:
        return

    if config.dry_run:
        print(row_ignore_change(release_id, 'mb_primary_type',
                                new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'mb_primary_type', new_value, old_value))
    with db_ops(config) as cur:
        cur.execute('UPDATE items SET mb_primary_type = ? WHERE release_id = ?',
                    (new_value, release_id))


def update_version_id(release_id, new_value, old_value, config):

    if old_value == new_value:
        return

    if config.dry_run:
        print(row_ignore_change(release_id, 'version_id',
                                new_value, old_value, 'read-only'))
        return

    print(row_change(release_id, 'version_id', new_value, old_value))
    with db_ops(config) as cur:
        cur.execute('UPDATE items SET version_id = ? WHERE release_id = ?',
                    (new_value, release_id))


def fetch_row_by_discogs_id(discogs_id, config):

    with db_ops(config) as cur:
        cur.execute('SELECT * FROM items WHERE release_id = ?', (int(discogs_id),))
        row = cur.fetchone()

    return row


def fetch_row_by_discogs_id_v2(discogs_id, read_only=False):

    with db_ops(read_only=read_only) as cur:
        cur.execute('SELECT * FROM discogs_releases WHERE discogs_id = ?', (int(discogs_id),))
        row = cur.fetchone()

    return row


def insert_row(
        artist=None,
        title=None,
        format=None,
        mb_primary_type=None,
        release_date=None,
        release_id=0,
        country=None,
        mb_id=None,
        mb_artist=None,
        mb_title=None,
        sort_name=None,
        version_id=0,
        config=None):

    if config.dry_run:
        print('dry run - not inserted')
        return

    with db_ops(config) as cur:
        cur.execute("""
            INSERT INTO items (artist, title, format, mb_primary_type, release_date, release_id, country, mb_id, mb_artist, mb_title, sort_name, version_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
                    (artist, title, format, mb_primary_type, release_date, release_id, country, mb_id, mb_artist, mb_title, sort_name, version_id))


def insert_row_v2(
        discogs_id=None,
        artist=None,
        title=None,
        country=None,
        format=None,
        release_date=None,
        barcodes=None,
        catnos=None,
        read_only=False):

    if read_only:
        print('dry run - not inserted')
        return

    with db_ops(read_only=read_only) as cur:
        cur.execute("""
            INSERT INTO discogs_releases (discogs_id, artist, title, country, format, release_date, barcodes, catnos)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
                    (discogs_id, artist, title, country, format, release_date, barcodes, catnos))


def get_release_date_by_discogs_id(discogs_id, config):

    with db_ops(config) as cur:
        cur.execute('SELECT * FROM items WHERE release_id = ?', (int(discogs_id),))
        row = cur.fetchone()

    if row is None:
        return row

    return row.release_date


def fetch_row_by_mb_id(mb_id, config):

    with db_ops(config) as cur:
        cur.execute('SELECT * FROM items WHERE mb_id = ?', (mb_id,))
        row = cur.fetchone()

    return row
