#!/usr/bin/env python3

import os
from contextlib import contextmanager
import sqlite3
from collections import namedtuple
import logging
from .utils import earliest_date

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


def fetch_discogs_release_rows():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM discogs_releases")
    rows = cursor.fetchall()
    conn.close()
    return rows


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


def update_discogs_artist(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute(f"""
            SELECT artist
            FROM discogs_releases
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.artist

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'artist', new_value, old_value))
        cur.execute("""
            UPDATE discogs_releases
            SET artist = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_discogs_title(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute(f"""
            SELECT title
            FROM discogs_releases
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.title

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'title', new_value, old_value))
        cur.execute("""
            UPDATE discogs_releases
            SET title = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_discogs_master_id(discogs_id, new_value, old_value):

    with db_ops() as cur:
        cur.execute(f"""
            SELECT master_id
            FROM discogs_releases
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.master_id

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'master_id', new_value, old_value))
        cur.execute("""
            UPDATE discogs_releases
            SET master_id = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_discogs_year(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute(f"""
            SELECT year
            FROM discogs_releases
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.year

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'year', new_value, old_value))
        cur.execute("""
            UPDATE discogs_releases
            SET year = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_mb_mbid(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute("""
            SELECT mbid
            FROM mb_matches
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.mbid

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'mbid', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET mbid = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_mb_artist(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute(f"""
            SELECT artist
            FROM mb_matches
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.artist

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'artist', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET artist = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_mb_title(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute(f"""
            SELECT title
            FROM mb_matches
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.title

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'title', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET title = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_mb_sort_name(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute(f"""
            SELECT sort_name
            FROM mb_matches
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.sort_name

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'sort_name', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET sort_name = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_discogs_sort_name(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute(f"""
            SELECT sort_name
            FROM discogs_releases
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.sort_name

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'sort_name', new_value, old_value))
        cur.execute("""
            UPDATE discogs_releases
            SET sort_name = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_mb_country(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute(f"""
            SELECT country
            FROM mb_matches
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.country

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'country', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET country = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_mb_format(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute("""
            SELECT format
            FROM mb_matches
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.format

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'format', new_value, old_value))

        cur.execute("""
            UPDATE mb_matches
            SET format = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_mb_primary_type(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute("""
            SELECT primary_type
            FROM mb_matches
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.primary_type

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'primary_type', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET primary_type = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_mb_score(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute("""
            SELECT score
            FROM mb_matches
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.score

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'score', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET score = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_mb_release_date(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute("""
            SELECT release_date
            FROM mb_matches
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.release_date

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'release_date', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET release_date = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_discogs_release_date(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute("""
            SELECT release_date
            FROM discogs_releases
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.release_date

        if old_value == new_value:
            return

        old_value = str(old_value)
        new_value = str(new_value)

        if earliest_date(old_value, new_value) == old_value:
            return

        # if old_value is not None and len(new_value) < len(old_value):
        #     print(row_ignore_change(discogs_id, 'release_date',
        #                             new_value, old_value, "ignored shorter"))
        #     return

        # if old_value is not None and old_value < new_value:
        #     print(row_ignore_change(discogs_id, 'release_date',
        #                             new_value, old_value, "ignored newer"))
        #     return

        print(row_change(discogs_id, 'release_date', new_value, old_value))

        cur.execute("""
            UPDATE discogs_releases
            SET release_date = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_discogs_country(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute(f"""
            SELECT country
            FROM discogs_releases
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.country

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'country', new_value, old_value))
        cur.execute("""
            UPDATE discogs_releases
            SET country = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_discogs_barcodes(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute(f"""
            SELECT barcodes
            FROM discogs_releases
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.barcodes

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'barcodes', new_value, old_value))
        cur.execute("""
            UPDATE discogs_releases
            SET barcodes = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_discogs_catnos(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute(f"""
            SELECT catnos
            FROM discogs_releases
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.catnos

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'catnos', new_value, old_value))
        cur.execute("""
            UPDATE discogs_releases
            SET catnos = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def update_discogs_format(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute(f"""
            SELECT format
            FROM discogs_releases
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.format

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'format', new_value, old_value))
        cur.execute("""
            UPDATE discogs_releases
            SET format = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def fetch_discogs_release(discogs_id):

    with db_ops() as cur:
        cur.execute("""
            SELECT *
            FROM discogs_releases
            WHERE discogs_id = ?""", (int(discogs_id),))
        row = cur.fetchone()

    return row


def fetch_musicbrainz_row(discogs_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT id, discogs_id, mbid, artist, title, sort_name, country, score
        FROM mb_matches
        WHERE discogs_id = {discogs_id}
        """)
    item = cursor.fetchone()
    conn.close()
    return item


def update_mb_format(discogs_id, new_value):

    with db_ops() as cur:
        cur.execute("""
            SELECT format
            FROM mb_matches
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = row.format

        if old_value == new_value:
            return

        print(row_change(discogs_id, 'format', new_value, old_value))

        cur.execute("""
            UPDATE mb_matches
            SET format = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def insert_row(
        discogs_id,
        artist=None,
        title=None,
        country=None,
        format=None,
        year=None,
        barcodes=None,
        catnos=None,
        release_date=None,
        sort_name=None,
        master_id=None):

    with db_ops() as cur:
        cur.execute("""
            INSERT INTO discogs_releases (discogs_id, artist, title, country, format, year, barcodes, catnos, release_date, sort_name, master_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (discogs_id, artist, title, country, format, year, barcodes, catnos, release_date, sort_name, master_id))


def insert_mb_matches_row(
        discogs_id=None,
        mbid=None,
        artist=None,
        title=None,
        sort_name=None,
        country=None,
        format=None,
        primary_type=None,
        score=None,
        release_date=None):

    with db_ops() as cur:
        cur.execute("""
            INSERT INTO mb_matches (discogs_id, mbid, artist, title, sort_name, country, format, primary_type, score, release_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (discogs_id, mbid, artist, title, sort_name, country, format, primary_type, score, release_date))


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


def db_summarise_row(id, config=None):

    with db_ops() as cur:
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
