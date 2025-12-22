import os
from contextlib import contextmanager
import sqlite3
from collections import namedtuple
import logging
CREATE_IDX_DISCOGS_ID = "CREATE UNIQUE INDEX IF NOT EXISTS idx_discogs_id ON discogs_releases (discogs_id);"
CREATE_IDX_RELEASE_DATE = "CREATE INDEX IF NOT EXISTS idx_release_date ON discogs_releases (release_date);"
CREATE_IDX_MATCHES_DISCOGS_ID = "CREATE UNIQUE INDEX IF NOT EXISTS idx_matches_discogs_id ON mb_matches (discogs_id);"
CREATE_IDX_MATCHES_MBID = "CREATE INDEX IF NOT EXISTS idx_matches_mbid ON mb_matches (mbid);"
CREATE_IDX_MB_CREDENTIALS_USERNAME = "CREATE UNIQUE INDEX IF NOT EXISTS idx_mb_credentials_username ON mb_credentials (username);"
CREATE_IDX_MB_MATCHES_MBID = "CREATE INDEX IF NOT EXISTS idx_mb_matches_mbid ON mb_matches (mbid);"
CREATE_IDX_SORT_NAME = "CREATE INDEX IF NOT EXISTS idx_sort_name ON discogs_releases (sort_name);"
CREATE_IDX_ARTIST = "CREATE INDEX IF NOT EXISTS idx_artist ON discogs_releases (artist);"
CREATE_IDX_TITLE = "CREATE INDEX IF NOT EXISTS idx_title ON discogs_releases (title);"
CREATE_IDX_FORMAT = "CREATE INDEX IF NOT EXISTS idx_format ON discogs_releases (format);"
#!/usr/bin/env python3


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

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
        play_count INTEGER DEFAULT 0,
        last_played TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        release_date_locked INTEGER DEFAULT 0
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

    # Create indexes for performance
    cursor.execute(CREATE_IDX_DISCOGS_ID)
    cursor.execute(CREATE_IDX_RELEASE_DATE)
    cursor.execute(CREATE_IDX_MATCHES_DISCOGS_ID)
    cursor.execute(CREATE_IDX_MATCHES_MBID)
    cursor.execute(CREATE_IDX_MB_CREDENTIALS_USERNAME)
    cursor.execute(CREATE_IDX_MB_MATCHES_MBID)
    cursor.execute(CREATE_IDX_SORT_NAME)
    cursor.execute(CREATE_IDX_ARTIST)
    cursor.execute(CREATE_IDX_TITLE)
    cursor.execute(CREATE_IDX_FORMAT)

    conn.commit()
    migrate_add_release_date_locked(db_path)
    migrate_add_play_stats(db_path)
    conn.close()


def migrate_add_release_date_locked(db_path):
    with context_manager(db_path) as cur:
        cur.execute("PRAGMA table_info(discogs_releases);")
        columns = [row[1] for row in cur.fetchall()]
        if "release_date_locked" not in columns:
            cur.execute("ALTER TABLE discogs_releases ADD COLUMN release_date_locked INTEGER DEFAULT 0;")


def migrate_add_play_stats(db_path):
    with context_manager(db_path) as cur:
        cur.execute("PRAGMA table_info(discogs_releases);")
        columns = [row[1] for row in cur.fetchall()]
        if "play_count" not in columns:
            cur.execute("ALTER TABLE discogs_releases ADD COLUMN play_count INTEGER DEFAULT 0;")
        if "last_played" not in columns:
            cur.execute("ALTER TABLE discogs_releases ADD COLUMN last_played TEXT;")


def increment_play_stats(db_path: str, discogs_id: int):
    """Increment play_count and stamp last_played for a release."""
    with context_manager(db_path) as cur:
        cur.execute("""
            UPDATE discogs_releases
            SET play_count = COALESCE(play_count, 0) + 1,
                last_played = CURRENT_TIMESTAMP
            WHERE discogs_id = ?
        """, (discogs_id,))
        cur.execute("""
            SELECT play_count, last_played
            FROM discogs_releases
            WHERE discogs_id = ?
        """, (discogs_id,))
        return cur.fetchone()


@contextmanager
def context_manager(db_path: str, read_only: bool = False, namedtuple: bool = True):
    """Wrapper to take care of committing and closing a database

    See https://stackoverflow.com/questions/67436362/decorator-for-sqlite3/67436763
    """
    try:
        conn = sqlite3.connect(f'file:{db_path}?mode=ro',
                               uri=True) if read_only else sqlite3.connect(db_path)

        if namedtuple:
            conn.row_factory = namedtuple_factory
        else:
            conn.row_factory = sqlite3.Row

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
