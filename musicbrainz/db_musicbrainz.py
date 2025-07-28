from shared.db import context_manager, row_change
import logging
import hashlib

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


def delete_match(db_path, discogs_id, callback=print):
    with context_manager(db_path) as cur:
        cur.execute("""
            DELETE FROM mb_matches
            WHERE discogs_id = ? """, (discogs_id,))


def update_field_if_changed(db_path, discogs_id, field_name, new_value, callback=print):
    with context_manager(db_path) as cur:
        cur.execute(f"""
            SELECT {field_name}
            FROM mb_matches
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = getattr(row, field_name)

        if old_value == new_value:
            return

        callback(row_change(discogs_id, field_name, new_value, old_value))
        cur.execute(f"""
            UPDATE mb_matches
            SET {field_name} = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def set_mbid(db_path, discogs_id, new_value, callback=print):
    update_field_if_changed(db_path, discogs_id, 'mbid', new_value, callback=callback)


def set_artist(db_path, discogs_id, new_value, callback=print):
    update_field_if_changed(db_path, discogs_id, 'artist', new_value, callback=callback)


def set_title(db_path, discogs_id, new_value, callback=print):
    update_field_if_changed(db_path, discogs_id, 'title', new_value, callback=callback)


def set_country(db_path, discogs_id, new_value, callback=print):
    update_field_if_changed(db_path, discogs_id, 'country', new_value, callback=callback)


def set_format(db_path, discogs_id, new_value, callback=print):
    update_field_if_changed(db_path, discogs_id, 'format', new_value, callback=callback)


def set_primary_type(db_path, discogs_id, new_value, callback=print):
    update_field_if_changed(db_path, discogs_id, 'primary_type', new_value, callback=callback)


def set_score(db_path, discogs_id, new_value, callback=print):
    update_field_if_changed(db_path, discogs_id, 'score', new_value, callback=callback)


def update_matched_at(db_path, discogs_id, callback=print):
    with context_manager(db_path) as cur:
        cur.execute("""
            UPDATE mb_matches
            SET matched_at = CURRENT_TIMESTAMP
            WHERE discogs_id = ? """, (discogs_id,))


def insert_row(
        db_path,
        discogs_id=None,
        mbid=None,
        artist=None,
        title=None,
        country=None,
        format=None,
        primary_type=None,
        score=None):

    with context_manager(db_path) as cur:
        cur.execute("""
            INSERT INTO mb_matches (discogs_id, mbid, artist, title, country, format, primary_type, score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (discogs_id, mbid, artist, title, country, format, primary_type, score))


def fetch_row(db_path, discogs_id):
    with context_manager(db_path) as cur:
        cur.execute(f"""
            SELECT id, discogs_id, mbid, artist, title, country, score, matched_at
            FROM mb_matches
            WHERE discogs_id = {discogs_id}
            """)
        item = cur.fetchone()
        return item


def set_credentials(db_path, username, password):
    hashed_password = hashlib.sha256(password.encode('utf-8')).hexdigest()
    with context_manager(db_path) as cur:
        cur.execute("DELETE FROM mb_credentials")
        cur.execute("INSERT INTO mb_credentials (username, password) VALUES (?, ?)",
                    (username, hashed_password))


def get_credentials(db_path):
    with context_manager(db_path) as cur:
        cur.execute("SELECT username, password FROM mb_credentials LIMIT 1")
        return cur.fetchone()


def verify_credentials(db_path, username, password):
    with context_manager(db_path) as cur:
        cur.execute("SELECT password FROM mb_credentials WHERE username = ? LIMIT 1", (username,))
        row = cur.fetchone()
    if row:
        hashed_input = hashlib.sha256(password.encode('utf-8')).hexdigest()
        return hashed_input == row.password
    return False
