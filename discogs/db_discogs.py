from modules import db

import logging

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def update_field_if_changed(discogs_id, field_name, new_value, callback=print):
    with db.context_manager() as cur:
        cur.execute(f"""
            SELECT {field_name}
            FROM discogs_releases
            WHERE discogs_id = ? """, (discogs_id,))
        row = cur.fetchone()
        if not row:
            raise Exception('Unexpected row not found error')
        old_value = getattr(row, field_name)

        if old_value == new_value:
            return

        callback(db.row_change(discogs_id, field_name, new_value, old_value))
        cur.execute(f"""
            UPDATE discogs_releases
            SET {field_name} = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def set_artist(discogs_id, new_value, callback=print):
    update_field_if_changed(discogs_id, 'artist', new_value, callback)


def set_title(discogs_id, new_value, callback=print):
    update_field_if_changed(discogs_id, 'title', new_value, callback)


def set_format(discogs_id, new_value, callback=print):
    update_field_if_changed(discogs_id, 'format', new_value, callback)


def set_country(discogs_id, new_value, callback=print):
    update_field_if_changed(discogs_id, 'country', new_value, callback)


def set_barcodes(discogs_id, new_value, callback=print):
    update_field_if_changed(discogs_id, 'barcodes', new_value, callback)


def set_catnos(discogs_id, new_value, callback=print):
    update_field_if_changed(discogs_id, 'catnos', new_value, callback)


def set_year(discogs_id, new_value, callback=print):
    update_field_if_changed(discogs_id, 'year', new_value, callback)


def set_master_id(discogs_id, new_value, callback=print):
    update_field_if_changed(discogs_id, 'master_id', new_value, callback)


def set_release_date(discogs_id, new_value, force=False, callback=print):
    with db.context_manager() as cur:
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

        if not force:
            if old_value is not None and (new_value is None or len(new_value) < len(old_value)):
                logger.debug(db.row_ignore_change(
                    discogs_id, 'release_date', new_value, old_value, "shorter"))
                return

            if old_value is not None and old_value < new_value:
                logger.debug(db.row_ignore_change(
                    discogs_id, 'release_date', new_value, old_value, "newer"))
                return

        callback(db.row_change(discogs_id, 'release_date', new_value, old_value))
        callback('skipped!')
        return

        cur.execute("""
            UPDATE discogs_releases
            SET release_date = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def set_sort_name(discogs_id, new_value):
    update_field_if_changed(discogs_id, 'sort_name', new_value)


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

    with db.context_manager() as cur:
        cur.execute("""
            INSERT INTO discogs_releases (discogs_id, artist, title, country, format, year, barcodes, catnos, release_date, sort_name, master_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (discogs_id, artist, title, country, format, year, barcodes, catnos, release_date, sort_name, master_id))


def fetch_row(discogs_id):

    with db.context_manager() as cur:
        cur.execute("""
            SELECT *
            FROM discogs_releases
            WHERE discogs_id = ?""", (int(discogs_id),))
        row = cur.fetchone()

    return row


def fetch_discogs_release_rows(where=None):
    conn = db.get_connection()
    cursor = conn.cursor()

    query = []
    query.append("SELECT * FROM discogs_releases")
    if where:
        query.append(where)
    query.append('ORDER BY discogs_id')
    cursor.execute(' '.join(query))
    rows = cursor.fetchall()
    conn.close()
    return rows


def fetch_unmatched_discogs_release_rows():
    conn = db.get_connection()
    cursor = conn.cursor()

    query = []
    query.append('SELECT d.*')
    query.append('FROM discogs_releases d')
    query.append('LEFT JOIN mb_matches m USING(discogs_id)')
    query.append('WHERE m.mbid IS NULL')
    query.append('ORDER BY d.discogs_id')
    cursor.execute(' '.join(query))
    rows = cursor.fetchall()
    conn.close()
    return rows


def fetch_row_by_discogs_id(discogs_id):

    with db.context_manager() as cur:
        cur.execute('SELECT * FROM items WHERE release_id = ?', (discogs_id,))
        row = cur.fetchone()

    return row


# OAuth token management for Discogs
def set_oauth_tokens(oauth_token, oauth_token_secret):
    with db.context_manager() as cur:
        cur.execute("DELETE FROM discogs_oauth")
        cur.execute("INSERT INTO discogs_oauth (oauth_token, oauth_token_secret) VALUES (?, ?)",
                    (oauth_token, oauth_token_secret))


def get_oauth_tokens():
    with db.context_manager() as cur:
        cur.execute("SELECT oauth_token, oauth_token_secret FROM discogs_oauth LIMIT 1")
        row = cur.fetchone()
    return row
