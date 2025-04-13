from modules.db import db_ops, row_change
from modules.utils import earliest_date


def set_artist(discogs_id, new_value):

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


def set_title(discogs_id, new_value):

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


def set_format(discogs_id, new_value):

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


def set_country(discogs_id, new_value):

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


def set_barcodes(discogs_id, new_value):

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


def set_catnos(discogs_id, new_value):

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


def set_year(discogs_id, new_value):

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


def set_master_id(discogs_id, new_value):

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


def set_release_date(discogs_id, new_value):

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


def set_sort_name(discogs_id, new_value):

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


def fetch_row(discogs_id):

    with db_ops() as cur:
        cur.execute("""
            SELECT *
            FROM discogs_releases
            WHERE discogs_id = ?""", (int(discogs_id),))
        row = cur.fetchone()

    return row
