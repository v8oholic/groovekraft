from modules import db


def set_mbid(discogs_id, new_value):

    with db.db_ops() as cur:
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

        print(db.row_change(discogs_id, 'mbid', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET mbid = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def set_artist(discogs_id, new_value):

    with db.db_ops() as cur:
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

        print(db.row_change(discogs_id, 'artist', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET artist = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def set_title(discogs_id, new_value):

    with db.db_ops() as cur:
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

        print(db.row_change(discogs_id, 'title', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET title = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def set_country(discogs_id, new_value):

    with db.db_ops() as cur:
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

        print(db.row_change(discogs_id, 'country', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET country = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def set_format(discogs_id, new_value):

    with db.db_ops() as cur:
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

        print(db.row_change(discogs_id, 'format', new_value, old_value))

        cur.execute("""
            UPDATE mb_matches
            SET format = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def set_primary_type(discogs_id, new_value):

    with db.db_ops() as cur:
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

        print(db.row_change(discogs_id, 'primary_type', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET primary_type = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def set_score(discogs_id, new_value):

    with db.db_ops() as cur:
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

        print(db.row_change(discogs_id, 'score', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET score = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def set_release_date(discogs_id, new_value):

    with db.db_ops() as cur:
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

        print(db.row_change(discogs_id, 'release_date', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET release_date = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def set_sort_name(discogs_id, new_value):

    with db.db_ops() as cur:
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

        print(db.row_change(discogs_id, 'sort_name', new_value, old_value))
        cur.execute("""
            UPDATE mb_matches
            SET sort_name = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def insert_row(
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

    with db.db_ops() as cur:
        cur.execute("""
            INSERT INTO mb_matches (discogs_id, mbid, artist, title, sort_name, country, format, primary_type, score, release_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (discogs_id, mbid, artist, title, sort_name, country, format, primary_type, score, release_date))


def fetch_row(discogs_id):
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT id, discogs_id, mbid, artist, title, sort_name, country, score
        FROM mb_matches
        WHERE discogs_id = {discogs_id}
        """)
    item = cursor.fetchone()
    conn.close()
    return item
