from modules import db


def update_field_if_changed(discogs_id, field_name, new_value):
    with db.context_manager() as cur:
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

        print(db.row_change(discogs_id, field_name, new_value, old_value))
        cur.execute(f"""
            UPDATE mb_matches
            SET {field_name} = ?
            WHERE discogs_id = ? """, (new_value, discogs_id))


def set_mbid(discogs_id, new_value):
    update_field_if_changed(discogs_id, 'mbid', new_value)


def set_artist(discogs_id, new_value):
    update_field_if_changed(discogs_id, 'artist', new_value)


def set_title(discogs_id, new_value):
    update_field_if_changed(discogs_id, 'title', new_value)


def set_country(discogs_id, new_value):
    update_field_if_changed(discogs_id, 'country', new_value)


def set_format(discogs_id, new_value):
    update_field_if_changed(discogs_id, 'format', new_value)


def set_primary_type(discogs_id, new_value):
    update_field_if_changed(discogs_id, 'primary_type', new_value)


def set_score(discogs_id, new_value):
    update_field_if_changed(discogs_id, 'score', new_value)


def set_release_date(discogs_id, new_value):
    update_field_if_changed(discogs_id, 'release_date', new_value)


def set_sort_name(discogs_id, new_value):
    update_field_if_changed(discogs_id, 'sort_name', new_value)


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

    with db.context_manager() as cur:
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
