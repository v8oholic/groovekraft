#!/usr/bin/env python3

import discogs_client
from discogs_client.exceptions import HTTPError
import sys
import argparse
import sqlite3
import signal
from collections import namedtuple

USER_AGENT = 'v8oholic_discogs_application/1.0'
CONSUMER_KEY = "yEJrrZEZrExGHEPjNQca"
CONSUMER_SECRET = "isFjruJTfmmXFXiaywRqCUSkIGwHlHKn"

NAMED_TUPLES = True

global args
global db


def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)


def select_distinct():
    global db

    if NAMED_TUPLES:
        db.row_factory = namedtuple_factory
    else:
        db.row_factory = sqlite3.Row

    cur = db.cursor()

    cur.execute("""
        SELECT DISTINCT item
        FROM items
        ORDER BY release_id
    """)
    return cur.fetchall()


def namedtuple_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    cls = namedtuple("Row", fields)
    return cls._make(row)


def fetch_file_row(artist, title):
    global db

    if NAMED_TUPLES:
        db.row_factory = namedtuple_factory
    else:
        db.row_factory = sqlite3.Row

    cur = db.cursor()

    cur.execute("""
        SELECT artist, title, format, year, release_id, folder
        FROM status
        WHERE artist = ? AND title = ?
    """,
                (artist, title))
    return cur.fetchone()


def fetch_file_rows(artist):
    global db

    if NAMED_TUPLES:
        db.row_factory = namedtuple_factory
    else:
        db.row_factory = sqlite3.Row

    cur = db.cursor()

    if artist:
        cur.execute("""
            SELECT artist, title, format, year, release_id, folder
            FROM items
            WHERE artist = ?
            ORDER BY artist, year, title
        """,
                    (artist,))
    else:
        cur.execute("""
            SELECT artist, title, format, year, release_id, folder
            FROM items
            ORDER BY artist, year, title
        """)

    return cur.fetchall()


def insert_file_row(artist, title, format, year, release_id, folder):
    global db

    db.execute("""
        INSERT INTO items (artist, title, format, year, release_id, folder)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
               (artist, title, format, year, release_id, folder))
    db.commit()


def update_file_row(artist, title, format, year, release_id, folder):
    global db

    db.execute("""
        UPDATE items
        SET artist = ?, title = ?, format = ?, year = ?, release_id = ?
        WHERE artist = ? AND title = ?
    """,
               (artist, title, format, year, release_id, folder))
    db.commit()


def delete_file_row(artist, title):
    db.execute("""
        DELETE FROM items
        WHERE artist = ? AND title = ?
    """,
               (artist, title))
    db.commit()


def connect_to_discogs(oauth_token=None, ouath_token_secret=None):
    authenticated = False

    if oauth_token and ouath_token_secret:
        try:
            client = discogs_client.Client(
                USER_AGENT,
                consumer_key=CONSUMER_KEY,
                consumer_secret=CONSUMER_SECRET,
                token=oauth_token,
                secret=ouath_token_secret
            )
            access_token = oauth_token
            access_secret = ouath_token_secret
            authenticated = True

        except HTTPError:
            print("Unable to authenticate.")
            access_token = None
            access_secret = None

    if not authenticated:

        # instantiate discogs_client object
        client = discogs_client.Client(USER_AGENT)

        # prepare the client with our API consumer data
        client.set_consumer_key(CONSUMER_KEY, CONSUMER_SECRET)
        token, secret, url = client.get_authorize_url()

        print(" == Request Token == ")
        print(f"    * oauth_token        = {token}")
        print(f"    * oauth_token_secret = {secret}")
        print()

        # visit the URL in auth_url to allow the app to connect

        print(f"Please browse to the following URL {url}")

        accepted = "n"
        while accepted.lower() == "n":
            print()
            accepted = input(f"Have you authorized me at {url} [y/n] :")

        # note the token

        # Waiting for user input. Here they must enter the verifier key that was
        # provided at the unqiue URL generated above.
        oauth_verifier = input("Verification code : ")

        try:
            access_token, access_secret = client.get_access_token(oauth_verifier)
            authenticated = True

        except HTTPError:
            print("Unable to authenticate.")
            sys.exit(1)

    return client, access_token, access_secret


if False:
    # With an active auth token, we're able to reuse the client object and request
    # additional discogs authenticated endpoints, such as database search.
    search_results = discogs_client.search(
        "House For All", type="release", artist="Blunted Dummies"
    )

    print("\n== Search results for release_title=House For All ==")
    for release in search_results:
        print(f"\n\t== discogs-id {release.id} ==")
        print(f'\tArtist\t: {", ".join(artist.name for artist in release.artists)}')
        print(f"\tTitle\t: {release.title}")
        print(f"\tYear\t: {release.year}")
        print(f'\tLabels\t: {", ".join(label.name for label in release.labels)}')

    # You can reach into the Fetcher lib if you wish to used the wrapped requests
    # library to download an image. The following example demonstrates this.
    image = search_results[0].images[0]["uri"]
    content, resp = discogs_client._fetcher.fetch(
        None, "GET", image, headers={"User-agent": discogs_client.user_agent}
    )

    print(" == API image request ==")
    print(f"    * response status      = {resp}")
    print(f'    * saving image to disk = {image.split("/")[-1]}')

    with open(image.split("/")[-1], "wb") as fh:
        fh.write(content)

# x = dir(discogs_client.release('9459125'))
# print(x)


def import_from_discogs():
    oauth_token = 'bTNnyxNgaHvEarRXVjBiRAoJZgTBPuUXosDEdiEG'
    ouath_token_secret = 'YJfCvMXmaxJgfroTnSjtSDKWZpsLbEYPEwUwuSyK'

    client, access_token, access_secret = connect_to_discogs(oauth_token, ouath_token_secret)

    # fetch the identity object for the current logged in user.
    user = client.identity()

    print()
    print(" == User ==")
    print(f"    * username           = {user.username}")
    print(f"    * name               = {user.name}")
    print(" == Access Token ==")
    print(f"    * oauth_token        = {access_token}")
    print(f"    * oauth_token_secret = {access_secret}")
    print(" Authentication complete. Future requests will be signed with the above tokens.")

    print(f'number of items in all collections: {len(user.collection_folders[0].releases)}')

    collection = user.collection_folders
    sorted_collection = sorted(collection, key=lambda folder: folder.id)
    print(f'number of folders: {len(sorted_collection)}')
    for folder in sorted_collection:
        print(f'id: {folder.id:>7} name: {folder.name} items: {folder.count}')

        if folder.id == 0:
            continue
        # print(dir(folder))

        for item in folder.releases:
            # print(item)

            release = item.release
            master = release.master

            cur = db.cursor()
            cur.execute('SELECT * FROM items WHERE release_id = ?', (release.id,))
            row = cur.fetchone()

            if not row is None:
                print(f'skipping {row.release_id} {row.artist} {row.title} {row.year}')
                continue

            if master == None:
                year_status = f'no master release, using release year {release.year}'
                year = release.year
            elif release.year == 0 and master.year == 0:
                year_status = 'no release year'
                year = 0
            elif release.year == 0 and master.year > 0:
                year_status = f'no release year, using master release year {master.year}'
                year = master.year
            elif release.year > 0 and master.year == 0:
                year_status = f'no master release year, using release year {release.year}'
                year = release.year
            elif release.year == master.year:
                year_status = f'release year {release.year} and master release year {master.year} agree'
                year = release.year
            elif release.year > release.master.year:
                year_status = f'using master release year ({master.year} earlier than {release.year})'
                year = release.master.year
            else:
                year_status = f'using release year ({release.year} earlier than {master.year})'
                year = release.year

            master_url = '' if master is None else master.url

            print()
            print(f'artist          : {release.artists_sort}')
            print(f'title           : {release.title}')
            print(f'format          : {release.formats[0]['name']}')
            print(f'year            : {year}')
            print(f'year status     : {year_status}')
            print(f'release_id      : {release.id}')
            print(f'folder          : {folder.name}')
            print(f'url             : {release.url}')
            print(f'master_url      : {master_url}')

            db.execute("""
            INSERT INTO items (artist, title, format, year, release_id, folder, url, master_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                       (release.artists_sort, release.title, release.formats[0]['name'], year, release.id, folder.name, release.url, master_url))
            db.commit()


def main():
    global args, db

    parser = argparse.ArgumentParser(
        description="Discogs Collection",
        epilog=""
    )

    # autopep8: off
    parser.add_argument('--init', required=False, action=argparse.BooleanOptionalAction, default=True, help='import from Discogs')

    parser.add_argument('--dry-run', required=False, action="store_true", help='dry run to test filtering')
    parser.add_argument('--verbose', required=False, action=argparse.BooleanOptionalAction, default=False, help='verbose messages')
    # autopep8: on

    args = parser.parse_args()

    # TODO create database from scratch

    # open database
    db = sqlite3.connect('discogs.db')
    res = db.execute("SELECT name FROM sqlite_master WHERE name='items'")

    if res.fetchone() is None:
        print("creating table")
        db.execute(
            "CREATE TABLE items(artist, title, format, year, release_id, folder, url, master_url)")
        db.execute("CREATE UNIQUE INDEX idx_items_release_id ON items(release_id)")
        db.execute(
            "CREATE UNIQUE INDEX idx_items_sort ON items(artist, year, title, release_id)")
    # else:
    #   cur.execute("ALTER TABLE status ADD locked NOT NULL DEFAULT 0")
        # word count, word list, words
        # db.execute("ALTER TABLE status ADD word_count DEFAULT 0")
        # db.execute("ALTER TABLE status DROP words_found")
        # db.execute("ALTER TABLE status ADD transcription")
        # db.execute("CREATE UNIQUE INDEX idx_status_primary ON status(girl, number)")

    if NAMED_TUPLES:
        db.row_factory = namedtuple_factory
    else:
        db.row_factory = sqlite3.Row

    if args.init:
        import_from_discogs()

    db.close()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    main()
