import logging
from modules import db, discogs_importer, utils
from modules import config


def fls(data_str, length):
    if len(data_str) > length:
        return data_str[:length-3]+'...'
    else:
        return data_str.ljust(length)


def missing(config: config.AppConfig):

    with db.context_manager() as cur:

        # LEFT JOIN mb_matches m ON d.discogs_id = m.discogs_id

        print()
        print('Items in Discogs collection which have no MusicBrainz match')
        print('-----------------------------------------------------------')

        # construct query
        query = []

        find_string = config.find
        format_string = config.format

        query.append('SELECT d.sort_name, d.title, d.format, d.release_date, d.discogs_id')
        query.append('FROM discogs_releases d')
        query.append('LEFT JOIN mb_matches m USING(discogs_id)')
        query.append('WHERE m.mbid IS NULL')
        if find_string:
            query.append(f'AND (d.artist LIKE "%{find_string}%"')
            query.append(f'OR d.title LIKE "%{find_string}%"')
            query.append(f'OR d.sort_name LIKE "%{find_string}%")')
        if format_string:
            query.append(f'AND d.format LIKE "%{format_string}%"')
        query.append('ORDER BY d.sort_name, d.release_date, d.title, d.discogs_id')

        query_string = ' '.join(query)

        cur.execute(query_string)

        rows = cur.fetchall()
        if len(rows) == 0:
            print('no matching items')
            return

        max_title_len = 45

        artist_len = 0
        title_len = 0
        format_len = 0
        release_date_len = 0

        for p in range(2):
            if p == 0:
                artist_len = 0
                title_len = 0
                format_len = 0
                release_date_len = 0

                last_artist = None
            else:
                title_len = min(title_len, max_title_len)

            for row in rows:

                artist = row.sort_name
                title = row.title

                format = row.format.split(',')[0] if row.format else ''
                release_date = row.release_date if row.release_date else ''

                if p == 0:
                    artist_len = max(artist_len, len(artist))
                    title_len = max(title_len, len(title))

                    format_len = max(format_len, len(format))
                    release_date_len = max(release_date_len, len(release_date))
                else:
                    # logging.debug(
                    #     f"{row.release_id:>8} {artist:20} {title:20} {format:10} {release_date:10}")
                    # logging.debug(
                    #     f'{row.release_id:>8} {fls(artist, artist_len)} {fls(title, title_len)} {fls(format, format_len)} {fls(release_date, release_date_len)}')
                    if artist != last_artist:
                        last_artist = artist
                        print()
                        print(artist)
                        print('='*len(artist))

                    print(
                        f'{row.discogs_id:>8} {fls(title, title_len)} {fls(format, format_len)} {fls(release_date, release_date_len)}')

        print()
        print(f'{len(rows)} rows')
        print()


def list(config: config.AppConfig, order_by_date: bool = False):

    with db.context_manager() as cur:

        find_string = config.find
        format_string = config.format

        query = []

        query.append('SELECT d.sort_name AS artist, d.title, d.format, d.release_date, d.discogs_id')
        query.append('FROM discogs_releases d')
        query.append('LEFT JOIN mb_matches m USING(discogs_id)')
        query.append('WHERE TRUE')
        if config.find:
            query.append(f'AND (d.artist LIKE "%{config.find}%"')
            query.append(f'OR d.title LIKE "%{config.find}%"')
            query.append(f'OR d.sort_name LIKE "%{config.find}%")')
        if config.format:
            query.append(f'AND d.format LIKE "%{config.format}%"')
        if config.undated:
            query.append(f'AND length(d.release_date) < 10')
        if config.unmatched:
            query.append(f'AND m.mbid IS NULL')
        if order_by_date:
            query.append('ORDER BY d.release_date, d.sort_name, d.title, d.discogs_id')
        else:
            query.append('ORDER BY d.sort_name, d.release_date, d.title, d.discogs_id')

        cur.execute(' '.join(query))

        items = cur.fetchall()
        if len(items) == 0:
            print('no matching items')
            return

        max_title_len = 45

        artist_len = 0
        title_len = 0
        format_len = 0
        release_date_len = 0

        for p in range(2):
            if p == 0:
                artist_len = 0
                title_len = 0
                format_len = 0
                release_date_len = 0

                last_artist = None
            else:
                title_len = min(title_len, max_title_len)

            for item in items:

                artist = item.artist
                title = item.title

                format = item.format.split(',')[0] if item.format else ''
                release_date = item.release_date if item.release_date else ''

                if p == 0:
                    artist_len = max(artist_len, len(artist))
                    title_len = max(title_len, len(title))

                    format_len = max(format_len, len(format))
                    release_date_len = max(release_date_len, len(release_date))
                else:
                    # logging.debug(
                    #     f"{row.release_id:>8} {artist:20} {title:20} {format:10} {release_date:10}")
                    # logging.debug(
                    #     f'{row.release_id:>8} {fls(artist, artist_len)} {fls(title, title_len)} {fls(format, format_len)} {fls(release_date, release_date_len)}')
                    if artist != last_artist:
                        last_artist = artist
                        print()
                        artist_caption = f'{artist}'
                        print(artist_caption)
                        print('='*len(artist_caption))

                    print(
                        f'{item.discogs_id:>8} {fls(title, title_len)} {fls(format, format_len)} {fls(release_date, release_date_len)}')

    print()
    print(f'{len(items)} items')
    print()


def status(config: config.AppConfig):

    def output_nvp(label, value):
        print(f'{fls(label, 45)}: {value}')

    def output_nvp_percentage(label, count, total):
        value = f'{count} ({round(count * 100 / total, 1)}%)'
        print(f'{fls(label, 45)}: {value}')

    discogs_client, discogs_access_token, discogs_access_secret = discogs_importer.connect_to_discogs(
        config)

    # fetch the identity object for the current logged in user.
    discogs_user = discogs_client.identity()

    output_nvp("Discogs username", discogs_user.username)
    output_nvp("MusicBrainz username", config.username)

    folder = discogs_user.collection_folders[0]

    output_nvp('releases on Discogs', len(folder.releases))
    discogs_count = len(folder.releases)

    with db.context_manager() as cur:

        cur.execute("""
            SELECT COUNT(*) as count
            FROM discogs_releases
        """)
        row = cur.fetchone()

        localdb_count = row.count
        output_nvp_percentage('releasees in local database', localdb_count, discogs_count)

        query = []

        query.append('SELECT COUNT(d.discogs_id) as count')
        query.append('FROM discogs_releases d')
        query.append('LEFT JOIN mb_matches m USING(discogs_id)')
        query.append('WHERE m.mbid IS NOT NULL')

        cur.execute(' '.join(query))
        row = cur.fetchone()

        output_nvp_percentage('releases matched in MusicBrainz', row.count, localdb_count)

        query = []

        query.append('SELECT COUNT(d.discogs_id) as count')
        query.append('FROM discogs_releases d')
        query.append('LEFT JOIN mb_matches m USING(discogs_id)')
        query.append('WHERE m.mbid IS NULL')

        cur.execute(' '.join(query))
        row = cur.fetchone()

        output_nvp_percentage('releases not matched in MusicBrainz', row.count, localdb_count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM discogs_releases
            WHERE release_date IS NULL
        """)
        row = cur.fetchone()

        output_nvp_percentage('no release date', row.count, localdb_count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM discogs_releases
            WHERE LENGTH(release_date) = 4
        """)
        row = cur.fetchone()

        output_nvp_percentage('just release year', row.count, localdb_count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM discogs_releases
            WHERE LENGTH(release_date) = 7
        """)
        row = cur.fetchone()

        output_nvp_percentage('just month and release year', row.count, localdb_count)

        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM discogs_releases
            WHERE LENGTH(release_date) = 10
        """)
        row = cur.fetchone()

        output_nvp_percentage('full release date', row.count, localdb_count)


def output_row(row):
    if row.release_date:
        delta = utils.humanize_date_delta(row.release_date)

    print(f'released        : {delta}')
    print(f'artist          : {row.artist}')
    print(f'title           : {row.title}')
    print(f'format          : {row.format}')
    print(f'discogs_id      : {row.discogs_id}')
    if row.release_date:
        # print(f'release_date    : {row.release_date}')
        print(f'release_date    : {utils.parse_and_humanize_date(row.release_date)}')


def random_selection(config: config.AppConfig):

    with db.context_manager() as cur:

        query = []

        query.append('SELECT d.sort_name AS artist, d.title, d.format, d.release_date, d.discogs_id')
        query.append('FROM discogs_releases d')
        query.append('LEFT JOIN mb_matches m USING(discogs_id)')
        query.append('WHERE TRUE')
        if config.find:
            query.append(f'AND (d.artist LIKE "%{config.find}%"')
            query.append(f'OR d.title LIKE "%{config.find}%"')
            query.append(f'OR d.sort_name LIKE "%{config.find}%")')
        if config.format:
            query.append(f'AND d.format LIKE "%{config.format}%"')
        query.append('ORDER BY RANDOM()')
        query.append('LIMIT 1')

        cur.execute(' '.join(query))

        item = cur.fetchone()

        print()
        print('random selection:')
        print()
        output_row(item)
        print()


def on_this_day(today_str='', config: config.AppConfig = None):

    with db.context_manager() as cur:

        query = []

        query.append('SELECT d.sort_name AS artist, d.title, d.format, d.release_date, d.discogs_id')
        query.append('FROM discogs_releases d')
        query.append('LEFT JOIN mb_matches m USING(discogs_id)')
        query.append('WHERE d.release_date IS NOT NULL')
        if config.find:
            query.append(f'AND (d.artist LIKE "%{config.find}%"')
            query.append(f'OR d.title LIKE "%{config.find}%"')
            query.append(f'OR d.sort_name LIKE "%{config.find}%")')
        if config.format:
            query.append(f'AND d.format LIKE "%{config.format}%"')
        query.append(
            'ORDER BY length(d.release_date) DESC, d.release_date, d.sort_name, d.title, d.discogs_id')

        cur.execute(' '.join(query))

        items = cur.fetchall()

        include_count = 0

        for item in items:

            if item.release_date and len(item.release_date) == 10:
                if today_str:
                    include = utils.is_today_anniversary(item.release_date, today_str)
                else:
                    include = utils.is_today_anniversary(item.release_date)

            elif item.release_date and len(item.release_date) == 7:
                if today_str:
                    include = utils.is_month_anniversary(item.release_date, today_str)
                else:
                    include = utils.is_month_anniversary(item.release_date)
            else:
                include = False

            if include:
                include_count += 1

                print()
                output_row(item)
                print()

        print()
        print(f'{include_count} items')
        print()
