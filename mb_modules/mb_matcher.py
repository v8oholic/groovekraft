#!/usr/bin/env python3

# MusicBrainz matching logic

import logging

from discogs import db_discogs
from mb_modules import db_musicbrainz
from modules import db
from modules import utils
import musicbrainzngs
from discogs_client.exceptions import HTTPError
# from musicbrainzngs import musicbrainz
import sys
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

SCORE_WEIGHTS = {
    "artist": 100,
    "title": 100,
    "country": 50,
    "format": 100,
    "barcode": 100,
    "unilateral_barcode": 0,
    "no_barcode": 0,
    "catno": 100,
    "unilateral_catno": 0,
    "no_catno": 0
}

MAXIMUM_SCORE = SCORE_WEIGHTS['artist'] + SCORE_WEIGHTS['title'] + SCORE_WEIGHTS['country'] + \
    SCORE_WEIGHTS['format']+SCORE_WEIGHTS['barcode']+SCORE_WEIGHTS['catno']

PERFECT_SCORE = 100
MINIMUM_SCORE = 40
MINIMUM_ARTIST_SCORE = 30
MINIMUM_TITLE_SCORE = 50


def score_stars(score):
    thresholds = [
        (100, '🟢'),    # perfect
        (70, '🟡'),     # good
        (40, '🟠'),     # weak
        (0, '🔴')       # poor
    ]
    for threshold, icon in thresholds:
        if score >= threshold:
            return icon
    return '❓'


def mb_normalize_artist(name):
    artist = utils.trim_if_ends_with_number_in_brackets(name)
    if artist == 'Various':
        artist = 'Various Artists'
    return artist


def mb_normalize_title(title):
    slash_pos = title.find('/')

    if slash_pos == -1:
        return title

    title = title[:slash_pos].rstrip()

    return title


def parse_discogs_format(primary_type, secondary_types):
    if primary_type == 'Vinyl':
        if 'EP' in secondary_types and '7"' in secondary_types:
            return '7" EP', 'single'
        elif 'EP' in secondary_types and '12"' in secondary_types:
            return '12" EP', 'single'
        elif '12"' in secondary_types:
            return '12" Single', 'single'
        elif '7"' in secondary_types:
            return '7" Single', 'single'
        elif 'Compilation' in secondary_types:
            return 'LP Compilation', 'album'
        elif 'LP' in secondary_types:
            return 'LP', 'album'
        else:
            return '?', '?'

    elif primary_type == 'CD':
        if 'Mini' in secondary_types:
            return 'CD 3" Mini Single', 'single'
        elif 'Single' in secondary_types or 'Maxi-Single' in secondary_types:
            return 'CD Single', 'single'
        elif 'EP' in secondary_types:
            return 'CD EP', 'single'
        elif 'HDCD' in secondary_types and 'Album' in secondary_types:
            return 'CD HDCD Album', 'album'
        elif 'LP' in secondary_types or 'Album' in secondary_types:
            return 'CD Album', 'album'
        elif 'Mini-Album' in secondary_types:
            return 'CD Mini-Album', 'album'
        elif 'Compilation' in secondary_types:
            return 'CD Compilation', 'album'
        else:
            return 'CD Single', 'single'

    elif primary_type == 'Flexi-disc':
        if '7"' in secondary_types:
            return '7" flexi-disc', 'single'
        return '?', '?'

    elif primary_type == 'Box Set':
        if '12"' in secondary_types:
            return '12" Singles Box Set', 'single'
        elif '7"' in secondary_types:
            return '7" Singles Box Set', 'single'
        elif 'LP' in secondary_types:
            return 'LP Box Set', 'album'
        elif 'EP' in secondary_types:
            return 'EP Box Set', 'single'
        elif 'Single' in secondary_types or 'Maxi-Single' in secondary_types:
            return 'Singles Box Set', 'single'
        return 'Box Set', 'Other'

    return '?', '?'


def mb_normalize_format(discogs_format):
    if not discogs_format:
        raise Exception('format error')

    parts = discogs_format.split(':')
    primary_type = parts[0].strip()
    secondary_types = [p.strip() for p in parts[1].split(',')] if len(parts) > 1 else []

    fmt, mb_primary_type = parse_discogs_format(primary_type, secondary_types)
    mb_secondary_type = f'{primary_type} 12"' if '12"' in secondary_types else primary_type

    return fmt, mb_primary_type, primary_type, mb_secondary_type


def convert_format(discogs_format):
    primary_type = discogs_format.get('name')
    secondary_types = discogs_format.get('descriptions') or []

    fmt, mb_primary_type = parse_discogs_format(primary_type, secondary_types)

    return fmt, mb_primary_type, primary_type


def mb_normalize_country(country):
    return utils.convert_country_from_discogs_to_musicbrainz(country)


def barcode_match_scorer(barcodes1, barcodes2):
    # note that Discogs can have multiple barcodes, MusicBrainz only has one.
    barcodes1 = utils.normalize_identifier_list(barcodes1)
    barcodes2 = utils.normalize_identifier_list(barcodes2)

    if len(barcodes1) > 0 and len(barcodes2) == 0:
        # only on one side
        return SCORE_WEIGHTS['unilateral_barcode']
    elif len(barcodes1) == 0 and len(barcodes2) > 0:
        # only on the other side
        return SCORE_WEIGHTS['unilateral_barcode']
    elif len(barcodes1) == 0 and len(barcodes2) == 0:
        # neither side
        return SCORE_WEIGHTS['no_barcode']

    for tmp in barcodes2:
        if tmp in barcodes1:
            return SCORE_WEIGHTS['barcode']

    return 0


def catno_match_scorer(catnos1, catnos2):

    catnos1 = utils.normalize_identifier_list(catnos1)
    catnos2 = utils.normalize_identifier_list(catnos2)

    if len(catnos1) > 0 and len(catnos2) == 0:
        # only on one side
        return SCORE_WEIGHTS['unilateral_catno']
    elif len(catnos1) == 0 and len(catnos2) > 0:
        # only on the other side
        return SCORE_WEIGHTS['unilateral_catno']
    elif len(catnos1) == 0 and len(catnos2) == 0:
        # neither side
        return SCORE_WEIGHTS['no_catno']

    for tmp in catnos2:
        if tmp in catnos1:
            return SCORE_WEIGHTS['catno']

    return 0


def disambiguator_score(
        artist=None,
        title=None,
        country=None,
        format=None,
        barcodes=None,
        catnos=None,
        mb_release=None,
        mbid=None):
    """Score a MusicBrainz release against a Discogs release

    Returns a rounded percentage score, with 100 being a perfect match."""

    load_mbid = None

    if mb_release:
        if not all([
            mb_release.get('label-info-count'),
            mb_release.get('artist-credit-phrase'),
            mb_release.get('medium-list')
        ]):
            load_mbid = mb_release.get('id')
    else:
        load_mbid = mbid

    if mb_release is None and load_mbid == 0:
        raise Exception("Unable to load MusicBrainz release")

    if load_mbid:
        mb_release = musicbrainzngs.get_release_by_id(
            load_mbid, includes=['artists', 'artist-credits', 'labels', 'media'])
        if mb_release:
            mb_release = mb_release.get('release')
        else:
            raise Exception("Unable to load MusicBrainz release")

    # print(summarise(
    #     artist=artist,
    #     title=title,
    #     country=country,
    #     format=format,
    #     catnos=catnos,
    #     barcodes=barcodes))

    artist_score = 0
    title_score = 0
    country_score = 0
    format_score = 0
    barcode_score = 0
    catno_score = 0

    mb_artist = mb_release.get('artist-credit-phrase')
    mb_title = mb_release.get('title')
    mb_country = mb_release.get('country')
    mb_format = mb_get_format(mb_release=mb_release)
    mb_barcode = mb_release.get('barcode')
    mb_catnos = []
    for label_info in mb_release.get('label-info-list'):
        find_catno = label_info.get('catalog-number')
        if find_catno:
            mb_catnos.append(utils.sanitise_identifier(find_catno))
    mb_catnos = list(set(mb_catnos))

    artist_score = fuzzy_match(artist, mb_artist, 'artist') * SCORE_WEIGHTS['artist'] / 100
    if artist_score < MINIMUM_ARTIST_SCORE:
        return 0

    title_score = fuzzy_match(title, mb_title, 'title') * SCORE_WEIGHTS['title'] / 100
    if title_score < MINIMUM_TITLE_SCORE:
        return 0

    if country == mb_country:
        country_score = SCORE_WEIGHTS['country']

    format_score = fuzz.token_sort_ratio(format, mb_format) * SCORE_WEIGHTS['format'] / 100
    barcode_score = barcode_match_scorer(barcodes, mb_barcode)
    catno_score = catno_match_scorer(catnos, mb_catnos)

    total_points = artist_score+title_score+country_score+format_score+catno_score+barcode_score

    # x = summarise(artist=mb_artist, title=mb_title, country=mb_country,
    #               format=mb_format, catnos=mb_catnos, barcodes=mb_barcode)
    # print(f'{x} {total_points} points {round(100 * total_points/MAXIMUM_SCORE)}%')

    return round(100 * total_points/MAXIMUM_SCORE)


def fuzzy_match(str1, str2, desc=''):
    global test_str1
    global test_str2

    if not str1 and not str2:
        return PERFECT_SCORE

    str1 = utils.sanitise_compare_string(str1)
    str2 = utils.sanitise_compare_string(str2)

    if str1 == str2:
        ratio = PERFECT_SCORE
        # print(f'match status for {desc}: "{str1}" "{str2}" {ratio}%')
        return ratio

    # simple = round(fuzz.ratio(str1, str2), 1)
    # partial = round(fuzz.partial_ratio(str1, str2), 1)
    # weighted = round(fuzz.WRatio(str1, str2), 1)
    quick = round(fuzz.QRatio(str1, str2), 1)

    # print(f'{desc} "{str1}" "{str2}" simple {simple} partial {partial} weighted {weighted} quick {quick}')

    ratio = quick
    # print(f'match status for {desc}: "{str1}" "{str2}" {ratio}')
    return ratio


def mb_browse_release_groups_by_discogs_master_link(discogs_master_id=0):
    """Browse release groups for a given Discogs master URL

    The expected result is either no matches or a single match. However
    it is possible for more than one match to be found. Returns a list
    of release groups."""

    discogs_url = f"https://www.discogs.com/master/{discogs_master_id}"
    offset = 0
    batch_size = 25
    max_release_groups = 200
    release_groups = []
    all_results = 0

    try:
        while all_results < max_release_groups:

            mb_url = musicbrainzngs.browse_urls(
                discogs_url,
                includes=["release-group-rels"],
                limit=batch_size,
                offset=offset
            )

            url = mb_url.get('url')
            release_group_relation_list = url.get('release_group-relation-list')

            all_results += len(release_group_relation_list)

            for release_relation in release_group_relation_list:
                if release_relation.get('type') == 'discogs':
                    release_group = release_relation.get('release-group')
                    release_groups.append(release_group)

            # Check if we've reached the end
            if len(release_group_relation_list) < batch_size:
                break

            # Move to the next page
            offset += batch_size

    except musicbrainzngs.musicbrainz.ResponseError:
        # probably 404
        pass

    except Exception as e:
        print(e)

    finally:
        return release_groups


def mb_browse_releases_by_discogs_release_link(discogs_id=0):
    """Browse releases for a given Discogs URL

    The expected result is either no matches or a single match. However
    it is possible for more than one match to be found. Returns a list
    of releases."""

    discogs_url = f"https://www.discogs.com/release/{discogs_id}"
    offset = 0
    batch_size = 25
    max_releases = 200
    releases = []
    all_results = 0

    try:
        while all_results < max_releases:

            mb_url = musicbrainzngs.browse_urls(
                discogs_url,
                includes=["release-rels"],
                limit=batch_size,
                offset=offset
            )

            url = mb_url.get('url')
            release_relation_list = url.get('release-relation-list')

            all_results += len(release_relation_list)

            for release_relation in release_relation_list:
                if release_relation.get('type') == 'discogs':
                    release = release_relation.get('release')
                    releases.append(release)

            # Check if we've reached the end
            if len(release_relation_list) < batch_size:
                break

            # Move to the next page
            offset += batch_size

    except musicbrainzngs.musicbrainz.ResponseError as e:
        # probably 404
        logging.debug(e)
        pass

    except Exception as e:
        print(e)

    finally:
        return releases


def mb_get_releases_for_release_group(mb_id):

    batch_offset = 0
    batch_size = 25
    max_results = 200
    release_list = []

    try:
        while len(release_list) < max_results:
            rels = musicbrainzngs.browse_releases(
                release_group=mb_id,
                includes=['artist-credits', 'labels', 'media', 'release-groups'],
                limit=batch_size,
                offset=batch_offset
            )

            # Add new results
            batch_release_list = rels.get('release-list', [])
            release_list.extend(batch_release_list)

            # Check if we've reached the end
            if len(batch_release_list) < batch_size:
                break

            # Move to the next page
            batch_offset += batch_size

    except Exception as e:
        logger.error(e)
        release_list = []

    finally:
        return release_list


def mb_summarise_release(mb_release=None, mbid=None):

    load_mbid = None

    if mb_release:
        x = mb_release.get('label-info-count')
        if not x:
            load_mbid = mb_release.get('id')
        x = mb_release.get('artist-credit-phrase')
        if not x:
            load_mbid = mb_release.get('id')
        x = mb_release.get('medium-count')
        if not x:
            load_mbid = mb_release.get('id')
    else:
        load_mbid = mbid

    if mb_release is None and load_mbid == 0:
        raise Exception("Unable to load MusicBrainz release")

    if load_mbid:
        mb_release = musicbrainzngs.get_release_by_id(
            load_mbid, includes=['artists', 'artist-credits', 'labels', 'media'])
        if mb_release:
            mb_release = mb_release.get('release')
        else:
            raise Exception("Unable to load MusicBrainz release")

    output = []

    mbid = mb_release.get('id')
    output.append(f'MB release')

    artist = mb_release.get('artist-credit-phrase')
    if not artist:
        artist = mb_release.get('artist')
    if artist:
        output.append(artist)

    title = mb_release.get('title')
    if title:
        output.append(title)

    date = mb_release.get('date')
    if date:
        output.append(date)

    country = mb_release.get('country')
    if country:
        output.append(country)

    format = mb_get_format(mb_release=mb_release)
    if format:
        output.append(format)

    lil = mb_release.get('label-info-list')
    if lil:
        label_catnos = []
        for x in lil:

            label_catno = x.get('catalog-number')
            if label_catno:
                label_catnos.append(label_catno)
        catnos = ','.join(label_catnos)
        output.append(catnos)

    format = mb_get_format(mb_release=mb_release)

    return ' '.join(output)


def get_release_and_release_group(mb_release=None, mbid=None):

    load_mbid = None

    if mb_release:
        if not all([
            mb_release.get('release-group'),
        ]):
            load_mbid = mb_release.get('id')
    else:
        load_mbid = mbid

    if load_mbid:
        mb_release = musicbrainzngs.get_release_by_id(
            load_mbid, includes=['artists', 'artist-credits', 'labels', 'media', 'release-groups'])
        if mb_release:
            mb_release = mb_release.get('release')
        else:
            raise Exception("Unable to load MusicBrainz release")

    if mb_release is None and mbid is None:
        raise Exception('Error loading release')

    release_group_id = mb_release.get('release-group').get('id')

    mb_release_group = musicbrainzngs.get_release_group_by_id(
        release_group_id, includes=['artists', 'releases', 'artist-credits'])
    mb_release_group = mb_release_group.get('release-group')

    return mb_release_group, mb_release


def match_by_discogs_release_link(artist=None, title=None, country=None, format=None, barcodes=[], catnos=[], discogs_id=0):
    """Search MusicBrainz by Discogs release URL

    If the search fails, the master URL is also tried.

    Returns the release, release group and release date if successful."""

    releases = mb_browse_releases_by_discogs_release_link(discogs_id=discogs_id)
    if not releases:
        # nothing found that matches the Discogs release
        return releases
        return None, None, 0

    if len(releases) == 1:
        print(f'🎯 matched {mb_summarise_release(mb_release=releases[0])}')
        mb_release_group, mb_release = get_release_and_release_group(mb_release=releases[0])
        return mb_release_group, mb_release, PERFECT_SCORE

    # more than one match seems unusual, but possible
    best_match_release_group, best_match_release, best_match_score = disambiguate_releases(
        releases,
        artist=artist,
        title=title,
        country=country,
        format=format,
        catnos=catnos,
        barcodes=barcodes)

    if best_match_score < MINIMUM_SCORE:
        return None, None, 0

    return best_match_release_group, best_match_release, best_match_score


def match_by_discogs_master_link(master_id):
    """Search MusicBrainz by Discogs release URL

    If the search fails, the master URL is also tried.

    Returns all releases for any matching release group."""

    candidates = []

    # try to find the master/release group link
    release_groups = mb_browse_release_groups_by_discogs_master_link(master_id)
    for release_group in release_groups:
        releases = mb_get_releases_for_release_group(mb_id=release_group.get('id'))
        candidates.extend(releases)

    return candidates


def find_match_by_discogs_link(
        artist=None,
        title=None,
        country=None,
        format=None,
        barcodes=[],
        catnos=[],
        discogs_id=0,
        master_id=0):
    """Search MusicBrainz by Discogs release URL or master ID."""

    def get_best_release_match(releases):
        best_score = 0
        best_release = None
        for release in releases:
            score = disambiguator_score(
                artist=artist, title=title, country=country,
                format=format, barcodes=barcodes, catnos=catnos, mb_release=release)
            if score > best_score:
                best_score = score
                best_release = release
                if score == PERFECT_SCORE:
                    break
        return best_release, best_score

    def load_release_and_group(mb_release):
        mb_release = musicbrainzngs.get_release_by_id(
            mb_release['id'], includes=["release-groups", 'artists', 'artist-credits']
        ).get('release')
        group_id = mb_release['release-group']['id']
        mb_group = musicbrainzngs.get_release_group_by_id(
            group_id, includes=['artists', 'releases', 'artist-credits']
        ).get('release-group')
        return mb_group, mb_release

    def search_and_rank_releases_by_master(master_id):
        candidates = match_by_discogs_master_link(master_id)
        return disambiguate_releases(
            candidates,
            artist=artist, title=title, country=country,
            format=format, catnos=catnos, barcodes=barcodes)

    releases = mb_browse_releases_by_discogs_release_link(discogs_id=discogs_id)

    if not releases:
        if not master_id:
            return None, None, 0
    elif len(releases) == 1:
        best_release = releases[0]
        print(f'🎯 matched {mb_summarise_release(mb_release=best_release)}')
        return load_release_and_group(best_release) + (PERFECT_SCORE,)

    else:
        best_release, best_score = get_best_release_match(releases)
        if best_score >= MINIMUM_SCORE:
            return load_release_and_group(best_release) + (best_score,)

    # fallback to master link
    if not master_id:
        return None, None, 0

    group_candidates = mb_browse_release_groups_by_discogs_master_link(master_id)
    if not group_candidates:
        return None, None, 0

    if len(group_candidates) == 1:
        return group_candidates[0], None, PERFECT_SCORE

    best_group, best_release, best_score = search_and_rank_releases_by_master(master_id)

    if best_score >= MINIMUM_SCORE:
        print(
            f'{"💯" if best_score == PERFECT_SCORE else "📈"} {best_score}% match {mb_summarise_release(mbid=best_release["id"])}')
        return best_group, best_release, best_score

    return None, None, 0


def mb_match_discogs_release(mb_release_id, discogs_url):
    """ search release relationships for a Discogs link """

    mb_release = musicbrainzngs.get_release_by_id(
        mb_release_id, includes=["url-rels", 'artists', 'artist-credits'])

    local_release = mb_release.get('release', {})

    for rel in local_release.get('url-relation-list', []):
        # print(f'    type {rel['type']} target {rel['target']}')
        if rel['type'] == 'discogs':
            # print(f'    type {rel['type']} target {rel['target']}')
            if rel['type'] == 'discogs' and rel['target'] == discogs_url:
                # print(f"✅ matched {mb_summarise_release(mb_release=local_release)}")
                return True

        return False


def mb_find_release_group_releases(artist=None, title=None, country=None, format=None, catnos=None, barcodes=None, primary_type=None, discogs_id=0):
    """ Search for a release group and release (if discogs_id is specified)

    All these fields are searchable, only some are implemented.

    alias	            (part of) any alias attached to the release group (diacritics are ignored)
    arid	            the MBID of any of the release group artists
    artist	            (part of) the combined credited artist name for the release group, including join phrases (e.g. "Artist X feat.")
    artistname	        (part of) the name of any of the release group artists
    comment	            (part of) the release group's disambiguation comment
    creditname	        (part of) the credited name of any of the release group artists on this particular release group
    firstreleasedate	the release date of the earliest release in this release group (e.g. "1980-01-22")
    primarytype	        the primary type of the release group
    reid	            the MBID of any of the releases in the release group
    release	            (part of) the title of any of the releases in the release group
    releasegroup	    (part of) the release group's title (diacritics are ignored)
    releasegroupaccent	(part of) the release group's title (with the specified diacritics)
    releases	        the number of releases in the release group
    rgid	            the release group's MBID
    secondarytype	    any of the secondary types of the release group
    status	            the status of any of the releases in the release group
    tag	                (part of) a tag attached to the release group
    type	            legacy release group type field that predates the ability to set multiple types (see calculation code)
"""

    query = []

    if artist:
        query.append(f'artist:"{artist}"')

    if title:
        query.append(f'releasegroup:"{title}"')

    if primary_type:
        query.append(f'primarytype:{primary_type}')

    query_string = ' AND '.join(query)

    all_results = []
    offset = 0
    batch_size = 25
    max_results = 100
    candidates = []

    while len(all_results) < max_results:

        result = musicbrainzngs.search_release_groups(
            query=query_string, limit=batch_size, offset=offset)

        # Add new results
        release_group_list = result.get('release-group-list', [])
        all_results.extend(release_group_list)

        if release_group_list:
            # search this batch of release groups
            for group in release_group_list:
                # fetch the release group including releases - note
                gr = musicbrainzngs.get_release_group_by_id(group.get('id'), includes=['releases'])
                gr_group = gr.get('release-group')
                gr_release_list = gr_group.get('release-list')
                candidates.extend(gr_release_list)

        # Check if we've reached the end
        if len(release_group_list) < batch_size:
            break

        # Move to the next page
        offset += batch_size

    return candidates


def mb_find_releases(artist='', title='', catno=None, primary_type=None, country=None, barcode=None, format=None):
    """
        Search for any releases matching the query

        All these fields are searchable.

        alias	        (part of) any alias attached to the release (diacritics are ignored)
        arid	        the MBID of any of the release artists
        artist	        (part of) the combined credited artist name for the release, including join phrases (e.g. "Artist X feat.")
        artistname	    (part of) the name of any of the release artists
        asin	        an Amazon ASIN for the release
        barcode	        the barcode for the release
        catno	        any catalog number for this release (insensitive to case, spaces, and separators)
        comment	        (part of) the release's disambiguation comment
        country	        the 2-letter code (ISO 3166-1 alpha-2) for any country the release was released in
        creditname	    (part of) the credited name of any of the release artists on this particular release
        date	        a release date for the release (e.g. "1980-01-22")
        discids	        the total number of disc IDs attached to all mediums on the release
        discidsmedium	the number of disc IDs attached to any one medium on the release
        format	        the format of any medium in the release (insensitive to case, spaces, and separators)
        laid	        the MBID of any of the release labels
        label	        (part of) the name of any of the release labels
        lang	        the ISO 639-3 code for the release language
        mediums	        the number of mediums on the release
        packaging	    the format of the release (insensitive to case, spaces, and separators)
        primarytype	    the primary type of the release group for this release
        quality	        the listed quality of the data for the release (2 for “high”, 1 for “normal”; cannot search for “low” at the moment; see the related bug report)
        reid	        the release's MBID
        release	        (part of) the release's title (diacritics are ignored)
        releaseaccent	(part of) the release's title (with the specified diacritics)
        rgid	        the MBID of the release group for this release
        script	        the ISO 15924 code for the release script
        secondarytype	any of the secondary types of the release group for this release
        status	        the status of the release
        tag	            (part of) a tag attached to the release
        tracks	        the total number of tracks on the release
        tracksmedium	the number of tracks on any one medium on the release
        type	        legacy release group type field that predates the ability to set multiple types (see calculation code)
    """

    query = []

    if artist:
        query.append(f'artist:"{artist}"')

    if title:
        query.append(f'release:"{title}"')

    if catno:
        query.append(f'catno:"{catno}"')

    if primary_type:
        query.append(f'primarytype:"{primary_type}"')

    if country:
        query.append(f'country:{country}')

    if barcode:
        query.append(f'barcode:{barcode}')

    if format:
        query.append(f'format:{barcode}')

    all_results = []
    offset = 0
    batch_size = 25
    max_results = 200

    query_string = ' AND '.join(query)
    # print(f'query_string {query_string}')

    try:
        while len(all_results) < max_results:
            # Fetch results with pagination
            batch_releases = musicbrainzngs.search_releases(
                query=query_string, limit=batch_size, offset=offset)

            # Add new results
            releases = batch_releases.get('release-list', [])
            all_results.extend(releases)

            # Check if we've reached the end
            if len(releases) < batch_size:
                break

            # Move to the next page
            offset += batch_size

    except Exception as e:
        print(f"Error: {e}")
        all_results = []

    finally:
        return all_results


def add_candidates(candidates, releases):
    """Add missing releases in a list of releases to candidate list"""

    if len(releases) == 0:
        # nothing to add
        return candidates

    if len(candidates) == 0:
        # return the whole list of releases
        return releases

    # build a set of all existing MBIDs in the candidates list
    id_set = {item['id'] for item in candidates}

    for release in releases:
        mbid = release['id']
        if mbid not in id_set:
            candidates.append(release)

    return candidates


def match_release_in_musicbrainz(discogs_id):

    row = db_discogs.fetch_row(discogs_id)
    if not row:
        raise Exception('Error loading row')

    # translate some columns
    discogs_id = row.discogs_id
    artist = mb_normalize_artist(row.artist)
    title = mb_normalize_title(row.title)
    country = mb_normalize_country(row.country)
    catnos = row.catnos
    barcodes = row.barcodes
    _, primary_type, format, secondary_format = mb_normalize_format(row.format)

    # try the Discogs release link first
    candidates = mb_browse_releases_by_discogs_release_link(discogs_id=discogs_id)

    if len(candidates) == 1:
        # special case for a single match
        mb_release = candidates[0]

        output_match_summary(
            artist=artist,
            title=title,
            country=country,
            format=format,
            catnos=catnos,
            barcodes=barcodes,
            score=PERFECT_SCORE)

        mb_release_group, mb_release = get_release_and_release_group(mb_release=mb_release)

        update_tables_after_match(
            discogs_id=discogs_id,
            mb_release=mb_release,
            mb_release_group=mb_release_group,
            best_match_score=PERFECT_SCORE)
        return

    if row.master_id:
        # try the master release to release group link
        candidates = add_candidates(
            candidates, match_by_discogs_master_link(master_id=row.master_id))

    # try some other searches
    candidates = add_candidates(candidates, mb_match_barcodes(barcodes=barcodes))
    candidates = add_candidates(candidates, mb_match_catnos(catnos=catnos))
    candidates = add_candidates(candidates, mb_find_release_group_releases(
        artist=artist,
        title=title,
        primary_type=primary_type))

    mb_release_group, mb_release, best_match_score = disambiguate_releases(
        candidates,
        artist=artist,
        title=title,
        country=country,
        format=secondary_format,
        catnos=catnos,
        barcodes=barcodes)

    output_match_summary(
        artist=artist,
        title=title,
        country=country,
        format=secondary_format,
        catnos=catnos,
        barcodes=barcodes,
        score=best_match_score)

    if best_match_score < MINIMUM_SCORE:
        return

    update_tables_after_match(
        discogs_id=discogs_id,
        mb_release=mb_release,
        mb_release_group=mb_release_group,
        best_match_score=best_match_score)


def mb_match_barcodes(barcodes):

    candidates = []

    barcodes_list = utils.normalize_identifier_list(barcodes)
    for barcode in barcodes_list:
        result = musicbrainzngs.search_releases(barcode=barcode)
        releases = result['release-list']
        if len(releases):
            candidates.extend(releases)

    return candidates


def mb_match_catnos(catnos):

    candidates = []

    catno_list = utils.normalize_identifier_list(catnos)
    for catno in catno_list:
        result = musicbrainzngs.search_releases(catno=catno)
        releases = result['release-list']
        if len(releases):
            candidates.extend(releases)

    return candidates


def disambiguate_releases(
        candidates,
        artist=None,
        title=None,
        country=None,
        format=None,
        catnos=None,
        barcodes=None):

    best_match_score = 0
    best_match_release = None

    for release in candidates:
        disambiguation_score = disambiguator_score(
            artist=artist,
            title=title,
            country=country,
            format=format,
            catnos=catnos,
            barcodes=barcodes,
            mb_release=release)

        if disambiguation_score > best_match_score:
            best_match_score = disambiguation_score
            best_match_release = release
            # print(f'{best_match_score}% {mb_summarise_release(best_match_release)}')

            if best_match_score == PERFECT_SCORE:
                break

    if best_match_score < MINIMUM_SCORE:
        return None, None, 0

    mb_release_group, mb_release = get_release_and_release_group(mbid=best_match_release.get('id'))

    return mb_release_group, mb_release, best_match_score


def update_tables_after_match(discogs_id, mb_release=None, mb_release_group=None, best_match_score=0):

    if mb_release is None:
        # nothing further can be done with this release
        with db.context_manager() as cur:
            cur.execute("""
                DELETE FROM mb_matches
                WHERE discogs_id = ?""", (discogs_id))

        return

    if not all([
        mb_release.get('label-info-count'),
        mb_release.get('artist-credit-phrase'),
        mb_release.get('medium-count')
    ]):
        # reload the release including the missing sections
        mb_release = musicbrainzngs.get_release_by_id(
            mb_release['id'], includes=['artists', 'artist-credits', 'labels', 'media'])
        if mb_release:
            mb_release = mb_release.get('release')
        else:
            raise Exception("Unable to reload MusicBrainz release")

    load_mbid = 0

    if mb_release_group and not all([
        mb_release_group.get('first-release-date'),
        mb_release_group.get('primary-type'),
    ]):
        load_mbid = mb_release_group['id']

    elif not mb_release_group:
        # No group specified - attempt to load it
        mb_release_details = musicbrainzngs.get_release_by_id(
            mb_release['id'], includes=["release-groups", 'artists', 'artist-credits'])

        mb_release = mb_release_details.get('release')
        if mb_release:
            mb_release_group = mb_release.get('release-group')
            if mb_release_group:
                load_mbid = mb_release_group['id']

    if load_mbid:
        # load the release including the missing sections
        mb_release_group = musicbrainzngs.get_release_group_by_id(
            mb_release_group['id'], includes=['releases', 'media'])
        if mb_release_group:
            mb_release_group = mb_release_group.get('release-group')
        else:
            raise Exception("Unable to reload MusicBrainz release")

    if not mb_release_group:
        raise Exception("Unable to load MusicBrainz release group")

    mbid = mb_release['id']
    artist = mb_release.get('artist-credit-phrase')
    title = mb_release.get('title')
    sort_name = mb_release['artist-credit'][0]['artist']['sort-name']
    country = mb_release.get('country')
    primary_type = mb_release_group.get('primary-type')
    format = mb_get_format(mb_release=mb_release)

    release_date = utils.earliest_date(mb_release.get(
        'date'), mb_release_group.get('first-release-date'))

    row = db_musicbrainz.fetch_row(discogs_id)
    if row:
        # update any changed items
        db_musicbrainz.set_mbid(discogs_id, mb_release['id'])
        db_musicbrainz.set_artist(discogs_id, artist)
        db_musicbrainz.set_title(discogs_id, title)
        db_musicbrainz.set_country(discogs_id, country)
        db_musicbrainz.set_format(discogs_id, format)
        db_musicbrainz.set_score(discogs_id, best_match_score)
        db_musicbrainz.set_primary_type(discogs_id, primary_type)
        db_musicbrainz.set_release_date(discogs_id, release_date)
        db_musicbrainz.set_sort_name(discogs_id, sort_name)

        db_discogs.set_release_date(discogs_id, release_date)
        db_discogs.set_sort_name(discogs_id, sort_name)

    else:
        db_musicbrainz.insert_row(
            discogs_id=discogs_id,
            mbid=mbid,
            artist=artist,
            title=title,
            country=country,
            format=format,
            score=best_match_score,
            primary_type=primary_type,
            release_date=release_date,
            sort_name=sort_name)


def match_discogs_against_mb(callback=print, should_cancel=lambda: False, progress_callback=lambda pct: None):

    # Authentication and useragent setup removed as per instructions.

    rows = db_discogs.fetch_discogs_release_rows()

    for index, row in enumerate(rows):

        if should_cancel():
            callback("Import cancelled.")
            return

        percent = int(((index + 1) / len(rows)) * 100)
        progress_callback(percent)

        callback(f'⚙️ {index+1}/{len(rows)} {db.db_summarise_row(row.discogs_id)}')

        match_release_in_musicbrainz(row.discogs_id)


def mb_get_artist(artist):
    """
        Search for artist by name, returning a single match

        artist name can have more than one exact match

        alias	        (part of) any alias attached to the artist (diacritics are ignored)
        primary_alias	(part of) any primary alias attached to the artist (diacritics are ignored)
        area	        (part of) the name of the artist's main associated area
        arid	        the artist's MBID
        artist	        (part of) the artist's name (diacritics are ignored)
        artistaccent	(part of) the artist's name (with the specified diacritics)
        begin	        the artist's begin date (e.g. "1980-01-22")
        beginarea	    (part of) the name of the artist's begin area
        comment	        (part of) the artist's disambiguation comment
        country	        the 2-letter code (ISO 3166-1 alpha-2) for the artist's main associated country
        end	            the artist's end date (e.g. "1980-01-22")
        endarea	        (part of) the name of the artist's end area
        ended	        a boolean flag (true/false) indicating whether or not the artist has ended (is dissolved/deceased)
        gender	        the artist's gender (“male”, “female”, “other” or “not applicable”)
        ipi	            an IPI code associated with the artist
        isni	        an ISNI code associated with the artist
        sortname	    (part of) the artist's sort name
        tag	            (part of) a tag attached to the artist
        type	        the artist's type (“person”, “group”, etc.)
    """

    print(f'searching for artist {artist}')
    result = musicbrainzngs.search_artists(query=f'artist:"{artist}"')

    if result is None:
        return None

    artist_list = result['artist-list']

    if not artist_list:
        return None

    print(f'found {len(artist_list)} matches')

    # for arow in artist_list:
    #     print(f'    id  : {arow['id']}')
    #     print(f'    name: {arow['name']}')

    # match a single
    artist_index = -1
    for index, value in enumerate(artist_list):
        if value['name'].casefold() == artist.casefold():
            artist_index = index
            break

    if artist_index < 0:
        return None

    # arow = artist_list[artist_index]
    # print(arow)
    # print(f'id  : {arow['id']}')
    # print(f'name: {arow['name']}')

    newlist = [x for x in artist_list if artist.casefold() == x['name'].casefold()]

    return newlist[0]


def get_artist_mbid(discogs_id):
    if discogs_id > 0:
        discogs_url = f"https://www.discogs.com/release/{discogs_id}"
    else:
        discogs_url = None

    if not discogs_url:
        return None

    try:
        mb_url = musicbrainzngs.browse_urls(discogs_url, includes=["artist-rels"])

    except musicbrainzngs.musicbrainz.ResponseError:
        # TODO: Raise appropriate exception. (URL doesn't exist.)
        return None

    if "artist-relation-list" in mb_url["url"]:
        artists = mb_url["url"]["artist-relation-list"]
        if len(artists) == 1:
            return artists[0]["artist"]["id"]
        elif len(artists) > 1:
            # TODO: Raise appropriate exception. (More than one associated artist.)
            return None
    else:
        # TODO: Raise appropriate exception. (No associated artists.)
        return None


def make_discogs_url(release_id=0, master_release_id=0):
    if release_id:
        return f"https://www.discogs.com/release/{release_id}"
    elif master_release_id:
        return f"https://www.discogs.com/master/{master_release_id}"
    else:
        return ''


def mb_match_catno(mb_release, catno):
    # match catalog number(s) against a release (because search by catno is broken)

    if catno is None:
        return False

    lil = mb_release.get('label-info-list')
    if not lil:
        return False

    label_catnos = []
    for x in lil:

        label_catno = x.get('catalog-number')
        if label_catno:
            label_catnos.append(utils.sanitise_identifier(label_catno))

    catno_list = list(set(catno))
    catno_list_str = ','.join(catno_list)

    for full_catno in catno_list:
        catnostring = utils.sanitise_identifier(full_catno)

        for label_catno in label_catnos:
            if catnostring == label_catno:
                return True

    return False


def mb_summarise_release_group(mb_release_group=None, mb_id=None):
    load_mbid = None

    if mb_release_group:
        x = mb_release_group.get('release-list')
        if not x:
            load_mbid = mb_release_group.get('id')
        # x = mb_release_group.get('artists')
        # if not x:
        #     load_mbid = mb_release_group.get('id')
        x = mb_release_group.get('artist-credit-phrase')
        if not x:
            load_mbid = mb_release_group.get('id')
    else:
        load_mbid = mb_id

    if mb_release_group is None and load_mbid == 0:
        raise Exception("Unable to load MusicBrainz release group")

    if load_mbid:
        mb_release_group = musicbrainzngs.get_release_group_by_id(
            load_mbid, includes=['releases', 'artists', 'artist-credits'])
        if mb_release_group:
            mb_release_group = mb_release_group.get('release-group')
        else:
            raise Exception("Unable to load MusicBrainz release group")

    output = []

    mbid = mb_release_group.get('id')
    output.append(f'MB group {mbid}')

    artist = mb_release_group.get('artist-credit-phrase')
    if not artist:
        artist = mb_release_group.get('artist')
    if artist:
        output.append(artist)

    title = mb_release_group.get('title')
    if title:
        output.append(title)

    # release_list = mb_release_group.get('release-list')
    # if release_list:
    #     output.append(f'({len(release_list)} rel)')
    release_count = mb_release_group.get('release-count')
    if release_count:
        output.append(f'({release_count} {"release" if release_count == 1 else "releases"})')

    first_release_date = mb_release_group.get('first-release-date')
    if first_release_date:
        output.append(first_release_date)

    return ' '.join(output)


def mb_find_release(artist='', title='', discogs_id=0, catno=None, primary_type=None, country=None, barcode=None, format=None):
    """Search for a release

    All these fields are searchable.

    alias	        (part of) any alias attached to the release (diacritics are ignored)
    arid	        the MBID of any of the release artists
    artist	        (part of) the combined credited artist name for the release, including join phrases (e.g. "Artist X feat.")
    artistname	    (part of) the name of any of the release artists
    asin	        an Amazon ASIN for the release
    barcode	        the barcode for the release
    catno	        any catalog number for this release (insensitive to case, spaces, and separators)
    comment	        (part of) the release's disambiguation comment
    country	        the 2-letter code (ISO 3166-1 alpha-2) for any country the release was released in
    creditname	    (part of) the credited name of any of the release artists on this particular release
    date	        a release date for the release (e.g. "1980-01-22")
    discids	        the total number of disc IDs attached to all mediums on the release
    discidsmedium	the number of disc IDs attached to any one medium on the release
    format	        the format of any medium in the release (insensitive to case, spaces, and separators)
    laid	        the MBID of any of the release labels
    label	        (part of) the name of any of the release labels
    lang	        the ISO 639-3 code for the release language
    mediums	        the number of mediums on the release
    packaging	    the format of the release (insensitive to case, spaces, and separators)
    primarytype	    the primary type of the release group for this release
    quality	        the listed quality of the data for the release (2 for “high”, 1 for “normal”; cannot search for “low” at the moment; see the related bug report)
    reid	        the release's MBID
    release	        (part of) the release's title (diacritics are ignored)
    releaseaccent	(part of) the release's title (with the specified diacritics)
    rgid	        the MBID of the release group for this release
    script	        the ISO 15924 code for the release script
    secondarytype	any of the secondary types of the release group for this release
    status	        the status of the release
    tag	            (part of) a tag attached to the release
    tracks	        the total number of tracks on the release
    tracksmedium	the number of tracks on any one medium on the release
    type	        legacy release group type field that predates the ability to set multiple types (see calculation code)
"""
    if discogs_id > 0:
        discogs_url = f"https://www.discogs.com/release/{discogs_id}"
    else:
        discogs_url = None

    query = []

    if artist:
        query.append(f'artist:"{artist}"')

    if title:
        query.append(f'release:"{title}"')

    if catno:
        query.append(f'catno:"{catno}"')

    if primary_type:
        query.append(f'primarytype:"{primary_type}"')

    if country:
        query.append(f'country:{country}')

    if barcode:
        query.append(f'barcode:{barcode}')

    if format:
        query.append(f'format:{format}')

    all_results = []
    offset = 0
    batch_size = 25
    max_results = 200

    query_string = ' AND '.join(query)
    # print(f'query_string {query_string}')

    try:
        while len(all_results) < max_results:
            # Fetch results with pagination
            result = musicbrainzngs.search_releases(
                query=query_string, limit=batch_size, offset=offset)

            # Add new results
            releases = result.get('release-list', [])
            all_results.extend(releases)

            if releases:
                for release in releases:
                    # print(mb_summarise_release(release))

                    if mb_match_discogs_release(release.get('id'), discogs_url=discogs_url):
                        print(f'✅ matched {mb_summarise_release(release)}')
                        return release

                if len(releases) == 1:
                    # didn't match Discogs release URL, but there's only one result, so return it anyway
                    print(f'✅ matched {mb_summarise_release(release)}')
                    return release

                # if catno and mb_match_catno(release, catno):
                #     return release

                # if len(releases) == 1:
                #     # didn't match Discogs release URL, but there's only one result, so return it anyway
                #     return release

            # Check if we've reached the end
            if len(releases) < batch_size:
                break

            # Move to the next page
            offset += batch_size

            # print(f'{len(all_results)} result(s) from query {query_string}')

            # for release in all_results:
            #     print(mb_summarise_release(release))

        return None

    except Exception as e:
        print(f"Error: {e}")
        # return []
        return None


def mb_get_format(mb_release=None, mbid=None):

    format = ''
    load_mbid = None

    if mb_release:
        if not all([
            mb_release.get('medium-count')
        ]):
            load_mbid = mb_release.get('id')
    else:
        load_mbid = mbid

    if mb_release is None and load_mbid == 0:
        raise Exception("Unable to load MusicBrainz release")

    if load_mbid:
        mb_release = musicbrainzngs.get_release_by_id(
            load_mbid, includes=['artists', 'artist-credits', 'labels', 'media'])
        if mb_release:
            mb_release = mb_release.get('release')
        else:
            raise Exception("Unable to load MusicBrainz release")

    if not mb_release:
        raise Exception("Unable to load MusicBrainz release")

    medium_list = mb_release.get('medium-list')
    if medium_list and len(medium_list) > 0:
        for medium in medium_list:
            format = medium.get('format')
            if format:
                break

    return format


def summarise(
        artist=None,
        title=None,
        country=None,
        format=None,
        catnos=None,
        barcodes=None):

    output = []

    if artist:
        output.append(artist)

    if title:
        output.append(title)

    if country:
        output.append(country)

    if format:
        output.append(format)

    if catnos and len(catnos):
        output.append(','.join(catnos))

    if barcodes and len(barcodes):
        output.append(','.join(barcodes))

    return ' '.join(output)


def output_match_summary(
        artist=None,
        title=None,
        country=None,
        format=None,
        catnos=None,
        barcodes=None,
        score=0):

    output = []

    output.append(score_stars(score))
    output.append(f'{score}%')

    if artist:
        output.append(artist)

    if title:
        output.append(title)

    if country:
        output.append(country)

    if format:
        output.append(format)

    if catnos and len(catnos):
        catnos = utils.normalize_identifier_list(catnos)
        output.append(','.join(catnos))

    if barcodes and len(barcodes):
        barcodes = utils.normalize_identifier_list(barcodes)
        output.append(','.join(barcodes))

    print(' '.join(output))
