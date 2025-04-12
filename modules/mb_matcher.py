#!/usr/bin/env python3

# MusicBrainz matching logic

import logging

from .utils import earliest_date, sanitise_identifier, summarise_release
from .db import db_summarise_row, fetch_discogs_release_rows, insert_mb_matches_row, update_mb_score
from .db import update_mb_release_date, update_mb_artist, update_mb_mbid
from .db import update_mb_primary_type, update_mb_title, update_release_date, update_mb_sort_name
from .db import fetch_musicbrainz_row, db_ops, fetch_discogs_release, update_mb_country, update_mb_format
from .utils import convert_country_from_discogs_to_musicbrainz
import musicbrainzngs
from discogs_client.exceptions import HTTPError
from musicbrainzngs import musicbrainz
import sys
from rapidfuzz import fuzz
from .utils import convert_format, sanitise_identifier, trim_if_ends_with_number_in_brackets

logger = logging.getLogger(__name__)

SCORE_WEIGHTS = {
    "artist": 100,
    "title": 100,
    "country": 100,
    "format": 100,
    "barcode": 100,
    "unilateral_barcode": 30,
    "no_barcode": 60,
    "catno": 100,
    "unilateral_catno": 30,
    "no_catno": 60
}

MAXIMUM_SCORE = SCORE_WEIGHTS['artist'] + SCORE_WEIGHTS['title'] + SCORE_WEIGHTS['country'] + \
    SCORE_WEIGHTS['format']+SCORE_WEIGHTS['barcode']+SCORE_WEIGHTS['catno']

PERFECT_SCORE = 100
MINIMUM_SCORE = 25


def mb_normalize_artist(name):
    artist = trim_if_ends_with_number_in_brackets(name)
    if artist == 'Various':
        artist = 'Various Artists'
    return artist


def mb_normalize_title(title):
    return title


def mb_normalize_format(discogs_format):
    if not discogs_format:
        raise Exception('format error')

    parts = discogs_format.split(':')

    primary_type = parts[0].strip()
    secondary_types = []
    for tmp in parts[1].split(','):
        secondary_types.append(tmp.strip())

    if primary_type == 'Vinyl':

        if 'EP' in secondary_types and '7"' in secondary_types:
            format = '7" EP'
            mb_primary_type = 'single'
        elif 'EP' in secondary_types and '12"' in secondary_types:
            format = '12" EP'
            mb_primary_type = 'single'
        elif '12"' in secondary_types:
            format = '12" Single'
            mb_primary_type = 'single'
        elif '7"' in secondary_types:
            format = '7" Single'
            mb_primary_type = 'single'
        elif 'Compilation' in secondary_types:
            format = 'LP Compilation'
            mb_primary_type = 'album'
        elif 'LP' in secondary_types:
            format = 'LP'
            mb_primary_type = 'album'
        else:
            format = '?'
            mb_primary_type = "?"

    elif primary_type == 'CD':

        if 'Mini' in secondary_types:
            format = 'CD 3" Mini Single'
            mb_primary_type = 'single'
        elif 'Single' in secondary_types:
            format = 'CD Single'
            mb_primary_type = 'single'
        elif 'Maxi-Single' in secondary_types:
            format = 'CD Single'
            mb_primary_type = 'single'
        elif 'EP' in secondary_types:
            format = 'CD EP'
            mb_primary_type = 'single'
        elif 'HDCD' in secondary_types and 'Album' in secondary_types:
            format = 'CD HDCD Album'
            mb_primary_type = 'album'
        elif 'LP' in secondary_types:
            format = 'CD Album'
            mb_primary_type = 'album'
        elif 'Album' in secondary_types:
            format = 'CD Album'
            mb_primary_type = 'album'
        elif 'Mini-Album' in secondary_types:
            format = 'CD Mini-Album'
            mb_primary_type = 'album'
        elif 'Compilation' in secondary_types:
            format = 'CD Compilation'
            mb_primary_type = 'album'
        else:
            format = "CD Single"
            mb_primary_type = "single"

    elif primary_type == 'Flexi-disc':

        if '7"' in secondary_types:
            format = '7" flexi-disc'
            mb_primary_type = 'single'
        else:
            format = '?'
            mb_primary_type = "?"

    elif primary_type == 'Box Set':

        if '12"' in secondary_types:
            format = '12" Singles Box Set'
            mb_primary_type = 'single'
        elif '7"' in secondary_types:
            format = '7" Singles Box Set'
            mb_primary_type = 'single'
        elif 'LP' in secondary_types:
            format = "LP Box Set"
            mb_primary_type = 'album'
        elif 'EP' in secondary_types:
            format = "EP Box Set"
            mb_primary_type = 'single'
        elif 'Single' in secondary_types:
            format = 'Singles Box Set'
            mb_primary_type = 'single'
        elif 'Maxi-Single' in secondary_types:
            format = 'Maxi-singles Box Set'
            mb_primary_type = 'single'
        else:
            format = 'Box Set'
            mb_primary_type = "Other"
    else:
        format = '?'
        mb_primary_type = "?"

    return format, mb_primary_type, primary_type

    # for format in discogs_release.formats:
    #     format_desc = ','.join(format.get('descriptions'))
    # # formats = [x.data['name'] for x in release.formats]

    # format, mb_primary_type, mb_format = convert_format(discogs_release.formats[0])


def convert_format(discogs_format):

    mb_format = discogs_format.get('name')  # for example CD
    primary_type = discogs_format.get('name')  # for example CD
    quantity = discogs_format.get('qty')
    secondary_types = discogs_format.get('descriptions')  # for example album

    if primary_type == 'Vinyl':

        if 'EP' in secondary_types and '7"' in secondary_types:
            format = '7" EP'
            mb_primary_type = 'single'
        elif 'EP' in secondary_types and '12"' in secondary_types:
            format = '12" EP'
            mb_primary_type = 'single'
        elif '12"' in secondary_types:
            format = '12" Single'
            mb_primary_type = 'single'
        elif '7"' in secondary_types:
            format = '7" Single'
            mb_primary_type = 'single'
        elif 'Compilation' in secondary_types:
            format = 'LP Compilation'
            mb_primary_type = 'album'
        elif 'LP' in secondary_types:
            format = 'LP'
            mb_primary_type = 'album'
        else:
            format = '?'
            mb_primary_type = "?"

    elif primary_type == 'CD':

        if 'Mini' in secondary_types:
            format = 'CD 3" Mini Single'
            mb_primary_type = 'single'
        elif 'Single' in secondary_types:
            format = 'CD Single'
            mb_primary_type = 'single'
        elif 'Maxi-Single' in secondary_types:
            format = 'CD Single'
            mb_primary_type = 'single'
        elif 'EP' in secondary_types:
            format = 'CD EP'
            mb_primary_type = 'single'
        elif 'LP' in secondary_types:
            format = 'CD Album'
            mb_primary_type = 'album'
        elif 'Album' in secondary_types:
            format = 'CD Album'
            mb_primary_type = 'album'
        elif 'Mini-Album' in secondary_types:
            format = 'CD Mini-Album'
            mb_primary_type = 'album'
        elif 'Compilation' in secondary_types:
            format = 'CD Compilation'
            mb_primary_type = 'album'
        else:
            format = "CD Single"
            mb_primary_type = "single"

    elif primary_type == 'Flexi-disc':

        if '7"' in secondary_types:
            format = '7" flexi-disc'
            mb_primary_type = 'single'
        else:
            format = '?'
            mb_primary_type = "?"

    elif primary_type == 'Box Set':

        if '12"' in secondary_types:
            format = '12" Singles Box Set'
            mb_primary_type = 'single'
        elif '7"' in secondary_types:
            format = '7" Singles Box Set'
            mb_primary_type = 'single'
        elif 'LP' in secondary_types:
            format = "LP Box Set"
            mb_primary_type = 'album'
        elif 'EP' in secondary_types:
            format = "EP Box Set"
            mb_primary_type = 'single'
        elif 'Single' in secondary_types:
            format = 'Singles Box Set'
            mb_primary_type = 'single'
        elif 'Maxi-Single' in secondary_types:
            format = 'Maxi-singles Box Set'
            mb_primary_type = 'single'
        else:
            format = 'Box Set'
            mb_primary_type = "Other"
    else:
        format = '?'
        mb_primary_type = "?"

    return format, mb_primary_type, primary_type


def mb_normalize_country(country):
    return convert_country_from_discogs_to_musicbrainz(country)


def mb_normalize_barcodes(barcodes):
    mb_barcodes = []

    if barcodes is None:
        return mb_barcodes

    if isinstance(barcodes, str):
        for tmp in barcodes.split(','):
            mb_barcodes.append(tmp.strip())
        return mb_barcodes
    elif isinstance(barcodes, list):
        for tmp in barcodes:
            mb_barcodes.append(tmp.strip())
        return mb_barcodes
    elif isinstance(barcodes, set):
        for tmp in barcodes:
            mb_barcodes.append(tmp.strip())
        return mb_barcodes
    else:
        return mb_barcodes

    # barcodes = []
    # for identifier in identifiers:
    #     if identifier['type'].casefold() == 'barcode':
    #         barcodes.append(sanitise_identifier(identifier['value']))
    # return ', '.join(sorted(set(barcodes)))
    # # get the barcode
    # barcode = None
    # barcodes = []
    # discogs_identifiers = discogs_release.fetch('identifiers')
    # for identifier in discogs_identifiers:
    #     if identifier['type'] == 'Barcode':
    #         barcode = sanitise_identifier(identifier['value'])
    #         barcodes.append(sanitise_identifier(identifier['value']))
    # barcodes = list(set(barcodes))


def barcode_match_scorer(mb_barcodes, barcodes):
    # note that Discogs can have multiple barcodes, MusicBrainz only has one.

    if isinstance(mb_barcodes, str):
        tmp_mb_barcodes = mb_barcodes
        mb_barcodes = []
        for tmp in tmp_mb_barcodes.split(','):
            mb_barcodes.append(tmp.strip())

    elif mb_barcodes is None:
        mb_barcodes = []

    if isinstance(barcodes, str):
        # convert barcodes string to a list
        tmp_barcodes = barcodes
        barcodes = []
        for tmp in tmp_barcodes.split(','):
            barcodes.append(tmp.strip())

    elif barcodes is None:
        # no barcode on the Discogs side
        barcodes = []

    if len(mb_barcodes) > 0 and len(barcodes) == 0:
        # only in MusicBrainz
        return SCORE_WEIGHTS['unilateral_barcode']
    elif len(mb_barcodes) == 0 and len(barcodes) > 0:
        # only in Discogs
        return SCORE_WEIGHTS['unilateral_barcode']
    elif len(mb_barcodes) == 0 and len(barcodes) == 0:
        # neither side
        return SCORE_WEIGHTS['no_barcode']

    for barcode in barcodes:
        if barcode in mb_barcodes:
            return SCORE_WEIGHTS['barcode']

    return 0


def mb_normalize_catnos(catnos):

    mb_catnos = []

    if catnos is None:
        return mb_catnos

    if isinstance(catnos, str):
        for tmp in catnos.split(','):
            mb_catnos.append(tmp.strip())
        return mb_catnos
    elif isinstance(catnos, list):
        for tmp in catnos:
            mb_catnos.append(tmp.strip())
        return mb_catnos
    elif isinstance(catnos, set):
        for tmp in catnos:
            mb_catnos.append(tmp.strip())
        return mb_catnos
    else:
        return mb_catnos


def catno_match_scorer(mb_catnos, catnos):

    if isinstance(mb_catnos, str):
        tmp_catnos = mb_catnos
        mb_catnos = []
        for tmp in tmp_catnos.split(','):
            mb_catnos.append(tmp.strip())

    elif mb_catnos is None:
        mb_catnos = []

    if isinstance(catnos, str):
        tmp_catnos = catnos
        catnos = []
        for tmp in tmp_catnos.split(','):
            catnos.append(tmp.strip())

    elif catnos is None:
        catnos = []

    if len(mb_catnos) > 0 and len(catnos) == 0:
        # only in MusicBrainz
        return SCORE_WEIGHTS['unilateral_catno']
    elif len(mb_catnos) == 0 and len(catnos) > 0:
        # only in Discogs
        return SCORE_WEIGHTS['unilateral_catno']
    elif len(mb_catnos) == 0 and len(catnos) == 0:
        # neither side
        return SCORE_WEIGHTS['no_catno']

    for catno in catnos:
        if catno in mb_catnos:
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
        mb_catnos.append(find_catno)
    mb_catnos = list(set(mb_catnos))

    artist_score = fuzz.QRatio(
        artist, mb_artist) * SCORE_WEIGHTS['artist'] / 100
    title_score = fuzz.QRatio(title, mb_title) * SCORE_WEIGHTS['title'] / 100
    country_score = fuzz.QRatio(country, mb_country) * SCORE_WEIGHTS['country'] / 100
    format_score = fuzz.QRatio(format, mb_format) * SCORE_WEIGHTS['format'] / 100
    barcode_score = barcode_match_scorer(barcodes, mb_barcode)
    catno_score = catno_match_scorer(catnos, mb_catnos)

    total_points = artist_score+title_score+country_score+format_score+catno_score+barcode_score

    return round(100 * total_points/MAXIMUM_SCORE)


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

    except musicbrainzngs.musicbrainz.ResponseError:
        # probably 404
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
        x = mb_release.get('medium-list')
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

    lil = mb_release.get('label-info-list')
    if lil:
        label_catnos = []
        for x in lil:

            label_catno = x.get('catalog-number')
            if label_catno:
                label_catnos.append(label_catno)
        catnos = ','.join(label_catnos)
        output.append(catnos)

    media = mb_release.get('medium-list')
    if media and len(media):
        first_media = media[0]
        format = first_media.get('format')
        if format:
            output.append(format)

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


def match_by_discogs_master_link(artist=None, title=None, country=None, format=None, barcodes=[], catnos=[], master_id=0):
    """Search MusicBrainz by Discogs release URL

    If the search fails, the master URL is also tried.

    Returns the release, release group and release date if successful."""

    mb_release = None
    mb_release_group = None

    best_match_score = 0
    best_match_release = None
    best_match_release_group = None

    # try to find the master/release group link
    release_groups = mb_browse_release_groups_by_discogs_master_link(master_id)
    if not release_groups or len(release_groups) == 0:
        # no match found
        return None, None, 0

    if len(release_groups) == 1:
        all_releases = mb_get_releases_for_release_group(mb_id=release_groups[0].get('id'))

    else:
        # more than 1 release group matched
        all_releases = []
        for release_group in release_groups:
            releases = mb_get_releases_for_release_group(mb_id=release_group.get('id'))
            all_releases.extend(releases)

    best_match_release_group, best_match_release, best_match_score = disambiguate_releases(
        all_releases,
        artist=artist,
        title=title,
        country=country,
        format=format,
        catnos=catnos,
        barcodes=barcodes)

    if best_match_score < MINIMUM_SCORE:
        # nothing matched
        return None, None, 0

    if best_match_score == PERFECT_SCORE:
        print(f'💯 {best_match_score}% match {mb_summarise_release(mbid=best_match_release.get('id'))}')
    elif best_match_score > MINIMUM_SCORE:
        print(f'📈 {best_match_score}% match {mb_summarise_release(mbid=best_match_release.get('id'))}')

    mb_release_group, mb_release = get_release_and_release_group(mb_release=best_match_release)
    return mb_release_group, mb_release, best_match_score


def find_match_by_discogs_link(artist=None, title=None, country=None, format=None, barcodes=[], catnos=[], discogs_id=0, master_id=0):
    """Search MusicBrainz by Discogs release URL

    If the search fails, the master URL is also tried.

    Returns the release, release group and release date if successful."""

    mb_release = None
    mb_release_group = None

    best_match_score = 0
    best_match_release = None
    best_match_release_group = None

    releases = mb_browse_releases_by_discogs_release_link(discogs_id=discogs_id)
    if not releases:
        # nothing found that matches the Discogs release
        if not master_id:
            # no master specified for this Discogs release, cannot continue
            return None, None, 0

        best_match_score = 0
        best_match_release = None

    elif len(releases) == 1:
        print(f'🎯 matched {mb_summarise_release(mbid=mb_release.get('id'))}')
        best_match_score = PERFECT_SCORE
        best_match_release = releases[0]

    else:
        # more than one match seems unusual, but possible
        best_match_score = 0
        best_match_release = None

        for release in releases:
            disambiguation_score = disambiguator_score(
                artist=artist, title=title, country=country, format=format, catnos=catnos, barcodes=barcodes, mb_release=release)

            if disambiguation_score > best_match_score:
                best_match_score = disambiguation_score
                best_match_release = release
                if best_match_score == PERFECT_SCORE:
                    break

    if best_match_score > MINIMUM_SCORE:
        # success - load the release group too
        mb_release = musicbrainzngs.get_release_by_id(best_match_release['id'], includes=[
                                                      "release-groups", 'artists', 'artist-credits'])
        mb_release = mb_release.get('release')

        release_group_id = mb_release.get('release-group').get('id')

        mb_release_group = musicbrainzngs.get_release_group_by_id(
            release_group_id, includes=['artists', 'releases', 'artist-credits'])
        mb_release_group = mb_release_group.get('release-group')

        return mb_release_group, mb_release, best_match_score

    # try to find the release group link instead
    release_groups = mb_browse_release_groups_by_discogs_master_link(master_id)
    if not release_groups:
        # no match found
        mb_release_group = None
    elif len(release_groups) == 1:
        mb_release_group = release_groups[0]
        # we are only expecting 1 match
        best_match_score = PERFECT_SCORE
    else:
        # more than 1 release group match
        all_releases = []
        for release_group in release_groups:
            releases = mb_get_releases_for_release_group(mb_id=release_group.get('id'))
            all_releases.extend(releases)

        best_match_release_group, best_match_release, best_match_score = disambiguate_releases(
            releases,
            artist=artist,
            title=title,
            country=country,
            format=format,
            catnos=catnos,
            barcodes=barcodes)

        if best_match_score == PERFECT_SCORE:
            print(
                f'💯 {best_match_score}% match {mb_summarise_release(mbid=best_match_release.get('id'))}')
        elif best_match_score > MINIMUM_SCORE:
            print(
                f'📈 {best_match_score}% match {mb_summarise_release(mbid=best_match_release.get('id'))}')
        else:
            # nothing matched
            return None, None, None, 0

        return best_match_release_group, best_match_release, best_match_score

    if not mb_release_group:
        # failed to find the group
        return None, None, None, 0

    batch_offset = 0
    batch_size = 25
    max_results = 200
    release_list = []

    try:
        while len(release_list) < max_results:
            rels = musicbrainzngs.browse_releases(
                release_group=mb_release_group.get('id'),
                includes=['artist-credits', 'labels', 'media'],
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
        logging.error(e)
        rels = []

    if not release_list:
        # no releases, shouldn't happen
        return None, None, None, 0

    best_match_release_group, best_match_release, best_match_score = disambiguate_releases(
        releases,
        artist=artist,
        title=title,
        country=country,
        format=format,
        catnos=catnos,
        barcodes=barcodes)

    if best_match_score == PERFECT_SCORE:
        print(f'💯 {best_match_score}% match {mb_summarise_release(mbid=best_match_release.get('id'))}')
    elif best_match_score > MINIMUM_SCORE:
        print(f'📈 {best_match_score}% match {mb_summarise_release(mbid=best_match_release.get('id'))}')
    else:
        # nothing matched
        return None, None, 0

    return best_match_release_group, best_match_release, best_match_score


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


def mb_find_release_group_and_release(artist=None, title=None, country=None, format=None, catnos=None, barcodes=None, primary_type=None, discogs_id=0):
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

    if discogs_id > 0:
        discogs_url = f"https://www.discogs.com/release/{discogs_id}"
    else:
        discogs_url = None

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

    best_match_score = 0
    best_match_release = None
    best_match_release_group = None

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

                for release in gr_release_list:
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
                        best_match_release_group = gr
                        if best_match_score == PERFECT_SCORE:
                            break

                if best_match_score == PERFECT_SCORE:
                    break

            if best_match_score == PERFECT_SCORE:
                break

        # Check if we've reached the end
        if len(release_group_list) < batch_size:
            break

        # Move to the next page
        offset += batch_size

    if best_match_score < MINIMUM_SCORE:
        # nothing matched
        return None, None, 0

    if best_match_score == PERFECT_SCORE:
        print(
            f'💯 {best_match_score}% match {mb_summarise_release(mbid=best_match_release.get('id'))}')
    elif best_match_score > MINIMUM_SCORE:
        print(
            f'📈 {best_match_score}% match {mb_summarise_release(mbid=best_match_release.get('id'))}')

    return best_match_release, best_match_release_group, best_match_score


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


def match_release_in_musicbrainz(discogs_id):

    row = fetch_discogs_release(discogs_id)
    if not row:
        raise Exception('Error loading row')

    # translate some columns
    discogs_id = row.discogs_id
    artist = mb_normalize_artist(row.artist)
    title = mb_normalize_title(row.title)
    country = mb_normalize_country(row.country)
    catnos = mb_normalize_catnos(row.catnos)
    barcodes = mb_normalize_barcodes(row.barcodes)
    _, _, format = mb_normalize_format(row.format)

    # try the Discogs release link first
    mb_release_group, mb_release, best_match_score = match_by_discogs_release_link(
        discogs_id=discogs_id,
        artist=artist,
        title=title,
        country=country,
        catnos=catnos,
        barcodes=barcodes,
        format=format)

    if mb_release:
        update_tables_after_match(
            discogs_id=discogs_id,
            mb_release=mb_release,
            mb_release_group=mb_release_group,
            best_match_score=best_match_score)
        return

    if row.master_id:
        # try the master release to release group link
        mb_release_group, mb_release, best_match_score = match_by_discogs_master_link(
            master_id=row.master_id,
            artist=artist,
            title=title,
            country=country,
            catnos=catnos,
            barcodes=barcodes,
            format=format)

        if mb_release:
            update_tables_after_match(
                discogs_id=discogs_id,
                mb_release=mb_release,
                mb_release_group=mb_release_group,
                best_match_score=best_match_score)
            return

    # try some other searches
    for barcode in barcodes:
        result = musicbrainzngs.search_releases(barcode=barcode)
        releases = result['release-list']
        if len(releases):
            best_release_group, best_release, best_match_score = disambiguate_releases(
                releases,
                artist=artist,
                title=title,
                country=country,
                format=format,
                catnos=catnos,
                barcodes=barcodes
            )

            if best_match_score > MINIMUM_SCORE:
                return best_release_group, best_release, best_match_score

    for catno in catnos:
        result = musicbrainzngs.search_releases(catno=catno)
        releases = result['release-list']
        if len(releases):
            best_release_group, best_release, best_match_score = disambiguate_releases(
                releases,
                artist=artist,
                title=title,
                country=country,
                format=format,
                catnos=catnos,
                barcodes=barcodes
            )

            if best_match_score > MINIMUM_SCORE:
                return best_release_group, best_release, best_match_score

    # attempt to match release group and release, and derive release date
    mb_release_group, mb_release, best_match_score = mb_find_release_group_and_release(
        artist=artist,
        title=title,
        country=country,
        catnos=catnos,
        barcodes=barcodes,
        primary_type=format,
        discogs_id=discogs_id)

    if mb_release:
        update_tables_after_match(
            discogs_id=discogs_id,
            mb_release=mb_release,
            mb_release_group=mb_release_group,
            best_match_score=best_match_score)
        return

    candidates = []

    for catno in catnos:
        candidates.extend(mb_find_releases(
            artist=artist,
            catno=catno,
            country=country,
            format=format,
        ))

    for barcode in barcodes:
        candidates.extend(mb_find_releases(
            artist=artist,
            barcode=barcode,
            country=country,
            format=format,
        ))

    for catno in catnos:
        candidates.extend(mb_find_releases(
            artist=artist,
            catno=catno,
            format=format,
        ))

    for catno in catnos:
        candidates.extend(mb_find_releases(
            title=title,
            catno=catno
        ))

    for barcode in barcodes:
        candidates.extend(mb_find_releases(
            title=title,
            barcode=barcode
        ))

    for catno in catnos:
        candidates.extend(mb_find_releases(
            artist=artist,
            title=title,
            catno=catno
        ))

    for catno in catnos:
        candidates.extend(mb_find_releases(
            artist=artist,
            title=title,
            catno=catno,
            primary_type=format,
            country=country
        ))

    best_match_release_group, best_match_release, best_match_score = disambiguate_releases(
        candidates,
        artist=artist,
        title=title,
        country=country,
        format=format,
        catnos=catnos,
        barcodes=barcodes)

    if best_match_score == PERFECT_SCORE:
        print(f'💯 {best_match_score}% match {mb_summarise_release(mbid=best_match_release.get('id'))}')
    elif best_match_score > MINIMUM_SCORE:
        print(
            f'📈 {best_match_score}% match {mb_summarise_release(mbid=best_match_release.get('id'))}')
    else:
        print(f'❌ {best_match_score}% match')
        return None, None, 0

    update_tables_after_match(
        discogs_id=discogs_id,
        mb_release=best_match_release,
        mb_release_group=best_match_release_group,
        best_match_score=best_match_score)


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
            artist=artist, title=title, country=country, format=format, catnos=catnos, barcodes=barcodes, mb_release=release)

        if disambiguation_score > best_match_score:
            best_match_score = disambiguation_score
            best_match_release = release
            if best_match_score == PERFECT_SCORE:
                break

    if best_match_score < MINIMUM_SCORE:
        return None, None, 0

    # if best_match_score == 100:
    #     print(f'💯 {best_match_score}% match {mb_summarise_release(mbid=best_match_release.get('id'))}')
    # elif best_match_score > 0:
    #     print(f'📈 {best_match_score}% match {mb_summarise_release(mbid=best_match_release.get('id'))}')

    mb_release_group, mb_release = get_release_and_release_group(mbid=best_match_release.get('id'))

    return mb_release_group, mb_release, best_match_score


def update_tables_after_match(discogs_id, mb_release=None, mb_release_group=None, best_match_score=0):

    if mb_release is None:
        # nothing further can be done with this release at this time, but certain fields
        # should be unset if present
        # TODO more columns? delete row? unset matched at, for example
        with db_ops() as cur:
            cur.execute("""
                UPDATE mb_matches
                SET mbid = ?, artist = ?, title = ?
                WHERE discogs_id = ?""", (None, None, None, discogs_id))

        return

    if not all([
        mb_release.get('label-info-count'),
        mb_release.get('artist-credit-phrase'),
        mb_release.get('medium-list')
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
    primary_type = mb_release_group['primary-type']
    format = mb_release['medium-list'][0]['format']

    release_date = earliest_date(mb_release.get('date'), mb_release_group['first-release-date'])

    row = fetch_musicbrainz_row(discogs_id)
    if row:
        # update any changed items
        update_mb_mbid(discogs_id, mb_release['id'])
        update_mb_artist(discogs_id, artist)
        update_mb_title(discogs_id, title)
        update_mb_sort_name(discogs_id, sort_name)
        update_mb_country(discogs_id, country)
        update_mb_format(discogs_id, format)
        update_mb_score(discogs_id, best_match_score)
        update_mb_primary_type(discogs_id, primary_type)
        update_mb_release_date(discogs_id, release_date)

    else:
        insert_mb_matches_row(
            discogs_id=discogs_id,
            mbid=mbid,
            artist=artist,
            title=title,
            sort_name=sort_name,
            country=country,
            format=format,
            score=best_match_score,
            primary_type=primary_type,
            release_date=release_date)

    update_release_date(discogs_id, release_date)


def match_discogs_against_mb(config=None):

    musicbrainz.set_useragent(app=config.user_agent,
                              version=config.app_version, contact=config.email)

    try:
        musicbrainz.auth(config.username, config.password)

    except HTTPError:
        logging.error("Unable to authenticate to Discogs.")
        sys.exit(1)

    except Exception as e:
        logging.error(f'MusicBrainz authentication error {e}')
        sys.exit(1)

    musicbrainzngs.set_rate_limit(1, 1)

    rows = fetch_discogs_release_rows()

    for index, row in enumerate(rows):

        print(f'⚙️ {index+1}/{len(rows)} {db_summarise_row(row.discogs_id)}')

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
            label_catnos.append(sanitise_identifier(label_catno))

    catno_list = list(set(catno))
    catno_list_str = ','.join(catno_list)

    for full_catno in catno_list:
        catnostring = sanitise_identifier(full_catno)

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
        output.append(f'({release_count} {'release' if release_count == 1 else 'releases'})')

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
