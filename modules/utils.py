#!/usr/bin/env python3

# Shared helper functions

import datetime
import re
import logging
import json

import dateparser
import dateutil


logger = logging.getLogger(__name__)

try:
    with open("config/countries.json", "r", encoding="utf-8") as f:
        COUNTRIES = json.load(f)
except FileNotFoundError:
    logger.error("countries.json not found. Please check your config path.")
    COUNTRIES = {}

try:
    with open("config/mediatypes.json", "r", encoding="utf-8") as f:
        MEDIATYPES = json.load(f)
except FileNotFoundError:
    logger.error("countries.json not found. Please check your config path.")
    MEDIATYPES = {}


def normalize_country_name(name):
    """
    Normalize and canonicalize country names to improve matching with COUNTRIES mapping.
    Handles leading 'The', trailing ', The', and common formatting inconsistencies.
    """
    if not name:
        return None

    # name = name.strip().lower()
    name = name.strip()

    # Move trailing ', the' to the front: 'Bahamas, The' -> 'The Bahamas'
    if name.endswith(', the'):
        name = 'the ' + name[:-5].strip()
    elif name.endswith(', The'):
        name = 'The ' + name[:-5].strip()

    # Remove leading 'the' if present (optional, depending on how COUNTRIES is structured)
    if name.startswith("the "):
        name = name[4:]
    elif name.startswith("The "):
        name = name[4:]

    # Normalize punctuation and spacing
    name = name.replace("&", "and").replace(",", "").replace("  ", " ")
    return name
    # return " ".join(word.capitalize() for word in name.split())


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

    return format, mb_primary_type, mb_format


def sanitise_identifier(catno):
    catno_string = ''.join(chr for chr in catno if chr.isalnum()).casefold() or chr == '-'
    return catno_string


def normalize_identifier_list(value):
    """Normalise a string or list of identifiers into a list.

    Identifiers are barcodes and catalog numbers. A (comma separated) string
    or a list of strings is converted into a list of normalized values."""

    value_set = []

    if value is None:
        pass

    elif isinstance(value, str):
        for tmp in value.split(','):
            value_set.append(sanitise_identifier(tmp.strip()))

    elif isinstance(value, list) or isinstance(value, set):
        for tmp in value:
            value_set.append(sanitise_identifier(tmp.strip()))

    return list(value_set)


def trim_if_ends_with_number_in_brackets(s):
    pattern = r' \(([1-9]\d*)\)$'
    return re.sub(pattern, '', s)


def sanitise_compare_string(tmp):
    if tmp is None:
        return ''

    tmp = tmp.casefold()
    tmp = ''.join(chr for chr in tmp if (chr.isalnum()) or chr == ' ')

    output = ' '.join(tmp.split())
    return output


def convert_country_from_discogs_to_musicbrainz(discogs_country):
    musicbrainz_country = COUNTRIES.get(normalize_country_name(discogs_country))
    if not musicbrainz_country:
        pass
        logger.warning(f"⚠️ Unknown country mapping for: {discogs_country}")
    return musicbrainz_country


def parse_date(date_str):

    if date_str == '0':
        return None

    if date_str and not isinstance(date_str, str):
        date_str = str(date_str)

    date_formats = ['%Y-%m-%d', '%Y-%m', '%Y']
    settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first'}

    if date_str is None or date_str == '':
        return None

    date_object = dateparser.parse(date_str, date_formats=date_formats, settings=settings)

    return date_object.date()


def earliest_date(dt1_str, dt2_str):
    """ Return the earlier of two dates

        Given two date strings, return the earlier of the two. Dates
        can be in the form YYYY-MM-DD, YYYY-MM or simply YY. The
        comparison takes into account partial dates, so that 1982-04
        is not considered earlier than 1982-04-10, but would be
        considered earlier than 1982-05
    """

    if not dt1_str:
        dt1_str = None
    if not dt2_str:
        dt2_str = None

    if dt1_str == '0' or dt1_str == 'None':
        dt1_str = None
    if dt2_str == '0' or dt2_str == 'None':
        dt2_str = None

    if dt1_str and isinstance(dt1_str, int):
        dt1_str = str(dt1_str)

    if dt2_str and isinstance(dt2_str, int):
        dt2_str = str(dt2_str)

    if dt1_str == dt2_str:
        return dt1_str if dt1_str else None

    dt1_obj = parse_date(dt1_str) if dt1_str else None
    dt2_obj = parse_date(dt2_str) if dt2_str else None

    if dt1_obj is None and dt2_obj is None:
        return None

    if dt1_obj is not None and dt2_obj is None:
        return dt1_str

    if dt1_obj is None and dt2_obj is not None:
        return dt2_str

    if dt1_obj.year < dt2_obj.year:
        return dt1_str

    if dt1_obj.year > dt2_obj.year:
        return dt2_str

    # year is the same
    # if one has a month and the other doesn't, it wins
    if len(dt1_str) == 4 and len(dt2_str) >= 7:
        return dt2_str

    if len(dt1_str) >= 7 and len(dt2_str) == 4:
        return dt1_str

    if dt1_obj.month < dt2_obj.month:
        return dt1_str

    if dt1_obj.month > dt2_obj.month:
        return dt2_str

    # year and month are the same
    # if one has a day and the other doesn't, it wins
    if len(dt1_str) == 7 and len(dt2_str) == 10:
        return dt2_str

    if len(dt1_str) == 10 and len(dt2_str) == 7:
        return dt1_str

    if dt1_obj.day < dt2_obj.day:
        return dt1_str

    if dt1_obj.day > dt2_obj.day:
        return dt2_str

    return dt1_str


def parse_and_humanize_date(ymd_date):

    if not ymd_date:
        return ''

    if len(ymd_date) == 10:
        # try day, month and year match first
        formats = ["%Y-%m-%d"]
        settings = {'REQUIRE_PARTS': ['day', 'month', 'year']}
        date_object = dateparser.parse(ymd_date, date_formats=formats, settings=settings)

        if date_object:
            return date_object.strftime('%A %-d %B %Y')

    if len(ymd_date) == 7:
        # try month and year
        formats = ["%Y-%m"]
        settings = {'REQUIRE_PARTS': ['month', 'year']}

        date_object = dateparser.parse(ymd_date, date_formats=formats, settings=settings)

        if date_object:
            return date_object.strftime("%B %Y")

    if len(ymd_date) == 4:
        # try for just a year
        formats = ["%Y"]
        settings = {'REQUIRE_PARTS': ['year']}

        date_object = dateparser.parse(ymd_date, date_formats=formats, settings=settings)

        if date_object:
            return date_object.strftime("%Y")

    return ''


def humanize_date_delta(dt1, dt2=datetime.datetime.today().date()):
    date_formats = ['%Y-%m-%d', '%Y-%m', '%Y']
    settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first'}

    dt2 = datetime.date.today().strftime('%Y-%m-%d')
    dt2_object = dateparser.parse(dt2, date_formats=date_formats, settings=settings)
    dt1_object = dateparser.parse(dt1, date_formats=date_formats, settings=settings)

    rd = dateutil.relativedelta.relativedelta(dt2_object, dt1_object)

    if len(dt1) == 4:
        # just the year
        x = []

        if rd.years == 1:
            x.append(f'{rd.years} year')
        elif rd.years > 1:
            x.append(f'{rd.years} years')

        xl = len(x)
        if xl == 1:
            xd = x[0]
        else:
            x2 = x.pop()
            xd = ', '.join(x)
            xd += ' and ' + x2

        xd += ' ago this year'

    elif len(dt1) == 7:
        # just the year and month

        x = []

        if rd.years == 1:
            x.append(f'{rd.years} year')
        elif rd.years > 1:
            x.append(f'{rd.years} years')

        if rd.months == 1:
            x.append(f'{rd.months} month')
        elif rd.months > 1:
            x.append(f'{rd.months} months')

        xl = len(x)
        if xl == 1:
            xd = x[0]
        else:
            x2 = x.pop()
            xd = ', '.join(x)
            xd += ' and ' + x2

        xd += ' ago this month'

    else:

        x = []

        if rd.years == 1:
            x.append(f'{rd.years} year')
        elif rd.years > 1:
            x.append(f'{rd.years} years')

        if rd.months == 1:
            x.append(f'{rd.months} month')
        elif rd.months > 1:
            x.append(f'{rd.months} months')

        if rd.days == 1:
            x.append(f'{rd.days} day')
        elif rd.days > 1:
            x.append(f'{rd.days} days')

        xl = len(x)
        if xl == 1:
            xd = x[0]
        else:
            x2 = x.pop()
            xd = ', '.join(x)
            xd += ' and ' + x2

        xd += ' ago today'

    return xd


def is_today_anniversary(date_str):
    # Parse the input date (expects format YYYY-MM-DD)
    dt1 = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    dt2 = datetime.datetime.today().date()

    # Compare month and day (ignore year)
    return dt1.month == dt2.month and dt1.day == dt2.day


def summarise_release(
    discogs_id=None,
    artist=None,
    title=None,
    country=None,
    format=None,
    year=None,
    release_date=None,
    barcodes=None,
    catnos=None,
    master_id=None,
):

    output = []

    output.append(f'Discogs {discogs_id}')

    if artist:
        output.append(artist)

    if title:
        output.append(title)

    if year:
        output.append(str(year))

    if country:
        output.append(country)

    if release_date:
        output.append(release_date)

    if format:
        output.append(format)

    return ' '.join(output)
