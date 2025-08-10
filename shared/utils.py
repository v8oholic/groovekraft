#!/usr/bin/env python3

# Shared helper functions

from functools import wraps
import time
import datetime
import re
import logging
import json

import dateparser
import dateutil


logger = logging.getLogger(__name__)


def timed(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start

        # Color output depending on time
        if elapsed < 1:
            color_code = "\033[92m"  # Green
        elif elapsed < 5:
            color_code = "\033[93m"  # Yellow
        else:
            color_code = "\033[91m"  # Red
        reset_code = "\033[0m"

        print(f"{color_code}[{func.__name__}] took {elapsed:.2f} seconds{reset_code}")
        return result
    return wrapper


t0 = time.time()


def log_time(msg):
    print(f"[{time.time() - t0:.2f}s] {msg}")


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
    logger.error("mediatypes.json not found. Please check your config path.")
    MEDIATYPES = {}


def normalize_country_name(name):
    """
    Normalize and canonicalize country names to improve matching with COUNTRIES mapping.
    Handles leading 'The', trailing ', The', and common formatting inconsistencies.
    """
    if not name:
        return None

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


def convert_format(discogs_format):
    primary_type = discogs_format.get('name')  # for example CD
    secondary_types = discogs_format.get('descriptions')  # for example album

    if primary_type == 'Vinyl':
        return convert_vinyl_format(secondary_types)
    elif primary_type == 'CD':
        return convert_cd_format(secondary_types)
    elif primary_type == 'Flexi-disc':
        return convert_flexi_disc_format(secondary_types)
    elif primary_type == 'Box Set':
        return convert_box_set_format(secondary_types)

    return '?', '?', discogs_format.get('name')


def convert_vinyl_format(secondary_types):
    if 'EP' in secondary_types and '7"' in secondary_types:
        return '7" EP', 'single', 'Vinyl'
    elif 'EP' in secondary_types and '12"' in secondary_types:
        return '12" EP', 'single', 'Vinyl'
    elif '12"' in secondary_types:
        return '12" Single', 'single', 'Vinyl'
    elif '7"' in secondary_types:
        return '7" Single', 'single', 'Vinyl'
    elif 'Compilation' in secondary_types:
        return 'LP Compilation', 'album', 'Vinyl'
    elif 'LP' in secondary_types:
        return 'LP', 'album', 'Vinyl'
    else:
        return '?', '?', 'Vinyl'


def convert_cd_format(secondary_types):
    if 'Mini' in secondary_types:
        return 'CD 3" Mini Single', 'single', 'CD'
    elif 'Single' in secondary_types:
        return 'CD Single', 'single', 'CD'
    elif 'Maxi-Single' in secondary_types:
        return 'CD Single', 'single', 'CD'
    elif 'EP' in secondary_types:
        return 'CD EP', 'single', 'CD'
    elif 'LP' in secondary_types:
        return 'CD Album', 'album', 'CD'
    elif 'Album' in secondary_types:
        return 'CD Album', 'album', 'CD'
    elif 'Mini-Album' in secondary_types:
        return 'CD Mini-Album', 'album', 'CD'
    elif 'Compilation' in secondary_types:
        return 'CD Compilation', 'album', 'CD'
    else:
        return 'CD Single', 'single', 'CD'


def convert_flexi_disc_format(secondary_types):
    if '7"' in secondary_types:
        return '7" flexi-disc', 'single', 'Flexi-disc'
    return '?', '?', 'Flexi-disc'


def convert_box_set_format(secondary_types):
    if '12"' in secondary_types:
        return '12" Singles Box Set', 'single', 'Box Set'
    elif '7"' in secondary_types:
        return '7" Singles Box Set', 'single', 'Box Set'
    elif 'LP' in secondary_types:
        return "LP Box Set", 'album', 'Box Set'
    elif 'EP' in secondary_types:
        return "EP Box Set", 'single', 'Box Set'
    elif 'Single' in secondary_types:
        return 'Singles Box Set', 'single', 'Box Set'
    elif 'Maxi-Single' in secondary_types:
        return 'Maxi-singles Box Set', 'single', 'Box Set'
    else:
        return 'Box Set', 'Other', 'Box Set'


def sanitise_identifier(catno):
    catno_string = ''.join(chr for chr in catno if chr.isalnum() or chr == '-').casefold()
    return catno_string


def normalize_identifier_list(value):
    """Normalise a string or list of identifiers into a list.

    Identifiers are barcodes and catalog numbers. A (comma separated) string
    or a list of strings is converted into a list of normalized values."""

    value_set = []

    if value is None:
        return []

    elif isinstance(value, str):
        for tmp in value.split(','):
            value_set.append(sanitise_identifier(tmp.strip()))

    elif isinstance(value, (list, set)):
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

    if not date_object:
        logger.warning(f"Could not parse date: {date_str}")
        return None

    return date_object.date()


def earliest_date(dt1_str, dt2_str):
    def clean(d): return None if not d or d in ['0', 'None'] else str(d)
    dt1_str, dt2_str = clean(dt1_str), clean(dt2_str)

    if dt1_str == dt2_str:
        return dt1_str

    dt1_obj = parse_date(dt1_str) if dt1_str else None
    dt2_obj = parse_date(dt2_str) if dt2_str else None

    if not dt1_obj:
        return dt2_str
    if not dt2_obj:
        return dt1_str

    t1 = (dt1_obj.year, dt1_obj.month or 0, dt1_obj.day or 0)
    t2 = (dt2_obj.year, dt2_obj.month or 0, dt2_obj.day or 0)

    return dt1_str if t1 < t2 else dt2_str


def pluralize(count, singular, plural=None):
    if plural is None:
        plural = singular + 's'
    return f"{count} {singular}" if count == 1 else f"{count} {plural}"


def parse_and_humanize_date(ymd_date):
    if not ymd_date:
        return ''

    strategies = [
        {"length": 10, "format": "%Y-%m-%d",
            "parts": ['day', 'month', 'year'], "output": "%A %-d %B %Y"},
        {"length": 7,  "format": "%Y-%m",    "parts": ['month', 'year'],        "output": "%B %Y"},
        {"length": 4,  "format": "%Y",       "parts": ['year'],                 "output": "%Y"},
    ]

    for strat in strategies:
        if len(ymd_date) == strat["length"]:
            settings = {"REQUIRE_PARTS": strat["parts"]}
            date_object = dateparser.parse(ymd_date, date_formats=[
                                           strat["format"]], settings=settings)
            if date_object:
                return date_object.strftime(strat["output"])

    return ''


def humanize_date_delta(dt1, dt2=datetime.date.today()):
    def build_parts(rd, fields):
        label_map = {
            'years': 'year',
            'months': 'month',
            'days': 'day'
        }
        parts = []
        for field in fields:
            value = getattr(rd, field)
            if value:
                parts.append(pluralize(value, label_map[field]))
        return parts

    if not dt1:
        return ''

    date_formats = ['%Y-%m-%d', '%Y-%m', '%Y']
    settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first'}

    dt2_object = dt2
    dt1_object = dateparser.parse(dt1, date_formats=date_formats, settings=settings)

    if not dt1_object:
        return ''

    dt1_object = dt1_object.date()
    rd = dateutil.relativedelta.relativedelta(dt2_object, dt1_object)

    if len(dt1) == 4:
        parts = build_parts(rd, ['years'])
        suffix = 'ago this year'
    elif len(dt1) == 7:
        parts = build_parts(rd, ['years', 'months'])
        suffix = 'ago this month'
    else:
        parts = build_parts(rd, ['years', 'months', 'days'])
        suffix = 'ago'

    if not parts:
        return f'just now ({suffix})'

    if len(parts) == 1:
        return f"{parts[0]} {suffix}"
    else:
        return f"{', '.join(parts[:-1])} and {parts[-1]} {suffix}"


def is_today_anniversary(date_str: str):
    # Parse the input date (expects format YYYY-MM-DD)
    dt1 = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    dt2 = datetime.datetime.today().date()

    # Compare month and day (ignore year)
    return dt1.month == dt2.month and dt1.day == dt2.day


def is_month_anniversary(date_str: str) -> bool:
    # Parse the input date (expects format YYYY-MM)
    dt1 = datetime.datetime.strptime(date_str, "%Y-%m").date()
    dt2 = datetime.datetime.today().date()

    # Compare month (ignore year)
    return dt1.month == dt2.month


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
