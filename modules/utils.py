#!/usr/bin/env python3

# Shared helper functions

def normalize_country_name(name):
    """
    Normalize and canonicalize country names to improve matching with COUNTRIES mapping.
    Handles leading 'The', trailing ', The', and common formatting inconsistencies.
    """
    if not name:
        return None

    name = name.strip().lower()

    # Move trailing ', the' to the front: 'Bahamas, The' -> 'The Bahamas'
    if name.endswith(', the'):
        name = 'the ' + name[:-5].strip()

    # Remove leading 'the' if present (optional, depending on how COUNTRIES is structured)
    if name.startswith("the "):
        name = name[4:]

    # Normalize punctuation and spacing
    name = name.replace("&", "and").replace(",", "").replace("  ", " ")
    return " ".join(word.capitalize() for word in name.split())


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
