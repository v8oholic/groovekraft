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
