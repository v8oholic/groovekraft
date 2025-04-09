#!/usr/bin/env python3

# Shared helper functions

def normalize_country_name(name):
    """
    Normalize and canonicalize country names to improve matching with COUNTRIES mapping.
    """
    if not name:
        return None

    name = name.strip().lower()

    # Handle common prefix patterns like "The Bahamas" → "Bahamas"
    if name.startswith("the "):
        name = name[4:]

    # Replace common punctuation or formatting inconsistencies
    name = name.replace("&", "and")
    name = name.replace(",", "")
    name = name.replace("  ", " ")

    # Capitalize each word properly
    return " ".join(word.capitalize() for word in name.split())
