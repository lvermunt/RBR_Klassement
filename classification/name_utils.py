"""Utilities for normalizing participant names consistently across the project."""

import math
import unicodedata

import pandas as pd


def normalize_name(name):
    """Normalize a participant name so accented and unaccented versions match."""
    if pd.isna(name):
        return name

    if isinstance(name, float) and math.isnan(name):
        return name

    normalized = str(name).strip()
    normalized = unicodedata.normalize("NFD", normalized)
    normalized = "".join(character for character in normalized if unicodedata.category(character) != "Mn")
    return normalized.casefold().title()
