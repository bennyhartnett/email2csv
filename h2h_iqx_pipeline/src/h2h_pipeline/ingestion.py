from pathlib import Path
from typing import Any, Dict, Mapping

import logging
import pandas as pd

from .constants import (
    EMAIL_COLUMN,
    FIRST_NAME_COLUMN,
    LAST_NAME_COLUMN,
    PHONE_COLUMN,
    PROFESSION_COLUMN,
    SERVICE_BRANCH_COLUMN,
    SOURCE_CODE_COLUMN,
    SOURCE_COLUMN,
    ZIP_COLUMN,
)
from .models import DiscoveryResult


logger = logging.getLogger(__name__)

# Maps normalized column tokens to canonical names
CANONICAL_COLUMN_MAP = {
    "last name": LAST_NAME_COLUMN,
    "lastname": LAST_NAME_COLUMN,
    "last_name": LAST_NAME_COLUMN,
    "first name": FIRST_NAME_COLUMN,
    "firstname": FIRST_NAME_COLUMN,
    "first_name": FIRST_NAME_COLUMN,
    "email": EMAIL_COLUMN,
    "email address": EMAIL_COLUMN,
    "email_address": EMAIL_COLUMN,
    "phone": PHONE_COLUMN,
    "phone number": PHONE_COLUMN,
    "phone_number": PHONE_COLUMN,
    "zip": ZIP_COLUMN,
    "zipcode": ZIP_COLUMN,
    "postal": ZIP_COLUMN,
    "profession": PROFESSION_COLUMN,
    "service branch": SERVICE_BRANCH_COLUMN,
    "service": SERVICE_BRANCH_COLUMN,
    "source": SOURCE_COLUMN,
}


def load_sources(
    discovery: DiscoveryResult, config: Mapping[str, Any]
) -> Dict[str, pd.DataFrame]:
    """Load source Excel files into DataFrames."""
    frames: Dict[str, pd.DataFrame] = {}

    if discovery.sources:
        for source_name, path in discovery.sources.items():
            code = _lookup_source_code(source_name, config)
            frames[source_name] = _read_and_normalize(path, source_name, code)
    else:
        logger.warning("No source files discovered; continuing with empty data.")

    if discovery.previous_combo:
        frames["_previous_combo"] = _read_and_normalize(discovery.previous_combo, "Previous Combo", None)

    return frames


def _lookup_source_code(source_name: str, config: Mapping[str, Any]) -> str | None:
    for entry in config.get("sources", []):
        if entry.get("name") == source_name:
            return entry.get("code")
    return None


def _read_and_normalize(path: Path, source_name: str, source_code: str | None) -> pd.DataFrame:
    if not path.exists():
        logger.warning("Expected source file missing: %s", path)
        return _empty_df()
    try:
        df = pd.read_excel(path, dtype=str)
    except Exception as exc:  # pragma: no cover - safety
        logger.error("Failed to read %s: %s", path, exc)
        return _empty_df()

    df = _normalize_columns(df)

    # Add metadata columns
    df[SOURCE_COLUMN] = source_name
    if source_code:
        df[SOURCE_CODE_COLUMN] = source_code

    # Drop rows with no identifiers
    df = df.dropna(how="all")
    return df


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed: Dict[str, str] = {}
    for col in df.columns:
        key = _normalize_token(col)
        canonical = CANONICAL_COLUMN_MAP.get(key, col)
        renamed[col] = canonical
    return df.rename(columns=renamed)


def _normalize_token(text: str) -> str:
    return text.strip().lower().replace("-", " ").replace("_", " ")


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame()
