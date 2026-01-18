from pathlib import Path
from typing import Any, Dict, Mapping

import logging
import pandas as pd

from .constants import (
    DATE_AVAILABLE_COLUMN,
    CREATE_DATE_COLUMN,
    EMAIL_COLUMN,
    EXTERNAL_IDENTIFIER_COLUMN,
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
from .utils.series import combine_keys, digits_only, normalize_series


logger = logging.getLogger(__name__)

# Maps normalized column tokens to canonical names
CANONICAL_COLUMN_MAP = {
    "record id": EXTERNAL_IDENTIFIER_COLUMN,
    "record_id": EXTERNAL_IDENTIFIER_COLUMN,
    "external identifier": EXTERNAL_IDENTIFIER_COLUMN,
    "external_identifier": EXTERNAL_IDENTIFIER_COLUMN,
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
    "mobile phone number": PHONE_COLUMN,
    "phone_number": PHONE_COLUMN,
    "zip": ZIP_COLUMN,
    "zipcode": ZIP_COLUMN,
    "zip code": ZIP_COLUMN,
    "postal": ZIP_COLUMN,
    "postal code": ZIP_COLUMN,
    "profession": PROFESSION_COLUMN,
    "trade of interest": PROFESSION_COLUMN,
    "service branch": SERVICE_BRANCH_COLUMN,
    "branch of service": SERVICE_BRANCH_COLUMN,
    "service": SERVICE_BRANCH_COLUMN,
    "create date": CREATE_DATE_COLUMN,
    "create_date": CREATE_DATE_COLUMN,
    "source": SOURCE_COLUMN,
    "external source": SOURCE_COLUMN,
    "external_source": SOURCE_COLUMN,
    "external source code": SOURCE_CODE_COLUMN,
    "external_source_code": SOURCE_CODE_COLUMN,
}


def load_sources(
    discovery: DiscoveryResult, config: Mapping[str, Any]
) -> Dict[str, pd.DataFrame]:
    """Load source Excel files into DataFrames."""
    frames: Dict[str, pd.DataFrame] = {}

    if discovery.sources:
        for source_name, path in discovery.sources.items():
            code = _lookup_source_code(source_name, config)
            label = _lookup_source_label(source_name, config)
            frames[source_name] = _read_and_normalize(path, source_name, code, label)
    else:
        logger.warning("No source files discovered; continuing with empty data.")

    if discovery.previous_combo:
        frames["_previous_combo"] = _read_and_normalize(
            discovery.previous_combo, "Previous Combo", None, None, add_source=False
        )

    frames = _filter_by_last_import(frames, config)
    return frames


def _lookup_source_code(source_name: str, config: Mapping[str, Any]) -> str | None:
    for entry in config.get("sources", []):
        if entry.get("name") == source_name:
            return entry.get("code")
    return None


def _lookup_source_label(source_name: str, config: Mapping[str, Any]) -> str | None:
    for entry in config.get("sources", []):
        if entry.get("name") == source_name:
            return entry.get("output_label") or entry.get("label") or entry.get("name")
    return None


def _read_and_normalize(
    path: Path,
    source_name: str,
    source_code: str | None,
    source_label: str | None,
    add_source: bool = True,
) -> pd.DataFrame:
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
    if add_source:
        df[SOURCE_COLUMN] = source_label or source_name
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


def _filter_by_last_import(
    frames: Dict[str, pd.DataFrame], config: Mapping[str, Any]
) -> Dict[str, pd.DataFrame]:
    date_cfg = config.get("date_handling", {}) if isinstance(config, Mapping) else {}
    strategy = str(date_cfg.get("last_import_strategy", "")).lower()
    if not strategy:
        return frames

    cutoff_by_source: Dict[str, Any] = {}
    if strategy == "from_config":
        cutoff_by_source = date_cfg.get("last_import_date_by_source", {}) or {}
    elif strategy == "from_combo_file":
        cutoff_by_source = _cutoffs_from_previous_combo(frames.get("_previous_combo"))
        if not cutoff_by_source:
            fallback = date_cfg.get("last_import_date_by_source", {}) or {}
            if fallback:
                logger.warning("Falling back to configured last_import_date_by_source.")
                cutoff_by_source = fallback
    else:
        logger.warning("Unknown last_import_strategy '%s'; skipping filter.", strategy)
        return frames

    parsed_cutoffs = {}
    for source, value in cutoff_by_source.items():
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            logger.warning("Invalid last import date for %s: %s", source, value)
            continue
        parsed_cutoffs[source] = parsed

    if not parsed_cutoffs:
        return frames

    default_cutoff = parsed_cutoffs.get("_all")

    previous_df = frames.get("_previous_combo")
    include_cutoff = date_cfg.get("include_cutoff_date")
    if include_cutoff is None:
        include_cutoff = previous_df is not None and date_cfg.get("exclude_previously_imported", True)

    labels_by_source = {
        entry.get("name"): entry.get("output_label") or entry.get("label") or entry.get("name")
        for entry in config.get("sources", [])
        if entry.get("name")
    }

    filtered: Dict[str, pd.DataFrame] = {}
    for source_name, df in frames.items():
        if source_name.startswith("_previous"):
            filtered[source_name] = df
            continue
        label = labels_by_source.get(source_name)
        cutoff = parsed_cutoffs.get(source_name) or (parsed_cutoffs.get(label) if label else None) or default_cutoff
        if cutoff is None or CREATE_DATE_COLUMN not in df.columns:
            filtered[source_name] = df
            continue
        series = pd.to_datetime(df[CREATE_DATE_COLUMN], errors="coerce")
        series_date = series.dt.date
        cutoff_date = cutoff.date()
        if include_cutoff:
            mask = series_date.isna() | (series_date >= cutoff_date)
        else:
            mask = series_date.isna() | (series_date > cutoff_date)
        dropped = (~mask).sum()
        if dropped:
            logger.info("Filtered %s rows from %s before %s", dropped, source_name, cutoff.date())
        filtered[source_name] = df.loc[mask].copy()

    if date_cfg.get("exclude_previously_imported", True):
        filtered = _filter_previously_imported(filtered)

    return filtered


def _cutoffs_from_previous_combo(previous_df: pd.DataFrame | None) -> Dict[str, Any]:
    if previous_df is None or previous_df.empty:
        return {}

    date_col = None
    for candidate in (CREATE_DATE_COLUMN, DATE_AVAILABLE_COLUMN, "Date Available", "Create Date"):
        if candidate in previous_df.columns:
            date_col = candidate
            break
    if not date_col:
        return {}

    source_col = None
    for candidate in (SOURCE_COLUMN, "Source"):
        if candidate in previous_df.columns:
            source_col = candidate
            break

    dates = pd.to_datetime(previous_df[date_col], errors="coerce")
    if source_col:
        grouped = previous_df.assign(_parsed_date=dates).dropna(subset=["_parsed_date"]).groupby(source_col)
        return {str(name): group["_parsed_date"].max() for name, group in grouped}

    if dates.notna().any():
        max_date = dates.max()
        return {"_all": max_date}
    return {}


def _filter_previously_imported(frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    previous_df = frames.get("_previous_combo")
    if previous_df is None or previous_df.empty:
        return frames

    prev_email = set(normalize_series(previous_df, EMAIL_COLUMN, lambda v: str(v).strip().lower()))
    prev_phone = set(normalize_series(previous_df, PHONE_COLUMN, digits_only))
    last = normalize_series(previous_df, LAST_NAME_COLUMN, lambda v: str(v).strip().lower())
    first = normalize_series(previous_df, FIRST_NAME_COLUMN, lambda v: str(v).strip().lower())
    zip_code = normalize_series(previous_df, ZIP_COLUMN, lambda v: str(v).strip())
    prev_name_zip = set(combine_keys([last, first, zip_code]))

    filtered: Dict[str, pd.DataFrame] = {"_previous_combo": previous_df}
    for source_name, df in frames.items():
        if source_name.startswith("_previous"):
            continue
        email = normalize_series(df, EMAIL_COLUMN, lambda v: str(v).strip().lower())
        phone = normalize_series(df, PHONE_COLUMN, digits_only)
        last = normalize_series(df, LAST_NAME_COLUMN, lambda v: str(v).strip().lower())
        first = normalize_series(df, FIRST_NAME_COLUMN, lambda v: str(v).strip().lower())
        zip_code = normalize_series(df, ZIP_COLUMN, lambda v: str(v).strip())
        name_zip = combine_keys([last, first, zip_code])

        mask = (~email.isin(prev_email)) & (~phone.isin(prev_phone)) & (~name_zip.isin(prev_name_zip))
        dropped = (~mask).sum()
        if dropped:
            logger.info("Excluded %s rows from %s already in previous combo", dropped, source_name)
        filtered[source_name] = df.loc[mask].copy()

    return filtered

