from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Mapping, Set

import logging
import pandas as pd
import yaml
from dateutil.relativedelta import relativedelta

from .constants import (
    CLEARANCE_AGENCY_COLUMN,
    CLEARANCE_INVESTIGATION_COLUMN,
    CLEARANCE_LEVEL_COLUMN,
    CLEARANCE_STATUS_COLUMN,
    CREATE_DATE_COLUMN,
    DATE_AVAILABLE_COLUMN,
    EMAIL_COLUMN,
    END_DATE_COLUMN,
    EXTERNAL_IDENTIFIER_COLUMN,
    FIRST_NAME_COLUMN,
    INDUSTRY_COLUMN,
    INTERNAL_COMMENT_COLUMN,
    LAST_NAME_COLUMN,
    LOCATION_RADIUS_COLUMN,
    PHONE_COLUMN,
    PROFESSION_COLUMN,
    SERVICE_BRANCH_COLUMN,
    SUMMARY_NOTES_COLUMN,
    TALENT_PRICE_CATEGORY_COLUMN,
    ZIP_COLUMN,
)
from .models import TransformResult, ValidationReport
from .utils.dates import parse_date, parse_month

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    LAST_NAME_COLUMN,
    FIRST_NAME_COLUMN,
    EMAIL_COLUMN,
    PHONE_COLUMN,
    ZIP_COLUMN,
    PROFESSION_COLUMN,
    SERVICE_BRANCH_COLUMN,
]


def build_combo(month: str, raw_data: Dict[str, pd.DataFrame], config: Mapping[str, Any]) -> TransformResult:
    """Transform raw source frames into a unified Combo DataFrame."""
    column_order = config.get("iqx_import", {}).get("column_order", [])
    defaults = config.get("defaults", {})
    mappings = _load_mappings(config)

    validation = ValidationReport()

    frames = [
        df for name, df in raw_data.items() if not name.startswith("_previous_combo")
    ]
    if not frames:
        logger.warning("No source frames to combine; returning empty Combo.")
        empty_df = pd.DataFrame(columns=column_order)
        return TransformResult(combo_df=empty_df, validation=validation)

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = combined.copy()

    # Normalize key fields
    email_series = _series_or_empty(combined, EMAIL_COLUMN).str.strip().str.lower()
    combined[EMAIL_COLUMN] = email_series

    phone_series = _series_or_empty(combined, PHONE_COLUMN)
    combined[PHONE_COLUMN] = _format_phone_series(phone_series, validation)
    zip_series = _series_or_empty(combined, ZIP_COLUMN)
    combined[ZIP_COLUMN] = _format_zip_series(zip_series, validation)
    profession_series = _series_or_empty(combined, PROFESSION_COLUMN)
    combined[PROFESSION_COLUMN] = profession_series.map(
        _mapper_with_tracking(mappings.get("professions", {}), validation.missing_profession_mappings)
    )

    service_raw = _series_or_empty(combined, SERVICE_BRANCH_COLUMN)
    combined[INTERNAL_COMMENT_COLUMN] = service_raw.map(
        _service_mapper_with_tracking(mappings.get("service_branches", {}), validation.missing_service_branch_mappings)
    )

    # Defaults and computed dates
    combined[LOCATION_RADIUS_COLUMN] = defaults.get("location_radius", defaults.get("location_radius_miles", 100))
    combined[INDUSTRY_COLUMN] = defaults.get("industry", "")
    combined[TALENT_PRICE_CATEGORY_COLUMN] = defaults.get(
        "talent_price_category",
        defaults.get("pay_scale", ""),
    )

    date_format = _resolve_date_format(config, defaults)
    base_date = _resolve_base_date(month, combined, config)
    date_available, end_date = _compute_dates(base_date, defaults, date_format)
    combined[DATE_AVAILABLE_COLUMN] = date_available
    combined[END_DATE_COLUMN] = end_date

    combined[SUMMARY_NOTES_COLUMN] = ""

    strategy = str(defaults.get("external_identifier_strategy", "email_or_phone")).lower()
    identifier_series = combined.get(EXTERNAL_IDENTIFIER_COLUMN)
    if identifier_series is None:
        identifier_series = pd.Series([""] * len(combined), index=combined.index, dtype=str)
    combined[EXTERNAL_IDENTIFIER_COLUMN] = identifier_series.fillna("")
    if strategy == "blank":
        combined[EXTERNAL_IDENTIFIER_COLUMN] = ""
    elif strategy != "record_id":
        mask_missing_id = combined[EXTERNAL_IDENTIFIER_COLUMN].astype(str).str.strip() == ""
        combined.loc[mask_missing_id, EXTERNAL_IDENTIFIER_COLUMN] = combined.loc[mask_missing_id, EMAIL_COLUMN]
        mask_missing_id = combined[EXTERNAL_IDENTIFIER_COLUMN].astype(str).str.strip() == ""
        combined.loc[mask_missing_id, EXTERNAL_IDENTIFIER_COLUMN] = combined.loc[mask_missing_id, PHONE_COLUMN]

    # Drop rows missing both email and phone
    combined = combined[
        (combined[EMAIL_COLUMN].astype(str).str.len() > 0) | (combined[PHONE_COLUMN].astype(str).str.len() > 0)
    ]

    validation.missing_required_columns = {
        col for col in REQUIRED_COLUMNS if col not in combined.columns
    }

    # Ensure expected columns exist, even if empty
    for col in column_order:
        if col not in combined.columns:
            combined[col] = pd.NA

    for col in (
        CLEARANCE_LEVEL_COLUMN,
        CLEARANCE_AGENCY_COLUMN,
        CLEARANCE_STATUS_COLUMN,
        CLEARANCE_INVESTIGATION_COLUMN,
    ):
        if col not in combined.columns:
            combined[col] = pd.NA

    extras = [c for c in combined.columns if c not in column_order]
    ordered_cols = list(column_order) + extras
    combined = combined[ordered_cols]

    logger.info("Built Combo frame with %s rows and %s columns", len(combined), len(combined.columns))
    return TransformResult(combo_df=combined, validation=validation)


def _load_mappings(config: Mapping[str, Any]) -> Dict[str, Dict[str, str]]:
    result: Dict[str, Dict[str, str]] = {}
    mapping_cfg = config.get("mappings", {}) if isinstance(config, Mapping) else {}
    for key in ("professions", "service_branches", "source_priority"):
        path_val = mapping_cfg.get(key)
        if not path_val:
            result[key] = {}
            continue
        path = Path(path_val)
        if not path.exists():
            logger.warning("Mapping file %s missing", path)
            result[key] = {}
            continue
        with path.open("r", encoding="utf-8") as handle:
            result[key] = yaml.safe_load(handle) or {}
    return result


def _mapper_with_tracking(mapping: Dict[str, str], missing_set: Set[str]):
    def mapper(raw: Any) -> str:
        key = str(raw).strip()
        if not key:
            return ""
        if key in mapping:
            return mapping[key]
        missing_set.add(key)
        return key

    return mapper


def _service_mapper_with_tracking(mapping: Dict[str, str], missing_set: Set[str]):
    def mapper(raw: Any) -> str:
        key = str(raw).strip()
        if not key:
            return ""
        if key in mapping:
            return mapping[key]
        missing_set.add(key)
        return f"Service:  {key}"

    return mapper


def _format_phone_series(series: pd.Series, validation: ValidationReport) -> pd.Series:
    formatted = []
    for idx, raw in series.items():
        digits = "".join(ch for ch in str(raw) if ch.isdigit())
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) == 10:
            formatted.append(f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}")
        else:
            formatted.append(digits)
            if digits:
                validation.invalid_phones.append(f"{idx}:{raw}")
    return pd.Series(formatted, index=series.index)


def _format_zip_series(series: pd.Series, validation: ValidationReport) -> pd.Series:
    formatted = []
    for idx, raw in series.items():
        digits = "".join(ch for ch in str(raw) if ch.isdigit())
        if len(digits) >= 5:
            formatted_zip = digits[:5]
        elif len(digits) > 0:
            formatted_zip = digits.zfill(5)
        else:
            formatted_zip = ""
        formatted.append(formatted_zip)
        if digits and len(digits) != 5:
            validation.invalid_zips.append(f"{idx}:{raw}")
    return pd.Series(formatted, index=series.index)


def _compute_dates(base_date: pd.Timestamp, defaults: Mapping[str, Any], date_format: str) -> tuple[str, str]:
    offset_days = int(defaults.get("date_available_offset_days", 1))
    end_years = int(defaults.get("end_date_years_from_available", 1))
    available = base_date + timedelta(days=offset_days)
    end_date = available + relativedelta(years=end_years)
    return available.strftime(date_format), end_date.strftime(date_format)


def _resolve_base_date(month: str, df: pd.DataFrame, config: Mapping[str, Any]) -> pd.Timestamp:
    run_cfg = config.get("run", {}) if isinstance(config, Mapping) else {}
    for key in ("output_date", "current_date", "run_date"):
        raw = run_cfg.get(key)
        if raw:
            return pd.Timestamp(parse_date(str(raw)))

    if CREATE_DATE_COLUMN in df.columns:
        series = pd.to_datetime(df[CREATE_DATE_COLUMN], errors="coerce")
        if series.notna().any():
            return pd.Timestamp(series.max())

    return pd.Timestamp(parse_month(month))


def _resolve_date_format(config: Mapping[str, Any], defaults: Mapping[str, Any]) -> str:
    date_cfg = config.get("date_handling", {}) if isinstance(config, Mapping) else {}
    return date_cfg.get("output_format") or defaults.get("date_format") or "%m/%d/%Y"


def _series_or_empty(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return df[column].fillna("")
    return pd.Series([""] * len(df), index=df.index, dtype=str)
