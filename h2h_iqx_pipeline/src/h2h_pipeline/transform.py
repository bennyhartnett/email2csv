from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Mapping, Set

import logging
import pandas as pd
import yaml
from dateutil.relativedelta import relativedelta

from .models import TransformResult, ValidationReport
from .utils.dates import parse_month

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["Last Name", "First Name", "Email", "Phone", "Zip", "Profession", "Service Branch"]


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
    combined["Email"] = combined.get("Email", pd.Series(dtype=str)).fillna("").str.strip().str.lower()

    combined["Phone"] = combined.get("Phone", pd.Series(dtype=str)).fillna("").astype(str).apply(
        lambda v: _format_phone(v, validation)
    )
    combined["Zip"] = combined.get("Zip", pd.Series(dtype=str)).fillna("").astype(str).apply(
        lambda v: _format_zip(v, validation)
    )
    combined["Profession"] = combined.get("Profession", pd.Series(dtype=str)).fillna("").map(
        _mapper_with_tracking(mappings.get("professions", {}), validation.missing_profession_mappings)
    )
    combined["Service Branch"] = combined.get("Service Branch", pd.Series(dtype=str)).fillna("").map(
        _mapper_with_tracking(mappings.get("service_branches", {}), validation.missing_service_branch_mappings)
    )

    # Defaults and computed dates
    combined["Invited"] = defaults.get("invited_flag", "I")
    combined["Location Radius"] = defaults.get("location_radius_miles", 100)
    combined["Trade"] = defaults.get("trade", "Electricians")
    combined["Union Code"] = defaults.get("union_code", "")
    combined["Pay Scale"] = defaults.get("pay_scale", "")

    date_available, end_date = _compute_dates(month, defaults)
    combined["Date Available"] = date_available
    combined["End Date"] = end_date

    # Internal comments derived from service branch when available
    combined["Internal Comments"] = combined["Service Branch"]

    # Build External ID (prefer email then phone)
    combined["External ID"] = combined["Email"]
    mask_missing_id = combined["External ID"] == ""
    combined.loc[mask_missing_id, "External ID"] = combined.loc[mask_missing_id, "Phone"]

    # Drop rows missing both email and phone
    combined = combined[
        (combined["Email"].astype(str).str.len() > 0) | (combined["Phone"].astype(str).str.len() > 0)
    ]

    validation.missing_required_columns = {
        col for col in REQUIRED_COLUMNS if col not in combined.columns
    }

    # Ensure expected columns exist, even if empty
    for col in column_order:
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


def _format_phone(value: Any, validation: ValidationReport) -> str:
    raw = str(value)
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    if digits:
        validation.invalid_phones.append(raw)
    return digits


def _format_zip(value: Any, validation: ValidationReport) -> str:
    raw = str(value)
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 5:
        formatted = digits[:5]
    elif len(digits) > 0:
        formatted = digits.zfill(5)
    else:
        formatted = ""
    if digits and len(digits) != 5:
        validation.invalid_zips.append(raw)
    return formatted


def _compute_dates(month: str, defaults: Mapping[str, Any]) -> tuple[str, str]:
    start = parse_month(month)
    offset_days = int(defaults.get("date_available_offset_days", 1))
    end_years = int(defaults.get("end_date_years_from_available", 1))
    available = start + timedelta(days=offset_days)
    end_date = available + relativedelta(years=end_years)
    return available.date().isoformat(), end_date.date().isoformat()
