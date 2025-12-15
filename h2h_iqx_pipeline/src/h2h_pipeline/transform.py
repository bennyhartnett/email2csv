from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Mapping

import logging
import pandas as pd
import yaml
from dateutil.relativedelta import relativedelta

from .utils.dates import parse_month

logger = logging.getLogger(__name__)


def build_combo(month: str, raw_data: Dict[str, pd.DataFrame], config: Mapping[str, Any]) -> pd.DataFrame:
    """Transform raw source frames into a unified Combo DataFrame."""
    column_order = config.get("iqx_import", {}).get("column_order", [])
    defaults = config.get("defaults", {})
    mappings = _load_mappings(config)

    frames = [
        df for name, df in raw_data.items() if not name.startswith("_previous_combo")
    ]
    if not frames:
        logger.warning("No source frames to combine; returning empty Combo.")
        return pd.DataFrame(columns=column_order)

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = combined.copy()

    # Normalize key fields
    combined["Email"] = combined.get("Email", pd.Series(dtype=str)).fillna("").str.strip().str.lower()
    combined["Phone"] = combined.get("Phone", pd.Series(dtype=str)).fillna("").astype(str).apply(_format_phone)
    combined["Zip"] = combined.get("Zip", pd.Series(dtype=str)).fillna("").astype(str).apply(_format_zip)
    combined["Profession"] = combined.get("Profession", pd.Series(dtype=str)).fillna("").map(
        lambda v: _apply_mapping(v, mappings.get("professions", {}))
    )
    combined["Service Branch"] = combined.get("Service Branch", pd.Series(dtype=str)).fillna("").map(
        lambda v: _apply_mapping(v, mappings.get("service_branches", {}))
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

    # Ensure expected columns exist, even if empty
    for col in column_order:
        if col not in combined.columns:
            combined[col] = pd.NA

    extras = [c for c in combined.columns if c not in column_order]
    ordered_cols = list(column_order) + extras
    combined = combined[ordered_cols]

    logger.info("Built Combo frame with %s rows and %s columns", len(combined), len(combined.columns))
    return combined


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


def _apply_mapping(raw: Any, mapping: Dict[str, str]) -> str:
    key = str(raw).strip()
    return mapping.get(key, key)


def _format_phone(value: Any) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    return digits


def _format_zip(value: Any) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) >= 5:
        return digits[:5]
    if len(digits) > 0:
        return digits.zfill(5)
    return ""


def _compute_dates(month: str, defaults: Mapping[str, Any]) -> tuple[str, str]:
    start = parse_month(month)
    offset_days = int(defaults.get("date_available_offset_days", 1))
    end_years = int(defaults.get("end_date_years_from_available", 1))
    available = start + timedelta(days=offset_days)
    end_date = available + relativedelta(years=end_years)
    return available.date().isoformat(), end_date.date().isoformat()
