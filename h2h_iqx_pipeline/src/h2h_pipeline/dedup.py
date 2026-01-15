from typing import Any, Callable, Mapping
from pathlib import Path

import logging
import pandas as pd
import yaml

from .constants import EMAIL_COLUMN, FIRST_NAME_COLUMN, LAST_NAME_COLUMN, PHONE_COLUMN, SOURCE_COLUMN, ZIP_COLUMN
from .models import DedupResult

logger = logging.getLogger(__name__)


def remove_duplicates(combo_df: pd.DataFrame, config: Mapping[str, Any]) -> DedupResult:
    """Remove duplicates prioritizing sources defined in mapping."""
    if combo_df.empty:
        return DedupResult(cleaned_df=combo_df.copy(), duplicates_df=combo_df.copy(), stats={"input_rows": 0, "duplicates_removed": 0})

    priority_map = _load_priority(config)
    working = combo_df.copy()
    working["_priority"] = working[SOURCE_COLUMN].map(priority_map).fillna(0)
    working["_norm_email"] = _normalize_column(working, EMAIL_COLUMN, lambda v: str(v).strip().lower())
    working["_norm_phone"] = _normalize_column(working, PHONE_COLUMN, _digits_only)
    last = _normalize_column(working, LAST_NAME_COLUMN, lambda v: str(v).strip().lower())
    first = _normalize_column(working, FIRST_NAME_COLUMN, lambda v: str(v).strip().lower())
    zip_code = _normalize_column(working, ZIP_COLUMN, lambda v: str(v).strip())
    working["_norm_name_zip"] = _combine_keys([last, first, zip_code])

    kept_rows = []
    duplicate_rows = []
    seen = {"_norm_email": set(), "_norm_phone": set(), "_norm_name_zip": set()}

    for _, row in working.sort_values(by="_priority", ascending=False).iterrows():
        keys = {
            "_norm_email": row["_norm_email"],
            "_norm_phone": row["_norm_phone"],
            "_norm_name_zip": row["_norm_name_zip"],
        }

        duplicate = False
        has_identifier = False
        for key_name, key_val in keys.items():
            if not key_val:
                continue
            has_identifier = True
            if key_val in seen[key_name]:
                duplicate = True
                break

        if duplicate:
            duplicate_rows.append(row)
            continue

        if has_identifier:
            for key_name, key_val in keys.items():
                if key_val:
                    seen[key_name].add(key_val)
        kept_rows.append(row)

    cleaned_df = pd.DataFrame(kept_rows).drop(
        columns=["_priority", "_norm_email", "_norm_phone", "_norm_name_zip"], errors="ignore"
    )
    duplicates_df = pd.DataFrame(duplicate_rows).drop(
        columns=["_priority", "_norm_email", "_norm_phone", "_norm_name_zip"], errors="ignore"
    )

    stats = {
        "input_rows": len(combo_df),
        "duplicates_removed": len(duplicate_rows),
        "output_rows": len(cleaned_df),
    }
    logger.info(
        "Dedup completed. In: %s, out: %s, duplicates: %s",
        len(combo_df),
        len(cleaned_df),
        len(duplicate_rows),
    )
    return DedupResult(cleaned_df=cleaned_df, duplicates_df=duplicates_df, stats=stats)


def _load_priority(config: Mapping[str, Any]) -> Mapping[str, int]:
    mapping_cfg = config.get("mappings", {}) if isinstance(config, Mapping) else {}
    path_val = mapping_cfg.get("source_priority")
    if not path_val:
        return {}
    path = Path(path_val)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    return {k: int(v) for k, v in raw.items()}


def _digits_only(value: Any) -> str:
    return "".join(ch for ch in str(value) if ch.isdigit())


def _normalize_column(df: pd.DataFrame, column: str, normalizer: Callable[[Any], str]) -> pd.Series:
    """Return a normalized string series aligned with df's index."""
    if column in df:
        series = df[column]
    else:
        series = pd.Series([""] * len(df), index=df.index, dtype=str)
    series = series.fillna("")
    return series.map(normalizer)


def _combine_keys(series_list: list[pd.Series]) -> pd.Series:
    """Join multiple key columns, returning empty when all components are blank."""
    frame = pd.concat(series_list, axis=1)
    combined = frame.astype(str).agg("|".join, axis=1)
    has_any = (frame != "").any(axis=1)
    combined[~has_any] = ""
    return combined
