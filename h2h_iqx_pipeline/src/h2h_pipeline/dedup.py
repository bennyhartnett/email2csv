from typing import Any, Mapping
from pathlib import Path

import logging
import pandas as pd
import yaml

from .models import DedupResult

logger = logging.getLogger(__name__)


def remove_duplicates(combo_df: pd.DataFrame, config: Mapping[str, Any]) -> DedupResult:
    """Remove duplicates prioritizing sources defined in mapping."""
    if combo_df.empty:
        return DedupResult(cleaned_df=combo_df.copy(), duplicates_df=combo_df.copy(), stats={"input_rows": 0, "duplicates_removed": 0})

    priority_map = _load_priority(config)
    working = combo_df.copy()
    working["_priority"] = working["Source"].map(priority_map).fillna(0)
    working["_norm_email"] = working.get("Email", pd.Series(dtype=str)).fillna("").str.lower()
    working["_norm_phone"] = working.get("Phone", pd.Series(dtype=str)).fillna("").apply(_digits_only)
    working["_norm_name_zip"] = (
        working.get("Last Name", pd.Series(dtype=str)).fillna("").str.lower()
        + "|"
        + working.get("First Name", pd.Series(dtype=str)).fillna("").str.lower()
        + "|"
        + working.get("Zip", pd.Series(dtype=str)).fillna("")
    )

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
        for key_name, key_val in keys.items():
            if key_val and key_val in seen[key_name]:
                duplicate = True
                break

        if duplicate:
            duplicate_rows.append(row)
            continue

        for key_name, key_val in keys.items():
            if key_val:
                seen[key_name].add(key_val)
        kept_rows.append(row)

    cleaned_df = pd.DataFrame(kept_rows).drop(columns=["_priority", "_norm_email", "_norm_phone", "_norm_name_zip"])
    duplicates_df = pd.DataFrame(duplicate_rows).drop(columns=["_priority", "_norm_email", "_norm_phone", "_norm_name_zip"])

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
