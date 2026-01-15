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

    working = working.sort_values(by="_priority", ascending=False).reset_index(drop=True)

    parent = list(range(len(working)))

    def find(idx: int) -> int:
        while parent[idx] != idx:
            parent[idx] = parent[parent[idx]]
            idx = parent[idx]
        return idx

    def union(a: int, b: int) -> None:
        root_a = find(a)
        root_b = find(b)
        if root_a != root_b:
            parent[root_b] = root_a

    key_to_index: dict[str, dict[str, int]] = {"_norm_email": {}, "_norm_phone": {}, "_norm_name_zip": {}}
    for idx, row in working.iterrows():
        for key_name in key_to_index:
            key_val = row[key_name]
            if not key_val:
                continue
            existing = key_to_index[key_name].get(key_val)
            if existing is None:
                key_to_index[key_name][key_val] = idx
            else:
                union(idx, existing)

    groups: dict[int, list[int]] = {}
    for idx in range(len(working)):
        root = find(idx)
        groups.setdefault(root, []).append(idx)

    kept_indices: list[int] = []
    duplicate_indices: list[int] = []
    combined_sources: dict[int, set[str]] = {}

    for indices in groups.values():
        kept = min(indices)
        kept_indices.append(kept)
        combined_sources[kept] = set(working.loc[indices, SOURCE_COLUMN].astype(str))
        for idx in indices:
            if idx != kept:
                duplicate_indices.append(idx)

    kept_indices.sort()
    duplicate_indices.sort()

    cleaned_df = (
        working.loc[kept_indices]
        .drop(columns=["_priority", "_norm_email", "_norm_phone", "_norm_name_zip"], errors="ignore")
        .reset_index(drop=True)
    )
    duplicates_df = (
        working.loc[duplicate_indices]
        .drop(columns=["_priority", "_norm_email", "_norm_phone", "_norm_name_zip"], errors="ignore")
        .reset_index(drop=True)
    )

    if not cleaned_df.empty and SOURCE_COLUMN in cleaned_df.columns:
        ordered_sources = []
        for kept in kept_indices:
            sources = combined_sources.get(kept, {str(working.at[kept, SOURCE_COLUMN])})
            ordered_sources.append(_format_sources(sources, priority_map))
        cleaned_df[SOURCE_COLUMN] = ordered_sources

    stats = {
        "input_rows": len(combo_df),
        "duplicates_removed": len(duplicate_indices),
        "output_rows": len(cleaned_df),
    }
    logger.info(
        "Dedup completed. In: %s, out: %s, duplicates: %s",
        len(combo_df),
        len(cleaned_df),
        len(duplicate_indices),
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


def _format_sources(sources: set[str], priority_map: Mapping[str, int]) -> str:
    ordered = sorted(sources, key=lambda s: (-priority_map.get(s, 0), s))
    return " & ".join(ordered)
