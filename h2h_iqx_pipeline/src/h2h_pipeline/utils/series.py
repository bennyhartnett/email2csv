from typing import Any, Callable, Iterable

import pandas as pd


def digits_only(value: Any) -> str:
    return "".join(ch for ch in str(value) if ch.isdigit())


def normalize_series(
    df: pd.DataFrame,
    column: str,
    normalizer: Callable[[Any], str],
) -> pd.Series:
    """Return a normalized string series aligned with df's index."""
    if column in df.columns:
        series = df[column]
    else:
        series = pd.Series([""] * len(df), index=df.index, dtype=str)
    series = series.fillna("")
    return series.map(normalizer)


def combine_keys(series_list: Iterable[pd.Series]) -> pd.Series:
    """Join multiple key columns, returning empty when all components are blank."""
    frame = pd.concat(list(series_list), axis=1)
    combined = frame.astype(str).agg("|".join, axis=1)
    has_any = (frame != "").any(axis=1)
    combined[~has_any] = ""
    return combined
