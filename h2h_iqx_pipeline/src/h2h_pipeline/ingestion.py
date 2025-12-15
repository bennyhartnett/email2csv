from pathlib import Path
from typing import Any, Dict, Mapping

import logging
import pandas as pd

from .models import DiscoveryResult


logger = logging.getLogger(__name__)


def load_sources(
    discovery: DiscoveryResult, config: Mapping[str, Any]
) -> Dict[str, pd.DataFrame]:
    """Load source Excel files into DataFrames.

    This placeholder ingests available files and falls back to empty frames when
    nothing is found, keeping the pipeline runnable while implementation is
    fleshed out.
    """
    frames: Dict[str, pd.DataFrame] = {}
    column_order = config.get("iqx_import", {}).get("column_order", [])

    if discovery.sources:
        for source_name, path in discovery.sources.items():
            frames[source_name] = _read_excel(path, column_order)
    else:
        logger.warning("No source files discovered; continuing with empty data.")

    if discovery.previous_combo:
        frames["_previous_combo"] = _read_excel(discovery.previous_combo, column_order)

    return frames


def _read_excel(path: Path, columns: Any) -> pd.DataFrame:
    if not path.exists():
        logger.warning("Expected source file missing: %s", path)
        return _empty_df(columns)
    try:
        df = pd.read_excel(path)
        return df
    except Exception as exc:  # pragma: no cover - placeholder
        logger.error("Failed to read %s: %s", path, exc)
        return _empty_df(columns)


def _empty_df(columns: Any) -> pd.DataFrame:
    cols = list(columns) if columns else []
    return pd.DataFrame(columns=cols)
