from typing import Any, Dict, Mapping

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def build_combo(raw_data: Dict[str, pd.DataFrame], config: Mapping[str, Any]) -> pd.DataFrame:
    """Transform raw source frames into a unified Combo DataFrame.

    This version focuses on structure: concatenating frames and aligning
    columns to the configured IQX order. Domain-specific cleaning can be added
    incrementally.
    """
    column_order = config.get("iqx_import", {}).get("column_order", [])

    frames = [
        df for name, df in raw_data.items() if not name.startswith("_previous_combo")
    ]
    if not frames:
        logger.warning("No source frames to combine; returning empty Combo.")
        return pd.DataFrame(columns=column_order)

    combo_df = pd.concat(frames, ignore_index=True, sort=False)

    # Ensure expected columns exist, even if empty
    for col in column_order:
        if col not in combo_df.columns:
            combo_df[col] = pd.NA

    # Keep configured order first, then any extra columns
    extras = [c for c in combo_df.columns if c not in column_order]
    ordered_cols = list(column_order) + extras
    combo_df = combo_df[ordered_cols]

    logger.info("Built Combo frame with %s rows and %s columns", len(combo_df), len(combo_df.columns))
    return combo_df
