from typing import Any, Mapping

import logging
import pandas as pd

from .models import DedupResult

logger = logging.getLogger(__name__)


def remove_duplicates(combo_df: pd.DataFrame, config: Mapping[str, Any]) -> DedupResult:
    """Placeholder duplicate removal that simply returns the input."""
    duplicates_df = combo_df.iloc[0:0].copy()
    stats = {"input_rows": len(combo_df), "duplicates_removed": 0}
    logger.info("Skipping duplicate removal placeholder. Rows in: %s", len(combo_df))
    return DedupResult(cleaned_df=combo_df.copy(), duplicates_df=duplicates_df, stats=stats)
