from pathlib import Path
from typing import Any, Dict, Mapping

import logging
import pandas as pd

from .utils.io_helpers import ensure_dir

logger = logging.getLogger(__name__)


def write_outputs(
    run_label: str, combo_df: pd.DataFrame, dedup_df: pd.DataFrame, config: Mapping[str, Any]
) -> Dict[str, Path]:
    """Write Combo, duplicates-removed, and IQX CSV outputs.

    Uses naming patterns from config. If writing fails, logs the error and
    continues so the prototype remains runnable.
    """
    paths_cfg = config.get("paths", {}) if isinstance(config, Mapping) else {}
    output_root = ensure_dir(Path(paths_cfg.get("output_root", "output")))

    column_order = config.get("iqx_import", {}).get("column_order", [])
    combo_excel_df = _reorder_columns(combo_df, column_order, keep_extra=True)
    dedup_excel_df = _reorder_columns(dedup_df, column_order, keep_extra=True)
    dedup_csv_df = _reorder_columns(dedup_df, column_order, keep_extra=False)

    combo_pattern = config.get("combo_files", {}).get("excel_pattern", "Combo {date}.xlsx")
    combo_excel = output_root / combo_pattern.format(date=run_label)

    dedup_excel = output_root / f"Combo Dups Removed {run_label}.xlsx"

    iqx_pattern = config.get("combo_files", {}).get("csv_pattern", "Bulk Import {date}.csv")
    iqx_csv = output_root / iqx_pattern.format(date=run_label)

    _safe_write_excel(combo_excel_df, combo_excel, "Combo")
    _safe_write_excel(dedup_excel_df, dedup_excel, "Combo Dups Removed")
    _safe_write_csv(dedup_csv_df, iqx_csv, "IQX CSV")

    return {"combo_excel": combo_excel, "dedup_excel": dedup_excel, "iqx_csv": iqx_csv}


def _reorder_columns(df: pd.DataFrame, column_order: list[str], keep_extra: bool) -> pd.DataFrame:
    if not column_order:
        return df
    out = df.copy()
    for col in column_order:
        if col not in out.columns:
            out[col] = pd.NA
    if keep_extra:
        extra = [c for c in out.columns if c not in column_order]
        return out[column_order + extra]
    return out[column_order]


def _safe_write_excel(df: pd.DataFrame, path: Path, label: str) -> None:
    try:
        df.to_excel(path, index=False)
        logger.info("Wrote %s to %s", label, path)
    except Exception as exc:  # pragma: no cover - placeholder
        logger.error("Failed to write %s Excel %s: %s", label, path, exc)


def _safe_write_csv(df: pd.DataFrame, path: Path, label: str) -> None:
    try:
        df.to_csv(path, index=False)
        logger.info("Wrote %s to %s", label, path)
    except Exception as exc:  # pragma: no cover - placeholder
        logger.error("Failed to write %s CSV %s: %s", label, path, exc)
