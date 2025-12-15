from pathlib import Path
from typing import Any, Dict, Mapping

import logging
import pandas as pd


logger = logging.getLogger(__name__)


def write_outputs(
    month: str, combo_df: pd.DataFrame, dedup_df: pd.DataFrame, config: Mapping[str, Any]
) -> Dict[str, Path]:
    """Write Combo, duplicates-removed, and IQX CSV outputs.

    Uses naming patterns from config. If writing fails, logs the error and
    continues so the prototype remains runnable.
    """
    paths_cfg = config.get("paths", {}) if isinstance(config, Mapping) else {}
    output_root = Path(paths_cfg.get("output_root", "output"))
    output_root.mkdir(parents=True, exist_ok=True)

    combo_pattern = config.get("combo_files", {}).get("excel_pattern", "Combo {date}.xlsx")
    combo_excel = output_root / combo_pattern.format(date=month)

    dedup_excel = output_root / f"Combo Dups Removed {month}.xlsx"

    iqx_pattern = config.get("combo_files", {}).get("csv_pattern", "Bulk Import {date}.csv")
    iqx_csv = output_root / iqx_pattern.format(date=month)

    _safe_write_excel(combo_df, combo_excel, "Combo")
    _safe_write_excel(dedup_df, dedup_excel, "Combo Dups Removed")
    _safe_write_csv(dedup_df, iqx_csv, "IQX CSV")

    return {"combo_excel": combo_excel, "dedup_excel": dedup_excel, "iqx_csv": iqx_csv}


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
