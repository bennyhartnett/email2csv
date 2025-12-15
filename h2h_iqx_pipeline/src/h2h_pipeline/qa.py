from pathlib import Path
from typing import Any, Mapping

import logging
import pandas as pd

from .models import DedupResult


logger = logging.getLogger(__name__)


def generate_report(
    month: str,
    combo_df: pd.DataFrame,
    dedup_result: DedupResult,
    export_paths: Mapping[str, Path],
    config: Mapping[str, Any],
) -> Path:
    """Write a lightweight QA report summarizing the run."""
    paths_cfg = config.get("paths", {}) if isinstance(config, Mapping) else {}
    output_root = Path(paths_cfg.get("output_root", "output"))
    output_root.mkdir(parents=True, exist_ok=True)

    report_path = output_root / f"QA Report {month}.txt"

    lines = [
        f"QA report for {month}",
        "",
        f"Rows in Combo: {len(combo_df)}",
        f"Rows after dedup: {len(dedup_result.cleaned_df)}",
        f"Duplicates removed: {len(dedup_result.duplicates_df)}",
        "",
        "Outputs:",
    ]
    for label, path in export_paths.items():
        lines.append(f"- {label}: {path}")

    with report_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))

    logger.info("QA report written to %s", report_path)
    return report_path
