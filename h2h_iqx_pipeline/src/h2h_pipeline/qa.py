from pathlib import Path
from typing import Any, Mapping

import logging
import pandas as pd

from .models import DedupResult, ValidationReport, DiscoveryResult
from .utils.io_helpers import ensure_dir


logger = logging.getLogger(__name__)


def generate_report(
    run_label: str,
    combo_df: pd.DataFrame,
    dedup_result: DedupResult,
    export_paths: Mapping[str, Path],
    validation: ValidationReport,
    discovery: DiscoveryResult,
    counts_before: Mapping[str, int],
    counts_after: Mapping[str, int],
    config: Mapping[str, Any],
) -> Path:
    """Write a QA report summarizing the run."""
    paths_cfg = config.get("paths", {}) if isinstance(config, Mapping) else {}
    output_root = ensure_dir(Path(paths_cfg.get("output_root", "output")))

    report_path = output_root / f"QA Report {run_label}.txt"

    lines = [
        f"QA report for {run_label}",
        "",
        f"Rows in Combo: {len(combo_df)}",
        f"Rows after dedup: {len(dedup_result.cleaned_df)}",
        f"Duplicates removed: {len(dedup_result.duplicates_df)}",
        "",
        "Outputs:",
    ]
    for label, path in export_paths.items():
        lines.append(f"- {label}: {path}")

    lines.extend(
        [
            "",
            "Per-source counts before dedup:",
        ]
    )
    if counts_before:
        for src, cnt in sorted(counts_before.items()):
            lines.append(f"- {src}: {cnt}")
    else:
        lines.append("- none")

    lines.append("")
    lines.append("Per-source counts after dedup:")
    if counts_after:
        for src, cnt in sorted(counts_after.items()):
            lines.append(f"- {src}: {cnt}")
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "Validation findings:",
            f"- Missing profession mappings: {sorted(validation.missing_profession_mappings) if validation.missing_profession_mappings else 'none'}",
            f"- Missing service branch mappings: {sorted(validation.missing_service_branch_mappings) if validation.missing_service_branch_mappings else 'none'}",
            f"- Invalid phone values (row:value): {validation.invalid_phones if validation.invalid_phones else 'none'}",
            f"- Invalid zip values (row:value): {validation.invalid_zips if validation.invalid_zips else 'none'}",
            f"- Missing required columns: {sorted(validation.missing_required_columns) if validation.missing_required_columns else 'none'}",
        ]
    )

    lines.append("")
    lines.append("Discovery warnings:")
    if discovery.month_dir_missing:
        lines.append(f"- Month directory not found under {discovery.input_root}")
    if discovery.missing_sources:
        lines.append(f"- Missing source files for: {', '.join(sorted(discovery.missing_sources))}")
    if not discovery.month_dir_missing and not discovery.missing_sources:
        lines.append("- none")

    with report_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))

    logger.info("QA report written to %s", report_path)
    return report_path
