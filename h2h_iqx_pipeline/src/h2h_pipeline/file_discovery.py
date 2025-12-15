from pathlib import Path
from typing import Any, Dict, Mapping

import logging

from .models import DiscoveryResult


logger = logging.getLogger(__name__)


def discover_month_files(
    month: str, input_root: Path, config: Mapping[str, Any]
) -> DiscoveryResult:
    """Locate the month directory, source files, and prior Combo file.

    This is a lightweight placeholder that favors clarity over completeness.
    """
    month_dir = Path(input_root) / month
    sources: Dict[str, Path] = {}

    if month_dir.exists():
        for source_cfg in config.get("sources", []):
            pattern = source_cfg.get("file_pattern")
            name = source_cfg.get("name")
            if not pattern or not name:
                continue
            matches = list(month_dir.glob(pattern))
            if matches:
                # If multiple matches, keep the first for now
                sources[name] = matches[0]
            else:
                logger.warning("No files found for source %s with pattern %s", name, pattern)
    else:
        logger.warning("Month directory %s does not exist", month_dir)

    previous_combo = _find_previous_combo(input_root, config)

    logger.info(
        "Discovery completed for %s: %s sources, previous combo: %s",
        month,
        len(sources),
        previous_combo if previous_combo else "none",
    )
    return DiscoveryResult(
        month=month,
        input_root=input_root,
        month_dir=month_dir if month_dir.exists() else None,
        sources=sources,
        previous_combo=previous_combo,
    )


def _find_previous_combo(input_root: Path, config: Mapping[str, Any]) -> Path | None:
    """Attempt to locate the prior month's Combo file based on config hints."""
    run_cfg = config.get("run", {}) if isinstance(config, Mapping) else {}
    prev_month = run_cfg.get("previous_month")
    if not prev_month:
        return None

    prev_dir = Path(input_root) / prev_month
    if not prev_dir.exists():
        return None

    patterns = [
        config.get("combo_files", {}).get("excel_pattern", ""),
        "*Combo*.xlsx",
    ]
    for pattern in patterns:
        if not pattern:
            continue
        matches = sorted(prev_dir.glob(pattern))
        if matches:
            return matches[0]
    return None
