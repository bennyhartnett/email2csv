from pathlib import Path
from typing import Any, Dict, Mapping

import logging

from .models import DiscoveryResult


logger = logging.getLogger(__name__)


def discover_month_files(
    month: str, input_root: Path, config: Mapping[str, Any]
) -> DiscoveryResult:
    """Locate the month directory, source files, and prior Combo file."""
    month_dir = _find_month_dir(input_root, month)
    sources: Dict[str, Path] = {}

    if month_dir:
        for source_cfg in config.get("sources", []):
            pattern = source_cfg.get("file_pattern")
            name = source_cfg.get("name")
            if not pattern or not name:
                continue
            matches = sorted(month_dir.glob(pattern))
            if matches:
                sources[name] = matches[0]
            else:
                logger.warning("No files found for source %s with pattern %s", name, pattern)
    else:
        logger.warning("No month directory found for %s under %s", month, input_root)

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
        month_dir=month_dir,
        sources=sources,
        previous_combo=previous_combo,
    )


def _find_month_dir(input_root: Path, month: str) -> Path | None:
    """Find a month directory, supporting common folder naming conventions."""
    candidates = [
        input_root / month,
        input_root / f"Vet Talents {month}",
        input_root / f"ORIG - Vet Talents {month}",
    ]
    candidates.extend(sorted(p for p in input_root.glob(f"*{month}*") if p.is_dir()))

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _find_previous_combo(input_root: Path, config: Mapping[str, Any]) -> Path | None:
    """Attempt to locate the prior month's Combo file based on config hints."""
    run_cfg = config.get("run", {}) if isinstance(config, Mapping) else {}
    prev_month = run_cfg.get("previous_month")
    if not prev_month:
        return None

    prev_dir = _find_month_dir(input_root, prev_month)
    if not prev_dir:
        return None

    patterns = [
        config.get("combo_files", {}).get("excel_pattern", "").format(date=prev_month),
        "*Combo*.xlsx",
    ]
    for pattern in patterns:
        if not pattern:
            continue
        matches = sorted(prev_dir.glob(pattern))
        if matches:
            return matches[0]
    return None
