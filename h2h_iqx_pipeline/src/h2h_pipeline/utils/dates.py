from datetime import datetime
from typing import Any, Mapping, Sequence


def parse_month(month_str: str) -> datetime:
    """Parse YYYY-MM (or YYYY-MM-DD) into a datetime."""
    try:
        return datetime.strptime(month_str, "%Y-%m")
    except ValueError:
        return datetime.strptime(month_str, "%Y-%m-%d")


def parse_date(date_str: str) -> datetime:
    """Parse a run/output date in YYYY-MM-DD or YYYY-MM format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return datetime.strptime(date_str, "%Y-%m")


def resolve_run_date_value(
    config: Mapping[str, Any],
    keys: Sequence[str] = ("output_date", "current_date", "run_date"),
) -> str | None:
    run_cfg = config.get("run", {}) if isinstance(config, Mapping) else {}
    if not isinstance(run_cfg, Mapping):
        return None
    for key in keys:
        value = run_cfg.get(key)
        if value:
            return str(value)
    return None
