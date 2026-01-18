from pathlib import Path
from typing import Any
import logging

import yaml


def load_yaml_mapping(
    path_val: str | Path | None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    if not path_val:
        return {}
    path = Path(path_val)
    if not path.exists():
        if logger:
            logger.warning("Mapping file %s missing", path)
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}
