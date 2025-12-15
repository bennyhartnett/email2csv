from pathlib import Path
from typing import Any, Mapping, MutableMapping

import yaml

REQUIRED_ROOT_KEYS = ["paths", "sources", "iqx_import"]


def load_config(path: Path) -> Mapping[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    validate_config(cfg, path)
    return cfg


def validate_config(cfg: MutableMapping[str, Any], path: Path) -> None:
    missing = [key for key in REQUIRED_ROOT_KEYS if key not in cfg]
    if missing:
        raise ValueError(f"Config {path} missing required sections: {missing}")

    paths = cfg.get("paths", {})
    input_root = Path(paths.get("input_root", ""))
    if not input_root.exists():
        raise FileNotFoundError(f"input_root does not exist: {input_root}")

    sources = cfg.get("sources", [])
    if not sources:
        raise ValueError("Config must define at least one source entry.")
    for src in sources:
        for field in ("name", "code", "file_pattern"):
            if field not in src:
                raise ValueError(f"Source entry missing '{field}': {src}")

    column_order = cfg.get("iqx_import", {}).get("column_order")
    if not column_order or not isinstance(column_order, list):
        raise ValueError("iqx_import.column_order must be a non-empty list.")

    mappings = cfg.get("mappings", {})
    for key, val in mappings.items():
        mp = Path(val)
        if not mp.exists():
            raise FileNotFoundError(f"Mapping file for '{key}' not found: {mp}")
