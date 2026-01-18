from pathlib import Path
from typing import Any, Mapping, MutableMapping

import yaml

REQUIRED_ROOT_KEYS = ["paths", "sources", "iqx_import"]


def load_config(path: Path, overrides: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    if not isinstance(cfg, MutableMapping):
        raise ValueError(f"Config {path} must be a mapping.")
    normalize_config_paths(cfg, path)
    if overrides:
        _deep_update(cfg, overrides)
    validate_config(cfg, path)
    return cfg


def normalize_config_paths(cfg: MutableMapping[str, Any], path: Path) -> None:
    """Resolve relative paths in config using the config file's directory."""
    base_dir = path.parent
    mappings = cfg.get("mappings", {})
    if isinstance(mappings, MutableMapping):
        for key, value in list(mappings.items()):
            if value:
                mappings[key] = _resolve_mapping_path(value, base_dir)


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


def _resolve_mapping_path(value: str | Path, base_dir: Path) -> str:
    path = Path(value)
    if path.is_absolute():
        return str(path)

    base_candidate = (base_dir / path).resolve()
    if base_candidate.exists():
        return str(base_candidate)

    if base_dir.name.lower() == "config":
        parent_candidate = (base_dir.parent / path).resolve()
        if parent_candidate.exists():
            return str(parent_candidate)

    cwd_candidate = (Path.cwd() / path).resolve()
    if cwd_candidate.exists():
        return str(cwd_candidate)

    return str(base_candidate)


def _deep_update(target: MutableMapping[str, Any], overrides: Mapping[str, Any]) -> None:
    for key, value in overrides.items():
        if isinstance(value, Mapping):
            current = target.get(key)
            if not isinstance(current, MutableMapping):
                current = {}
                target[key] = current
            _deep_update(current, value)
        else:
            target[key] = value
