from pathlib import Path
from typing import Any, Mapping

import yaml


def load_config(path: Path) -> Mapping[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
