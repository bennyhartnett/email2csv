from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Mapping, Optional

import pandas as pd


@dataclass
class DiscoveryResult:
    """Container for discovered input/output paths."""

    month: str
    input_root: Path
    month_dir: Optional[Path] = None
    sources: Optional[Mapping[str, Path]] = None
    previous_combo: Optional[Path] = None


@dataclass
class DedupResult:
    """Result of duplicate removal."""

    cleaned_df: pd.DataFrame
    duplicates_df: pd.DataFrame
    stats: Dict[str, int] | None = None
