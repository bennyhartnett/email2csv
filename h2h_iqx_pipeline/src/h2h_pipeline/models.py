from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Mapping, Optional, Set, List

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
class ValidationReport:
    """Validation findings captured during transform."""

    missing_profession_mappings: Set[str] = field(default_factory=set)
    missing_service_branch_mappings: Set[str] = field(default_factory=set)
    invalid_phones: List[str] = field(default_factory=list)
    invalid_zips: List[str] = field(default_factory=list)
    missing_required_columns: Set[str] = field(default_factory=set)


@dataclass
class TransformResult:
    """Output of transform stage."""

    combo_df: pd.DataFrame
    validation: ValidationReport


@dataclass
class DedupResult:
    """Result of duplicate removal."""

    cleaned_df: pd.DataFrame
    duplicates_df: pd.DataFrame
    stats: Dict[str, int] | None = None
