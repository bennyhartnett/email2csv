from pathlib import Path
from typing import Any, Mapping

from . import dedup, export, file_discovery, ingestion, qa, transform
from .constants import SOURCE_COLUMN
from .logging_config import configure_logging


def run_pipeline(month: str, input_root: Path, config: Mapping[str, Any]) -> None:
    """Top-level orchestration of the H2H → IQX pipeline."""

    configure_logging(config)

    # 1. Discover files and paths
    discovery = file_discovery.discover_month_files(
        month=month,
        input_root=input_root,
        config=config,
    )

    # 2. Ingest source Excel and existing Combo
    raw_data = ingestion.load_sources(discovery, config=config)

    # 3. Transform and standardize into "Combo All Lists" equivalent
    transform_result = transform.build_combo(month=month, raw_data=raw_data, config=config)
    combo_df = transform_result.combo_df

    # 4. De-duplicate and create "Combo Dups Removed"
    dedup_result = dedup.remove_duplicates(combo_df, config=config)

    # 5. Export Excel + CSV for IQX
    export_paths = export.write_outputs(
        month=month,
        combo_df=combo_df,
        dedup_df=dedup_result.cleaned_df,
        config=config,
    )

    # 6. Generate QA summary report
    qa.generate_report(
        month=month,
        combo_df=combo_df,
        dedup_result=dedup_result,
        export_paths=export_paths,
        validation=transform_result.validation,
        discovery=discovery,
        counts_before=_counts_by_source(combo_df),
        counts_after=_counts_by_source(dedup_result.cleaned_df),
        config=config,
    )


def _counts_by_source(df):
    if SOURCE_COLUMN not in df.columns:
        return {}
    return df[SOURCE_COLUMN].value_counts().to_dict()
