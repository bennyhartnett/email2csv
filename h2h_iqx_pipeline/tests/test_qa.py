from pathlib import Path

import pandas as pd

from h2h_pipeline import qa
from h2h_pipeline.models import DedupResult, ValidationReport, DiscoveryResult


def test_qa_report_includes_validation(tmp_path):
    combo = pd.DataFrame({"email": ["a@example.com"], "external_source": ["IBEW D4"]})
    dedup_result = DedupResult(
        cleaned_df=combo,
        duplicates_df=combo.iloc[0:0],
        stats={"input_rows": 1, "duplicates_removed": 0},
    )
    export_paths = {"combo_excel": tmp_path / "combo.xlsx"}
    validation = ValidationReport(
        missing_profession_mappings={"Unknown"},
        missing_service_branch_mappings=set(),
        invalid_phones=["0:12345"],
        invalid_zips=["0:12"],
        missing_required_columns={"phone_number"},
    )
    discovery = DiscoveryResult(
        month="2025-12",
        input_root=tmp_path,
        month_dir=None,
        missing_sources=["IBEW D8"],
        month_dir_missing=True,
    )
    config = {"paths": {"output_root": str(tmp_path)}}

    report = qa.generate_report(
        run_label="2025-12-04",
        combo_df=combo,
        dedup_result=dedup_result,
        export_paths=export_paths,
        validation=validation,
        discovery=discovery,
        counts_before={"IBEW D4": 1},
        counts_after={"IBEW D4": 1},
        config=config,
    )

    content = Path(report).read_text(encoding="utf-8")
    assert "Missing profession mappings: ['Unknown']" in content
    assert "Invalid phone values (row:value): ['0:12345']" in content
    assert "Missing required columns: ['phone_number']" in content
    assert "IBEW D4: 1" in content
    assert "Missing source files for: IBEW D8" in content
