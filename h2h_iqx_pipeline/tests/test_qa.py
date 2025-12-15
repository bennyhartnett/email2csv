from pathlib import Path

import pandas as pd

from h2h_pipeline import qa
from h2h_pipeline.models import DedupResult, ValidationReport


def test_qa_report_includes_validation(tmp_path):
    combo = pd.DataFrame({"Email": ["a@example.com"]})
    dedup_result = DedupResult(
        cleaned_df=combo,
        duplicates_df=combo.iloc[0:0],
        stats={"input_rows": 1, "duplicates_removed": 0},
    )
    export_paths = {"combo_excel": tmp_path / "combo.xlsx"}
    validation = ValidationReport(
        missing_profession_mappings={"Unknown"},
        missing_service_branch_mappings=set(),
        invalid_phones=["12345"],
        invalid_zips=["12"],
        missing_required_columns={"Phone"},
    )
    config = {"paths": {"output_root": str(tmp_path)}}

    report = qa.generate_report(
        month="2025-12",
        combo_df=combo,
        dedup_result=dedup_result,
        export_paths=export_paths,
        validation=validation,
        config=config,
    )

    content = Path(report).read_text(encoding="utf-8")
    assert "Missing profession mappings: ['Unknown']" in content
    assert "Invalid phone values: ['12345']" in content
    assert "Missing required columns: ['Phone']" in content
