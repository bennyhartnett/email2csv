from pathlib import Path

import pandas as pd

from h2h_pipeline import export


def test_export_writes_files_and_reorders(tmp_path):
    combo_df = pd.DataFrame({"external_source": ["IBEW D4"], "email": ["a@example.com"]})
    dedup_df = combo_df.copy()

    config = {
        "paths": {"output_root": str(tmp_path / "out")},
        "combo_files": {"excel_pattern": "Combo H2H {date}.xlsx", "csv_pattern": "Bulk Import {date}.csv"},
        "iqx_import": {"column_order": ["email", "external_source"]},
    }

    paths = export.write_outputs(run_label="2025-12-04", combo_df=combo_df, dedup_df=dedup_df, config=config)

    assert Path(paths["combo_excel"]).exists()
    assert Path(paths["iqx_csv"]).exists()

    csv_content = Path(paths["iqx_csv"]).read_text(encoding="utf-8")
    assert csv_content.splitlines()[0] == "email,external_source"
