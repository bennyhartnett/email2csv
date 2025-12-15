from pathlib import Path

import pandas as pd

from h2h_pipeline.pipeline import run_pipeline
from h2h_pipeline.config_loader import load_config


def test_run_pipeline_end_to_end(tmp_path, monkeypatch):
    # Prepare fixtures
    input_root = tmp_path / "input_data"
    input_root.mkdir()
    month_dir = input_root / "Vet Talents 2025-12"
    month_dir.mkdir(parents=True)
    prev_dir = input_root / "2025-11"
    prev_dir.mkdir()

    pd.DataFrame(
        {
            "Last Name": ["Doe", "Roe"],
            "First Name": ["Jane", "Sam"],
            "Email": ["jane@example.com", "sam@example.com"],
            "Phone": ["5551112222", "5553334444"],
            "Zip": ["12345", "67890"],
            "Profession": ["Ironworkers", "Electricians/Lineman"],
            "Service Branch": ["Army", "Air"],
        }
    ).to_excel(month_dir / "IBEW D4 sample.xlsx", index=False)

    pd.DataFrame({"Email": ["old@example.com"], "Phone": ["5559998888"]}).to_excel(
        prev_dir / "Combo H2H IBEW 4 8 9 Iron 2025-11.xlsx", index=False
    )

    config = {
        "paths": {"input_root": str(input_root), "output_root": str(tmp_path / "out"), "log_dir": str(tmp_path / "logs")},
        "run": {"previous_month": "2025-11"},
        "sources": [
            {"name": "IBEW D4", "code": "IBEW_4", "file_pattern": "IBEW D4 *.xlsx"},
        ],
        "combo_files": {
            "excel_pattern": "Combo H2H IBEW 4 8 9 Iron {date}.xlsx",
            "csv_pattern": "Bulk Import H2H Combo IBEW 4 8 9 Iron {date}.csv",
        },
        "iqx_import": {
            "column_order": [
                "External ID",
                "External Source",
                "Invited",
                "Source",
                "Internal Comments",
                "Last Name",
                "First Name",
                "Phone",
                "Email",
                "Zip",
                "Profession",
                "Date Available",
                "End Date",
                "Location Radius",
                "Trade",
                "Union Code",
                "Pay Scale",
            ]
        },
        "defaults": {
            "invited_flag": "I",
            "location_radius_miles": 100,
            "trade": "Electricians",
            "union_code": "22 Utilities",
            "pay_scale": "A",
            "date_available_offset_days": 1,
            "end_date_years_from_available": 1,
        },
        "mappings": {
            "professions": str(tmp_path / "prof.yml"),
            "service_branches": str(tmp_path / "svc.yml"),
            "source_priority": str(tmp_path / "priority.yml"),
        },
    }

    Path(config["mappings"]["professions"]).write_text(
        '"Ironworkers": "Structural Iron and Steel Workers"\n"Electricians/Lineman": "Electricians"\n',
        encoding="utf-8",
    )
    Path(config["mappings"]["service_branches"]).write_text(
        '"Army": "Service: Army"\n"Air": "Service: Air"\n', encoding="utf-8"
    )
    Path(config["mappings"]["source_priority"]).write_text('"IBEW D4": 90\n', encoding="utf-8")

    # Write config to disk and load it to mimic real usage
    config_path = tmp_path / "local_config.yml"
    import yaml

    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    loaded = load_config(config_path)

    # Run pipeline
    run_pipeline(month="2025-12", input_root=input_root, config=loaded)

    out_dir = Path(config["paths"]["output_root"])
    combo_csv = out_dir / "Bulk Import H2H Combo IBEW 4 8 9 Iron 2025-12.csv"
    qa_report = out_dir / "QA Report 2025-12.txt"

    assert combo_csv.exists(), "IQX CSV should be produced"
    assert qa_report.exists(), "QA report should be produced"

    df_out = pd.read_csv(combo_csv)
    # One of the two rows should remain after dedup (none duplicates here)
    assert len(df_out) == 2
    assert sorted(df_out["Profession"].unique()) == [
        "Electricians",
        "Structural Iron and Steel Workers",
    ]

    content = qa_report.read_text(encoding="utf-8")
    assert "Rows in Combo: 2" in content
    assert "Missing profession mappings: none" in content
