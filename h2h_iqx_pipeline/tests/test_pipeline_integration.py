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
            "Record ID": ["182240530351", "182190050458"],
            "Last Name": ["Doe", "Roe"],
            "First Name": ["Jane", "Sam"],
            "Create Date": ["2025-12-04 09:00:00", "2025-12-04 10:00:00"],
            "Mobile Phone Number": ["5551112222", "5553334444"],
            "Email": ["jane@example.com", "sam@example.com"],
            "Postal Code": ["12345", "67890"],
            "Trade of Interest": ["Ironworkers", "Electricians/Lineman"],
            "Branch of Service": ["Army", "Air Force"],
        }
    ).to_excel(
        month_dir / "Career Seekers Interested in IBEW D4 11052025-12042025.xlsx",
        index=False,
    )

    pd.DataFrame({"Email": ["old@example.com"], "Mobile Phone Number": ["5559998888"]}).to_excel(
        prev_dir / "Combo H2H IBEW 4 8 9 Iron 2025-11.xlsx", index=False
    )

    config = {
        "paths": {"input_root": str(input_root), "output_root": str(tmp_path / "out"), "log_dir": str(tmp_path / "logs")},
        "run": {"previous_month": "2025-11", "output_date": "2025-12-04"},
        "date_handling": {"output_format": "%m/%d/%Y"},
        "sources": [
            {
                "name": "IBEW D4",
                "code": "IBEW_4",
                "file_pattern": "Career Seekers Interested in IBEW D4 *.xlsx",
            },
        ],
        "combo_files": {
            "excel_pattern": "Combo H2H IBEW 4 8 9 Iron {date}.xlsx",
            "csv_pattern": "Bulk Import H2H Combo IBEW 4 8 9 Iron {date}.csv",
        },
        "iqx_import": {
            "column_order": [
                "Summary - Bulk Import Failure Notes",
                "external_identifier",
                "external_source",
                "internal_comment",
                "date_available",
                "end_date",
                "location_radius",
                "last_name",
                "first_name",
                "phone_number",
                "email",
                "location_zip",
                "profession",
                "industry",
                "talent_price_category",
                "clearance_level",
                "clearance_agency",
                "clearance_status",
                "clearance_investigation",
            ]
        },
        "defaults": {
            "location_radius": 100,
            "industry": "23 Construction",
            "talent_price_category": "A",
            "external_identifier_strategy": "blank",
            "date_available_offset_days": 6,
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
        '"Army": "Service: Army"\n"Air Force": "Service: Air Force"\n', encoding="utf-8"
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
    combo_csv = out_dir / "Bulk Import H2H Combo IBEW 4 8 9 Iron 2025-12-04.csv"
    qa_report = out_dir / "QA Report 2025-12-04.txt"

    assert combo_csv.exists(), "IQX CSV should be produced"
    assert qa_report.exists(), "QA report should be produced"

    df_out = pd.read_csv(combo_csv)
    # One of the two rows should remain after dedup (none duplicates here)
    assert len(df_out) == 2
    assert sorted(df_out["profession"].unique()) == [
        "Electricians",
        "Structural Iron and Steel Workers",
    ]

    content = qa_report.read_text(encoding="utf-8")
    assert "Rows in Combo: 2" in content
    assert "Missing profession mappings: none" in content
