import pandas as pd

from h2h_pipeline import transform


def test_transform_applies_mappings_and_defaults(tmp_path):
    raw = {
        "IBEW D4": pd.DataFrame(
            {
                "Last Name": ["Smith"],
                "First Name": ["Alex"],
                "Email": ["ALEX@EXAMPLE.COM"],
                "Phone": ["5551234567"],
                "Zip": ["1234"],
                "Profession": ["Ironworkers"],
                "Service Branch": ["Army"],
                "Source": ["IBEW D4"],
                "External Source": ["IBEW_4"],
            }
        )
    }

    prof_map = tmp_path / "professions.yml"
    prof_map.write_text('"Ironworkers": "Structural Iron and Steel Workers"\n', encoding="utf-8")
    service_map = tmp_path / "service_branches.yml"
    service_map.write_text('"Army": "Service: Army"\n', encoding="utf-8")

    config = {
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
        "mappings": {"professions": prof_map, "service_branches": service_map},
    }

    result = transform.build_combo(month="2025-12", raw_data=raw, config=config)
    combo = result.combo_df

    assert combo.at[0, "Profession"] == "Structural Iron and Steel Workers"
    assert combo.at[0, "Internal Comments"] == "Service: Army"
    assert combo.at[0, "Phone"] == "555-123-4567"
    assert combo.at[0, "Zip"] == "01234"
    assert combo.at[0, "External ID"] == "alex@example.com"
    # Dates derived from first day of month (2025-12-01) + offsets
    assert combo.at[0, "Date Available"] == "2025-12-02"
    assert combo.at[0, "End Date"] == "2026-12-02"
    assert not result.validation.missing_profession_mappings
    assert not result.validation.invalid_phones


def test_transform_reports_missing_and_invalid(tmp_path):
    raw = {
        "IBEW D4": pd.DataFrame(
            {
                "Last Name": ["Doe"],
                "First Name": ["Sam"],
                "Email": ["sam@example.com"],
                "Phone": ["12345"],
                "Zip": ["12"],
                "Profession": ["Unknown"],
                "Service Branch": ["Space Force"],
                "Source": ["IBEW D4"],
            }
        )
    }
    config = {
        "iqx_import": {"column_order": ["Email", "Phone", "Zip", "Profession", "Service Branch", "External ID"]},
        "defaults": {},
        "mappings": {"professions": tmp_path / "prof.yml", "service_branches": tmp_path / "svc.yml"},
    }
    # create empty mapping files
    (tmp_path / "prof.yml").write_text("", encoding="utf-8")
    (tmp_path / "svc.yml").write_text("", encoding="utf-8")

    result = transform.build_combo(month="2025-12", raw_data=raw, config=config)

    assert "Unknown" in result.validation.missing_profession_mappings
    assert "Space Force" in result.validation.missing_service_branch_mappings
    assert "12345" in result.validation.invalid_phones
    assert "12" in result.validation.invalid_zips
