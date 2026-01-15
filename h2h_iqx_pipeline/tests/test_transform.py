import pandas as pd

from h2h_pipeline import transform


def test_transform_applies_mappings_and_defaults(tmp_path):
    raw = {
        "IBEW D4": pd.DataFrame(
            {
                "last_name": ["Smith"],
                "first_name": ["Alex"],
                "email": ["ALEX@EXAMPLE.COM"],
                "phone_number": ["5551234567"],
                "location_zip": ["1234"],
                "profession": ["Ironworkers"],
                "service_branch": ["Army"],
                "external_source": ["IBEW D4"],
                "external_source_code": ["IBEW_4"],
            }
        )
    }

    prof_map = tmp_path / "professions.yml"
    prof_map.write_text('"Ironworkers": "Structural Iron and Steel Workers"\n', encoding="utf-8")
    service_map = tmp_path / "service_branches.yml"
    service_map.write_text('"Army": "Service: Army"\n', encoding="utf-8")

    config = {
        "run": {"output_date": "2025-12-04"},
        "date_handling": {"output_format": "%m/%d/%Y"},
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
            "external_identifier_strategy": "email_or_phone",
            "date_available_offset_days": 6,
            "end_date_years_from_available": 1,
        },
        "mappings": {"professions": prof_map, "service_branches": service_map},
    }

    result = transform.build_combo(month="2025-12", raw_data=raw, config=config)
    combo = result.combo_df

    assert combo.at[0, "profession"] == "Structural Iron and Steel Workers"
    assert combo.at[0, "internal_comment"] == "Service: Army"
    assert combo.at[0, "phone_number"] == "555-123-4567"
    assert combo.at[0, "location_zip"] == "01234"
    assert combo.at[0, "external_identifier"] == "alex@example.com"
    assert combo.at[0, "date_available"] == "12/10/2025"
    assert combo.at[0, "end_date"] == "12/10/2026"
    assert not result.validation.missing_profession_mappings
    assert not result.validation.invalid_phones


def test_transform_reports_missing_and_invalid(tmp_path):
    raw = {
        "IBEW D4": pd.DataFrame(
            {
                "last_name": ["Doe"],
                "first_name": ["Sam"],
                "email": ["sam@example.com"],
                "phone_number": ["12345"],
                "location_zip": ["12"],
                "profession": ["Unknown"],
                "service_branch": ["Space Force"],
                "external_source": ["IBEW D4"],
            }
        )
    }
    config = {
        "iqx_import": {"column_order": ["email", "phone_number", "location_zip", "profession", "service_branch"]},
        "defaults": {"external_identifier_strategy": "blank"},
        "mappings": {"professions": tmp_path / "prof.yml", "service_branches": tmp_path / "svc.yml"},
    }
    # create empty mapping files
    (tmp_path / "prof.yml").write_text("", encoding="utf-8")
    (tmp_path / "svc.yml").write_text("", encoding="utf-8")

    result = transform.build_combo(month="2025-12", raw_data=raw, config=config)

    assert "Unknown" in result.validation.missing_profession_mappings
    assert "Space Force" in result.validation.missing_service_branch_mappings
    assert "0:12345" in result.validation.invalid_phones
    assert "0:12" in result.validation.invalid_zips
