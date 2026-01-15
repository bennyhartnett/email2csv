import pandas as pd

from h2h_pipeline import ingestion
from h2h_pipeline.models import DiscoveryResult


def test_ingestion_normalizes_columns_and_sets_source(tmp_path):
    excel_path = tmp_path / "source.xlsx"
    pd.DataFrame(
        {
            "Record ID": ["182240530351"],
            "First Name": ["Jane"],
            "Last Name": ["Doe"],
            "Create Date": ["2025-12-04 23:09:00"],
            "Mobile Phone Number": ["(555) 123-4567"],
            "Email": ["Jane@Example.com"],
            "Branch of Service": ["Army"],
            "Trade of Interest": ["Ironworkers"],
            "Postal Code": ["1234"],
        }
    ).to_excel(excel_path, index=False)

    discovery = DiscoveryResult(
        month="2025-12",
        input_root=tmp_path,
        month_dir=tmp_path,
        sources={"IBEW D4": excel_path},
        previous_combo=None,
    )
    config = {"sources": [{"name": "IBEW D4", "code": "IBEW_4", "file_pattern": "IBEW D4 *.xlsx"}]}

    frames = ingestion.load_sources(discovery, config)
    df = frames["IBEW D4"]

    assert set(df.columns) >= {
        "last_name",
        "first_name",
        "email",
        "phone_number",
        "location_zip",
        "external_source",
        "external_source_code",
        "service_branch",
        "profession",
        "external_identifier",
        "create_date",
    }
    assert df.at[0, "external_source"] == "IBEW D4"
    assert df.at[0, "external_source_code"] == "IBEW_4"
    assert df.at[0, "email"] == "Jane@Example.com"


def test_ingestion_filters_by_last_import_date(tmp_path):
    excel_path = tmp_path / "source.xlsx"
    pd.DataFrame(
        {
            "First Name": ["Older", "Newer"],
            "Last Name": ["One", "Two"],
            "Email": ["old@example.com", "new@example.com"],
            "Create Date": ["2025-11-10 10:00:00", "2025-11-25 10:00:00"],
            "Mobile Phone Number": ["5551112222", "5552223333"],
        }
    ).to_excel(excel_path, index=False)

    discovery = DiscoveryResult(
        month="2025-12",
        input_root=tmp_path,
        month_dir=tmp_path,
        sources={"IBEW D4": excel_path},
        previous_combo=None,
    )
    config = {
        "sources": [{"name": "IBEW D4", "code": "IBEW_4", "file_pattern": "IBEW D4 *.xlsx"}],
        "date_handling": {
            "last_import_strategy": "from_config",
            "last_import_date_by_source": {"IBEW D4": "2025-11-20"},
        },
    }

    frames = ingestion.load_sources(discovery, config)
    df = frames["IBEW D4"]

    assert len(df) == 1
    assert df.at[df.index[0], "email"] == "new@example.com"
