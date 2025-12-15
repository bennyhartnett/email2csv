import pandas as pd

from h2h_pipeline import ingestion
from h2h_pipeline.models import DiscoveryResult


def test_ingestion_normalizes_columns_and_sets_source(tmp_path):
    excel_path = tmp_path / "source.xlsx"
    pd.DataFrame(
        {
            "last_name": ["Doe"],
            "first name": ["Jane"],
            "Email Address": ["Jane@Example.com"],
            "phone number": ["(555) 123-4567"],
            "ZIP": ["1234"],
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

    assert set(df.columns) >= {"Last Name", "First Name", "Email", "Phone", "Zip", "Source", "External Source"}
    assert df.at[0, "Source"] == "IBEW D4"
    assert df.at[0, "External Source"] == "IBEW_4"
    assert df.at[0, "Email"] == "Jane@Example.com"
