import pandas as pd

from h2h_pipeline import dedup


def test_dedup_prefers_higher_priority(tmp_path):
    data = pd.DataFrame(
        {
            "Source": ["Ironworkers", "IBEW D8"],
            "Last Name": ["Doe", "Doe"],
            "First Name": ["John", "John"],
            "Email": ["john@example.com", "john@example.com"],
            "Phone": ["555-123-4567", "555-123-4567"],
            "Zip": ["12345", "12345"],
        }
    )

    priority_file = tmp_path / "source_priority.yml"
    priority_file.write_text('"Ironworkers": 100\n"IBEW D8": 90\n', encoding="utf-8")

    config = {"mappings": {"source_priority": priority_file}}

    result = dedup.remove_duplicates(data, config)

    assert len(result.cleaned_df) == 1
    assert len(result.duplicates_df) == 1
    assert result.cleaned_df.iloc[0]["Source"] == "Ironworkers"
