import pandas as pd

from h2h_pipeline import dedup


def test_dedup_prefers_higher_priority(tmp_path):
    data = pd.DataFrame(
        {
            "external_source": ["Ironworkers", "IBEW D8"],
            "last_name": ["Doe", "Doe"],
            "first_name": ["John", "John"],
            "email": ["john@example.com", "john@example.com"],
            "phone_number": ["555-123-4567", "555-123-4567"],
            "location_zip": ["12345", "12345"],
        }
    )

    priority_file = tmp_path / "source_priority.yml"
    priority_file.write_text('"Ironworkers": 100\n"IBEW D8": 90\n', encoding="utf-8")

    config = {"mappings": {"source_priority": priority_file}}

    result = dedup.remove_duplicates(data, config)

    assert len(result.cleaned_df) == 1
    assert len(result.duplicates_df) == 1
    assert result.cleaned_df.iloc[0]["external_source"] == "Ironworkers & IBEW D8"


def test_dedup_keeps_rows_without_identifiers():
    data = pd.DataFrame({"external_source": ["A", "B"]})

    result = dedup.remove_duplicates(data, config={})

    assert len(result.cleaned_df) == 2
    assert result.stats["duplicates_removed"] == 0


def test_dedup_ignores_blank_identifier_values():
    data = pd.DataFrame(
        {
            "external_source": ["A", "B"],
            "email": ["", ""],
            "phone_number": [pd.NA, pd.NA],
            "last_name": ["", ""],
            "first_name": ["", ""],
            "location_zip": ["", ""],
        }
    )

    result = dedup.remove_duplicates(data, config={})

    assert len(result.cleaned_df) == 2
    assert result.stats["duplicates_removed"] == 0
