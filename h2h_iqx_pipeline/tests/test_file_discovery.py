from pathlib import Path

from h2h_pipeline import file_discovery


def test_discovery_finds_month_sources_and_previous_combo(tmp_path):
    month = "2025-12"
    input_root = tmp_path / "input_data"
    month_dir = input_root / f"Vet Talents {month}"
    month_dir.mkdir(parents=True)

    src_file = month_dir / "Career Seekers Interested in IBEW D4 11052025-12042025.xlsx"
    src_file.touch()

    prev_dir = input_root / "2025-11"
    prev_dir.mkdir(parents=True)
    prev_combo = prev_dir / "Combo H2H IBEW 4 8 9 Iron 2025-11.xlsx"
    prev_combo.touch()

    config = {
        "sources": [
            {
                "name": "IBEW D4",
                "code": "IBEW_4",
                "file_pattern": "Career Seekers Interested in IBEW D4 *.xlsx",
            }
        ],
        "combo_files": {"excel_pattern": "Combo H2H IBEW 4 8 9 Iron {date}.xlsx"},
        "run": {"previous_month": "2025-11"},
    }

    result = file_discovery.discover_month_files(month=month, input_root=input_root, config=config)

    assert result.month_dir == month_dir
    assert result.sources == {"IBEW D4": src_file}
    assert result.previous_combo == prev_combo


def test_discovery_uses_input_root_when_month_dir_missing(tmp_path):
    month = "2025-12"
    input_root = tmp_path / "inputs"
    input_root.mkdir()

    src_file = input_root / "Career Seekers Interested in IBEW D4 11052025-12042025.xlsx"
    src_file.touch()

    config = {
        "sources": [
            {
                "name": "IBEW D4",
                "code": "IBEW_4",
                "file_pattern": "Career Seekers Interested in IBEW D4 *.xlsx",
            }
        ]
    }

    result = file_discovery.discover_month_files(month=month, input_root=input_root, config=config)

    assert result.month_dir == input_root
    assert result.sources == {"IBEW D4": src_file}
    assert not result.month_dir_missing
