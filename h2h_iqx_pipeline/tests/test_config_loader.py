import pytest
from pathlib import Path

from h2h_pipeline.config_loader import load_config, validate_config


def test_validate_config_missing_sections(tmp_path):
    cfg_path = tmp_path / "cfg.yml"
    cfg_path.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError):
        load_config(cfg_path)


def test_validate_config_checks_input_root(tmp_path):
    cfg = {
        "paths": {"input_root": str(tmp_path / "missing"), "output_root": str(tmp_path / "out"), "log_dir": "logs"},
        "sources": [{"name": "A", "code": "A", "file_pattern": "*.xlsx"}],
        "iqx_import": {"column_order": ["email"]},
        "mappings": {},
    }
    cfg_path = tmp_path / "cfg.yml"
    import yaml

    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        load_config(cfg_path)
