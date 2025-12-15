# Tests

This folder contains unit tests and fixtures for the pipeline.

- `test_file_discovery.py` – Tests for locating month folders and source files.
- `test_ingestion.py` – Tests for loading Excel files and normalizing columns.
- `test_transform.py` – Tests for data transformations (phone formatting, service / profession mapping, column reshaping).
- `test_dedup.py` – Tests for duplicate detection and source priority logic.
- `fixtures/` – Sample configuration and input files for tests.

Run tests with:

```bash
pytest
```
