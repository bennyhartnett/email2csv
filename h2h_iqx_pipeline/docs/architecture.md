# Architecture

## Goals
- Keep the prototype deterministic and file-based: no network calls, no browser automation, no LLM usage.
- Make behavior configurable via YAML rather than hard-coded logic.
- Mirror the existing Excel workflow so results are explainable and auditable.

## Components
- **CLI (`h2h_pipeline.cli`)** – Parses arguments and hands control to `run_pipeline`.
- **Config loader (`config_loader`)** – Reads YAML config and supplies shared settings.
- **Logging (`logging_config`)** – Configures console and file logging, using `paths.log_dir`.
- **File discovery (`file_discovery`)** – Locates the month folder, source files, and prior Combo files.
- **Ingestion (`ingestion`)** – Loads Excel files into DataFrames, normalizes column names, and tags rows with source metadata.
- **Transform (`transform`)** – Cleans and standardizes fields (service branch, profession, phone, zip), reshapes into the Combo schema, and applies date defaults.
- **De-duplication (`dedup`)** – Detects duplicate candidates and keeps the preferred record using `mappings/source_priority.yml`.
- **Export (`export`)** – Writes Combo, Combo Dups Removed, bulk import CSV, and IQX-ready CSV with correct column order.
- **QA (`qa`)** – Produces a summary of counts, duplicates, anomalies, and output paths.

## Data flow
1. **Discover**: Determine month folder under `paths.input_root` and enumerate files per `sources.file_pattern`.
2. **Ingest**: Read each source Excel, normalize columns, attach `source` and `source_code`. Also load prior Combo (if available) to set last-import cutoffs.
3. **Transform**: Apply mappings from `config/mappings/`, format phone/zip, set default invite flags and availability dates, and build a unified Combo DataFrame.
4. **De-duplicate**: Identify duplicate records (email/phone/last+first+zip) and keep the highest-priority source. Produce a cleaned DataFrame plus duplicate report.
5. **Export**: Write Excel/CSV artifacts to `paths.output_root` using patterns in `combo_files`. Ensure IQX CSV honors `iqx_import.column_order`.
6. **QA**: Generate a report (text/CSV) summarizing counts, duplicates removed, and any anomalies (missing mapping keys, invalid phones/zips, missing required columns).

## Configuration
- Top-level settings live in `config/example_config.yml` (copy to `config/local_config.yml` for use).
- Mapping tables live in `config/mappings/` and are loaded at runtime so changes do not require code edits.
- The pipeline should validate that mapping keys cover all observed raw values and surface missing keys in QA output.

## Extensibility
- Additional sources can be added by extending `sources` in the config and adding mapping entries.
- Future enhancements (email ingestion, browser automation, enrichment) can hook into the same pipeline stages without changing the file-based contract.
