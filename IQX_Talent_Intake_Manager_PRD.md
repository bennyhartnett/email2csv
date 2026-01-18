# PRD: H2H IQX Bulk Import Pipeline (Prototype)

**Working name:** H2H IQX Pipeline

**Document version:** 1.0 (aligned to current codebase)

**Primary outcome:** Produce deterministic, IQX-ready CSVs from locally downloaded H2H Excel files by automating discovery, incremental filtering, normalization, deduplication, and export with a QA summary.

**Primary workflow source:** Current pipeline implementation in `h2h_iqx_pipeline/`.

---

## 1. Executive summary

This product is a file-based prototype that mirrors the offline Excel workflow for Helmets-to-Hardhats (H2H) trade-interest lists. It discovers input files in month folders, normalizes and cleans key fields, removes duplicates using source priority rules, and exports a CSV that matches the IQX bulk import template. A QA report summarizes counts, duplicates removed, and data anomalies.

It is not an intake management system and does not provide email ingestion, persistent storage, remediation workflows, or audit logs beyond file outputs and the QA report.

---

## 2. Problem statement (current scope)

Talent Operations needs a repeatable, low-effort way to:

- pull new rows from overlapping weekly H2H spreadsheets,
- normalize core fields to reduce IQX import errors, and
- generate a consistent bulk import CSV without manual spreadsheet manipulation.

---

## 3. Goals and non-goals

### 3.1 Goals

1. Automate month folder discovery and source file selection.
2. Filter to new rows based on last-import dates and/or previous combo outputs.
3. Normalize email, phone, ZIP, profession, and service-branch fields.
4. Deduplicate records across sources using configurable priority.
5. Export IQX-ready CSV and companion Excel files.
6. Generate a QA report that highlights anomalies and missing mappings.

### 3.2 Non-goals

- Email ingestion or mailbox integration.
- Persistent storage, audit logs, or per-row provenance in a database.
- Human-in-the-loop dedupe UI or merge tooling.
- Import job tracking, remediation queues, or correction exports.
- IQX API integration or browser automation.
- Any changes to IQX schema or downstream recruiting workflow.

---

## 4. Users

1. **Talent Operations Specialist**
   - Runs the pipeline for a given month and uploads the resulting CSV to IQX.

2. **Technical Administrator**
   - Maintains config and mapping files for sources, profession mappings, and source priority.

---

## 5. Supported workflow

1. User downloads H2H source Excel files into a month folder (e.g., `Vet Talents 2025-12`).
2. User runs the CLI or GUI, selects config, input root, output folder, and month.
3. System discovers source files and optional prior Combo file.
4. System filters out previously imported records.
5. System normalizes fields and applies mappings and defaults.
6. System deduplicates and merges source labels.
7. System exports Combo Excel, Dups Removed Excel, IQX CSV, and a QA report.

---

## 6. Functional requirements (current behavior)

### 6.1 File discovery

- Find a month folder using common patterns.
- Choose the first matching file per configured source pattern.
- Optionally load the previous month Combo file for last-import filtering.

**Acceptance criteria**
- If a month folder or source file is missing, the run completes with warnings in the QA report.
- Only one file per source is selected (first match).

### 6.2 Ingestion and column normalization

- Read Excel files with `dtype=str`.
- Normalize column headers using a fixed mapping for known headers.
- Preserve unmapped columns.
- Tag records with `external_source` and `external_source_code` from config.

**Acceptance criteria**
- Known headers map to canonical fields (email, phone, zip, profession, service branch, create date).
- Source label and code are populated when configured.

### 6.3 Incremental extraction

- Support two strategies for last-import filtering:
  - `from_config` via `date_handling.last_import_date_by_source`
  - `from_combo_file` via previous Combo max create date
- If create date is missing or unparseable, keep the row.
- Optional exclusion of previously imported rows by matching email, phone, or name+ZIP against the previous Combo.

**Acceptance criteria**
- Records before the cutoff date are excluded using `create_date` when available.
- Previously imported rows are excluded when enabled.

### 6.4 Normalization and mappings

- Email: lowercase and trim.
- Phone: digits-only; format as `XXX-XXX-XXXX` when 10 digits; otherwise keep digits-only and flag in QA.
- ZIP: digits-only; keep first 5 digits; pad 1-4 digits to 5; flag invalid lengths in QA.
- Profession: map using `config/mappings/professions.yml`; missing keys are reported in QA.
- Service branch: map using `config/mappings/service_branches.yml`; missing keys are reported in QA; raw values fall back to `Service:  <raw>`.

**Acceptance criteria**
- Normalized values appear in outputs.
- Missing mappings are listed in the QA report.

### 6.5 Defaults and computed fields

- `location_radius` default from config (fallback 100).
- `industry` and `talent_price_category` defaults from config.
- `date_available` and `end_date` derived from a base date:
  - config run date or output date, otherwise latest create date, otherwise month start.
- `external_identifier` based on configured strategy: blank, record id, or email/phone fallback.
- `Summary - Bulk Import Failure Notes` is blank.

**Acceptance criteria**
- Defaulted fields are present in exports.
- Dates respect configured format and offsets.

### 6.6 Deduplication

- Group potential duplicates by matching any of:
  - email, phone, or normalized name+ZIP
- Keep the record from the highest-priority source.
- Merge source labels into a single `external_source` string ordered by priority.

**Acceptance criteria**
- Output row count equals input minus removed duplicates.
- Source labeling reflects merged sources in priority order.

### 6.7 Export

- Write:
  - Combo Excel (all columns)
  - Combo Dups Removed Excel (all columns)
  - IQX CSV (restricted to `iqx_import.column_order`)
- Use file patterns from config with the resolved run label.

**Acceptance criteria**
- CSV column order matches the IQX template configuration.
- Excel outputs retain extra columns after the IQX ordered fields.

### 6.8 QA report

- Text report includes:
  - row counts (before/after dedup)
  - duplicates removed
  - output paths
  - per-source counts
  - missing mappings and invalid phone/zip values
  - missing required columns
  - discovery warnings

**Acceptance criteria**
- QA report is written for every run and reflects the current run outputs.

### 6.9 Interfaces

- CLI entry point for automation.
- Tkinter GUI for non-technical users (select config, input, output, month).

**Acceptance criteria**
- GUI can run the pipeline and surface log output.

---

## 7. Non-functional requirements

- Deterministic and config-driven behavior.
- File-based; no network calls.
- Handles hundreds to thousands of rows per run.
- Failures in a single output do not crash the run (logged as warnings).

---

## 8. Data outputs

### 8.1 IQX CSV columns (current template)

- Summary - Bulk Import Failure Notes
- external_identifier
- external_source
- internal_comment
- date_available
- end_date
- location_radius
- last_name
- first_name
- phone_number
- email
- location_zip
- profession
- industry
- talent_price_category
- clearance_level
- clearance_agency
- clearance_status
- clearance_investigation

### 8.2 Supplemental artifacts

- Combo Excel (all columns)
- Combo Dups Removed Excel (all columns)
- QA Report text file

---

## 9. Dependencies and assumptions

- Input files are locally downloaded Excel files.
- Input headers match the known mapping or are manually reconciled via config/mappings changes.
- IQX bulk import template column order is configured correctly.
- Profession and service-branch mappings are maintained in YAML files.

---

## 10. Acceptance criteria (current parity)

This prototype is considered complete when a user can:

- point it at a month folder with H2H spreadsheets,
- filter to new records since the last import date or prior Combo,
- normalize email/phone/ZIP and map professions and service branches,
- remove duplicates based on configured source priority,
- export IQX-ready CSV with correct column order, and
- review a QA report showing counts and anomalies.
