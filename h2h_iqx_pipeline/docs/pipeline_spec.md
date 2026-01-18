# H2H IQX Pipeline Spec (Translations and Transformations)

This document captures the exact input discovery, normalization, translation, transformation,
deduplication, and export rules used by the current codebase so the flow can be rebuilt and
validated by management.

## 1) File discovery

- Input root: `paths.input_root` from config.
- Month folder candidates (first existing wins):
  - `{input_root}/{month}`
  - `{input_root}/Vet Talents {month}`
  - `{input_root}/ORIG - Vet Talents {month}`
  - Any directory under input_root that contains `{month}` in its name
- If no month folder is found:
  - Use `{input_root}/inputs` if it exists and contains at least one file matching any source pattern.
  - Otherwise use `{input_root}` if it contains at least one file matching any source pattern.
- Source files:
  - For each `sources[]` entry, use `file_pattern` (glob) and select the first match in the month folder.
- Previous combo (optional):
  - Uses `run.previous_month`.
  - Searches the previous month folder for:
    - `combo_files.excel_pattern` with `{date}` formatted as previous month
    - Fallback: `*Combo*.xlsx`

## 2) Ingestion

### 2.1 Excel read
- Reads each Excel file with `pandas.read_excel(..., dtype=str)`.
- Rows that are entirely blank are dropped (`dropna(how="all")`).
- If a file is missing, an empty DataFrame is returned and a warning is logged.

### 2.2 Column normalization (header mapping)

Header tokens are normalized by:
- `strip()`
- `lower()`
- `-` and `_` replaced with spaces

The following input headers are mapped to canonical snake_case columns:

| Input header (normalized) | Canonical column |
| --- | --- |
| record id | external_identifier |
| external identifier | external_identifier |
| last name | last_name |
| first name | first_name |
| email, email address | email |
| phone, phone number, mobile phone number | phone_number |
| zip, zip code, zipcode, postal, postal code | location_zip |
| profession, trade of interest | profession |
| service branch, branch of service, service | service_branch |
| create date | create_date |
| external source | external_source |
| external source code | external_source_code |

Any header not mapped is preserved as-is.

### 2.3 Source tagging
- `external_source` is set to:
  - `sources[].output_label` if provided
  - otherwise `sources[].name`
- `external_source_code` is set to `sources[].code` (if provided).

### 2.4 Last import filtering
Config: `date_handling.last_import_strategy` controls filtering.

**from_config**
- Uses `date_handling.last_import_date_by_source`.

**from_combo_file**
- If previous combo exists, a cutoff date is derived per source from the latest `create_date` (or `date_available` if `create_date` is absent).
- If previous combo is missing, and `last_import_date_by_source` is defined, it is used as fallback.

**Cutoff inclusion**
- If `date_handling.include_cutoff_date` is set, it controls inclusion.
- If `include_cutoff_date` is not set:
  - It defaults to `true` only when a previous combo exists and `exclude_previously_imported` is true.
  - Otherwise it defaults to `false`.

**Filtering rule (by create_date)**
- If `create_date` is missing or unparseable: keep the row.
- Otherwise keep the row if:
  - `create_date.date >= cutoff_date` when cutoff is inclusive
  - `create_date.date > cutoff_date` when cutoff is exclusive

### 2.5 Exclude previously imported (optional)
Config: `date_handling.exclude_previously_imported` (default true).

If a previous combo exists:
- Normalize previous combo identifiers:
  - `email` lowercased/trimmed
  - `phone_number` digits-only
  - `name_zip` = `last_name|first_name|location_zip` (normalized)
- Drop any new row where **any** of the above identifiers matches a previous combo row.

If no previous combo exists: this step is skipped.

## 3) Transform

### 3.1 Combine sources
All source DataFrames are concatenated (previous combo is excluded).

### 3.2 Field normalization

- **email**: lowercased and trimmed.
- **phone_number**:
  - digits-only extraction
  - if 11 digits and starts with `1`, drop the leading `1`
  - if 10 digits: format as `XXX-XXX-XXXX`
  - otherwise leave digits-only; invalid values are tracked in QA
- **location_zip**:
  - digits-only extraction
  - if 5+ digits: keep first 5
  - if 1-4 digits: left pad with zeros to 5
  - empty stays empty; invalid values are tracked in QA

### 3.3 Mappings

- **profession** uses `config/mappings/professions.yml`.
  - If a key is missing in the mapping, the raw value is kept and reported in QA.
- **internal_comment** is derived from `service_branch` using `config/mappings/service_branches.yml`:
  - If key exists in mapping: mapped value used.
  - If missing: `Service:  {raw}` (note the double space after the colon).
  - Missing keys are recorded in QA.

### 3.4 Defaults and computed fields

- `location_radius` = `defaults.location_radius` or `defaults.location_radius_miles` (default 100)
- `industry` = `defaults.industry` (string)
- `talent_price_category` = `defaults.talent_price_category` or `defaults.pay_scale`
- `Summary - Bulk Import Failure Notes` = blank string

**Date fields**
- Base date (in order):
  1) `run.output_date` or `run.current_date` or `run.run_date`
  2) latest `create_date` from the inputs
  3) first day of `month` (YYYY-MM)
- `date_available` = base date + `defaults.date_available_offset_days`
- `end_date` = `date_available` + `defaults.end_date_years_from_available` years
- Format uses `date_handling.output_format` or `defaults.date_format` (default `%m/%d/%Y`)

**external_identifier**
- If `defaults.external_identifier_strategy = "blank"`: set to empty string.
- If `"record_id"`: use `external_identifier` from input (Record ID).
- Otherwise: `email` if present, else `phone_number`.

### 3.5 Required columns validation
Required (for QA reporting): `last_name`, `first_name`, `email`, `phone_number`, `location_zip`, `profession`, `service_branch`.

### 3.6 Ensure output columns exist
All columns listed in `iqx_import.column_order` are created if missing. Clearance fields are always added empty:
- `clearance_level`
- `clearance_agency`
- `clearance_status`
- `clearance_investigation`

## 4) Deduplication

### 4.1 Priority
Priority comes from `config/mappings/source_priority.yml`. Higher number wins. Missing = 0.

### 4.2 Matching keys
Rows are grouped if **any** of the following match:
- `email` (normalized)
- `phone_number` (digits-only)
- `name_zip` = normalized `last_name|first_name|location_zip`

A union-find algorithm merges all connected rows (transitive matches included).

### 4.3 Winner selection
Rows are processed in descending priority. The first row in the group is kept.

### 4.4 Source merging
For each kept row, `external_source` becomes the set of all sources in its group,
sorted by priority (desc) then name, joined with `" & "` (e.g., `Ironworkers & IBEW 9`).

## 5) Export

### 5.1 Output root and run label
- Output folder: `paths.output_root`
- Run label:
  - `run.output_date` or `run.current_date` or `run.run_date`, else
  - latest `create_date` in inputs, else
  - `month`

### 5.2 Files
- Combo Excel: `combo_files.excel_pattern` with `{date}` = run label
- Dups Removed Excel: `Combo Dups Removed {run_label}.xlsx`
- IQX CSV: `combo_files.csv_pattern` with `{date}` = run label

### 5.3 Column order
- CSV is **restricted** to `iqx_import.column_order`.
- Excel files keep all columns, with `iqx_import.column_order` first and extras appended.

## 6) QA Report
Outputs a text report with:
- Row counts (combo, deduped)
- Duplicates removed
- Output paths
- Per-source counts (before/after dedup)
- Missing mappings (profession, service branch)
- Invalid phones/zips
- Missing required columns
- Discovery warnings (missing month dir or missing source files)

## 7) Output CSV schema (IQX bulk import)

| Column | Source / Rule |
| --- | --- |
| Summary - Bulk Import Failure Notes | blank |
| external_identifier | blank/record_id/email_or_phone (per strategy) |
| external_source | source label or merged sources |
| internal_comment | mapped `service_branch` or `Service:  {raw}` |
| date_available | computed date (formatted) |
| end_date | computed date (formatted) |
| location_radius | default |
| last_name | raw from input |
| first_name | raw from input |
| phone_number | normalized phone |
| email | normalized email |
| location_zip | normalized zip |
| profession | mapped or raw |
| industry | default |
| talent_price_category | default |
| clearance_level | blank |
| clearance_agency | blank |
| clearance_status | blank |
| clearance_investigation | blank |
