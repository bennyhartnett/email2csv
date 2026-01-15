# Process Overview

This prototype mirrors the current manual Excel workflow for importing H2H talent into IQX.

## Inputs
- Monthly folders under `paths.input_root`, e.g., `Vet Talents 2025-12/`.
- Source Excel files per configured patterns (e.g., `Career Seekers Interested in IBEW D4 11052025-12042025.xlsx`).
- Prior month Combo file (optional) to derive "new since last import" cutoff dates.
- Mapping tables for service branches, professions, and source priority.

## Steps
1. **Locate files**  
   Find the month folder and expected source files using the patterns in `sources`. Identify the prior Combo file if `date_handling.last_import_strategy` is `from_combo_file`.

2. **Load data**  
   Read each source Excel into a DataFrame, standardize column names, tag rows with source metadata, and keep raw values for QA. Load the prior Combo (if present) for cutoff calculations.

3. **Filter to new records**  
   Remove rows older than the last import date per source (from config or inferred from prior Combo) using the `Create Date` field when available. Drop obvious blanks or rows without key identifiers (e.g., missing email and phone).

4. **Transform**  
   - Apply mapping tables to service branches and professions.  
   - Normalize phone numbers and zip codes.  
   - Set defaults (invited flag, location radius, trade, union code, pay scale).  
   - Compute availability dates (offset from run date) and end dates (offset in years).  
   - Reshape columns into the Combo schema and IQX-required order.

5. **De-duplicate**  
   Identify duplicate candidates using keys like email, phone, and name+zip. Keep the record from the highest-priority source per `source_priority.yml`, and retain the dropped rows for reporting.

6. **Export**  
   Write the unified Combo Excel, Combo Dups Removed Excel, bulk import CSV, and IQX-ready CSV to `paths.output_root` using `combo_files` patterns.

7. **QA report**  
   Emit counts (by source, before/after dedup), duplicate summaries, missing mapping keys, invalid phone/zip values, and paths to produced files.

## Outputs
- Combined and cleaned Excel files ready for review.
- CSV formatted for IQX bulk import with correct column order and date formatting.
- QA summary to validate completeness before uploading to IQX.
