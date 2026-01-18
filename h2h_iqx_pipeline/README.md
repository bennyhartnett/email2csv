# H2H → IQX Bulk Import Pipeline (Prototype)

This repository contains a Python prototype for automating the offline portion of the Helmets-to-Hardhats (H2H) → IQX talent import workflow.

## Scope of this prototype

- Starts from Excel files already downloaded to local folders.
- Applies deterministic rules to:
  - Discover source files for a given month.
  - Extract only new records since the last import.
  - Standardize and clean data (service branch, profession, phone, zip, etc.).
  - De-duplicate records across sources according to configured priorities.
  - Produce:
    - A combined "Combo" file.
    - A "duplicates removed" file.
    - A "bulk import format" file.
    - A final CSV ready for bulk import into IQX.
  - Generate a QA report (counts, duplicates, anomalies).

**Out of scope**

- No email integration.
- No IQX UI / browser automation.
- No LLM usage.

---

## Quick start

1. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate          # Linux / macOS
   # or:
   .venv\Scripts\activate             # Windows
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Copy and adapt the example configuration:

   ```bash
   cp config/example_config.yml config/local_config.yml
   ```

4. Edit `config/local_config.yml` to point at your local folders (e.g., Vet Talents 2025-12).

5. Run the pipeline for a given month:

   ```bash
   python -m h2h_pipeline.cli run \
     --month 2025-12 \
     --input-root "/path/to/VetTalents" \
     --config "config/local_config.yml"
   ```

6. Inspect outputs:
   - Combined and cleaned Excel files under your configured `output_root`.
   - Bulk import CSV ready for upload into IQX.
   - QA report with counts and anomalies.

---

## GUI (Windows)

For non-technical users, run the Tkinter app with folder pickers:

```bash
python -m h2h_pipeline.gui_app
```

The GUI lets you select the config file, input root folder, output folder, and month.
If the output folder is left blank, it defaults to `<input_root>/output`.
Logs are written to `<output_root>/logs/pipeline.log`.

---

## Build a one-click Windows app (PyInstaller)

From `h2h_iqx_pipeline/`, run:

```powershell
.\scripts\build_windows_exe.ps1
```

The executable will be in `dist/H2H IQX Pipeline/`.

---

## Repository layout

- `config/` – Configuration and mapping files (YAML).
- `src/h2h_pipeline/` – Python package with pipeline code.
- `docs/` – Architecture and process documentation.
- `scripts/` – Helper shell scripts for running and maintaining the project.
- `tests/` – Unit tests and sample fixtures.

See `docs/architecture.md` and `docs/process_overview.md` for a deeper description of design and processing steps.
