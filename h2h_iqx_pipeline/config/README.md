# Config directory

This folder contains configuration and mapping files that control how the pipeline behaves. The goal is to keep as much logic as possible in data / config rather than hard-coding it in Python.

## Files

- `example_config.yml`  
  Example top-level configuration. Copy this to a new file (e.g., `local_config.yml`) and adjust paths, month, and options for your environment.

- `mappings/`  
  - `professions.yml` – Maps raw profession strings in source files to canonical titles (e.g., O*NET-aligned names).
  - `service_branches.yml` – Maps raw branch strings ("Air", "Army", etc.) to the standardized text used in internal comments.
  - `source_priority.yml` – Defines the relative priority for each source (e.g., Ironworkers > IBEW 8 > IBEW 9) when de-duplicating.

You can extend or change mappings without modifying Python code. The pipeline should validate that all referenced mapping keys exist.
