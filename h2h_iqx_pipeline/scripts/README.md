# Scripts

Helper shell scripts for development and execution.

- `run_pipeline.sh`  
  Example wrapper to run the pipeline with a specific config and month.

- `lint.sh`  
  Runs static analysis (flake8, ruff, mypy, etc.) if configured.

- `format.sh`  
  Runs code formatters (e.g., black, isort) if configured.

These scripts are optional convenience tools. You can call the Python entry points directly instead.
