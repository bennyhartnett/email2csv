# Mapping files

This folder contains mapping tables used during transformation and de-duplication.

Each file is a YAML dictionary. Keys are raw values from the source Excel files. Values are the standardized forms used in the pipeline.

## Files

- `professions.yml`

  Example:

  ```yaml
  "Electricians/Lineman": "Electricians"
  "Ironworkers": "Structural Iron and Steel Workers"
  ```

- `service_branches.yml`

  Example:

  ```yaml
  "Air": "Service: Air"
  "Army": "Service: Army"
  "Coast": "Service: Coast"
  "Marine": "Service: Marine"
  "Navy": "Service: Navy"
  ```

- `source_priority.yml`

  Example:

  ```yaml
  # Higher number = higher priority when choosing which duplicate to keep
  "Ironworkers": 100
  "IBEW D8": 90
  "IBEW D4": 80
  "IBEW D9": 70
  "Other": 10
  ```
