#!/usr/bin/env bash
set -euo pipefail

MONTH="${1:-2025-12}"
CONFIG_PATH="${2:-config/local_config.yml}"
INPUT_ROOT="${3:-/path/to/VetTalents}"

python -m h2h_pipeline.cli run \
  --month "${MONTH}" \
  --input-root "${INPUT_ROOT}" \
  --config "${CONFIG_PATH}"
