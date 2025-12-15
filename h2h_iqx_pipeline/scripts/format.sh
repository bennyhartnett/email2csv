#!/usr/bin/env bash
set -euo pipefail

echo "Running formatters (configure tools as needed)..."
if command -v black >/dev/null 2>&1; then
  black src tests
fi
if command -v isort >/dev/null 2>&1; then
  isort src tests
fi
