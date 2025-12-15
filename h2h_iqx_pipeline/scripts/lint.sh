#!/usr/bin/env bash
set -euo pipefail

echo "Running linters (configure tools as needed)..."
if command -v ruff >/dev/null 2>&1; then
  ruff check src tests
fi
if command -v flake8 >/dev/null 2>&1; then
  flake8 src tests
fi
if command -v mypy >/dev/null 2>&1; then
  mypy src
fi
