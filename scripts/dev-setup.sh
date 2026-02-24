#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

uv sync --extra dev
uv run pre-commit install
uv run pre-commit install --hook-type pre-push
uv run pre-commit run --all-files
