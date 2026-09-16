#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
"${PYTHON:-python3}" -m venv "$PROJECT_ROOT/.buildenv"
source "$PROJECT_ROOT/.buildenv/bin/activate"
python -m pip install --upgrade pip 'setuptools<72' wheel

cd "$PROJECT_ROOT"
exec python package.py "$@"