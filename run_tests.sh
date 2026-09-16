#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
API_WHEEL=${CYCLECLOUD_API:-}
if [[ -z "$API_WHEEL" ]]; then
    for candidate in "$PROJECT_ROOT"/cyclecloud_api-*.whl "$PROJECT_ROOT"/libs/cyclecloud_api-*.whl; do
        if [[ -f "$candidate" ]]; then
            API_WHEEL=$candidate
            break
        fi
    done
fi
if [[ ! -f "$API_WHEEL" ]]; then
    echo "Set CYCLECLOUD_API to a local CycleCloud API wheel, or place one in the project root or libs/." >&2
    exit 1
fi

"${PYTHON:-python3}" -m venv "$PROJECT_ROOT/.testenv"
TEST_PYTHON="$PROJECT_ROOT/.testenv/bin/python"
"$TEST_PYTHON" -m pip install --upgrade pip 'setuptools<72' wheel
"$TEST_PYTHON" -m pip install --no-build-isolation \
    "$API_WHEEL" -e "$PROJECT_ROOT" 'pytest==6.2.5' 'hypothesis==6.31.0' 'typeguard<3'

cd "$PROJECT_ROOT"
export HPC_RUNTIME_CHECKS=${HPC_RUNTIME_CHECKS:-true}
exec "$TEST_PYTHON" -m pytest -s test --junitxml=build/test-results/pytest.xml -k 'not hypothesis' "$@"