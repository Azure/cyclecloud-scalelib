#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
if [[ $# -eq 1 && "$1" == --help ]]; then
	echo "Usage: $0 [--cyclecloud-api FILE] (build release assets in Docker without publishing)"
	exit 0
fi
api_wheel=
if [[ $# -eq 2 && "$1" == --cyclecloud-api && -r "$2" && -f "$2" ]]; then
	api_wheel=$2
elif [[ $# -ne 0 ]]; then
	echo "Usage: $0 [--cyclecloud-api FILE]; FILE must be readable." >&2
	exit 1
fi
command -v docker >/dev/null || { echo "Docker is required." >&2; exit 1; }
docker info >/dev/null
mkdir -p "$PROJECT_ROOT/build"
staging=$(mktemp -d "$PROJECT_ROOT/build/docker-release.XXXXXX")
trap 'rm -rf -- "$staging"' EXIT
mkdir "$staging/output"
docker_args=()
if [[ -n "$api_wheel" ]]; then
	mkdir "$staging/input"
	cp -- "$api_wheel" "$staging/input/"
	docker_args+=(--mount "type=bind,source=$staging/input,target=/input,readonly"
		--env "CYCLECLOUD_API=/input/$(basename -- "$api_wheel")")
fi

echo "Building release assets in Docker; release creation and uploads are disabled."
tar -C "$PROJECT_ROOT" --exclude=.git --exclude=.buildenv --exclude=.testenv \
	--exclude=.venv --exclude=venv --exclude=__pycache__ --exclude='*.egg-info' \
	--exclude=build --exclude=dist --exclude=libs -cf - . |
	docker run --rm -i --platform linux/amd64 \
		--mount "type=bind,source=$staging/output,target=/output" \
		"${docker_args[@]}" \
		ubuntu:24.04 /bin/bash -e -o pipefail -c '
			mkdir /work
			tar -xf - -C /work
			cd /work
			export DEBIAN_FRONTEND=noninteractive
			apt-get update
			apt-get install -y python3 python3-venv python3-yaml curl ca-certificates dpkg
			python3 util/local_release.py
			chown "$1:$2" /output/*
		' -- "$(id -u)" "$(id -g)"

mkdir -p "$PROJECT_ROOT/dist"
mv -- "$staging/output/"* "$PROJECT_ROOT/dist/"
echo "Build complete: $PROJECT_ROOT/dist (nothing published)."