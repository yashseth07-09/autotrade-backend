#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/autotrade"

cd "$APP_DIR"

git fetch --all --prune
git reset --hard origin/master

export GIT_COMMIT="$(git rev-parse --short HEAD)"
export BUILD_TIME="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

docker compose up -d --build
docker image prune -f

curl --fail --silent --show-error http://localhost:8000/health >/dev/null
curl --fail --silent --show-error http://localhost:8000/version >/dev/null

echo "Deploy complete: commit=${GIT_COMMIT} build_time=${BUILD_TIME}"

