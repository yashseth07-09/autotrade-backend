#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/autotrade"
cd "$APP_DIR"

echo "==> Updating repo"
git fetch --all --prune
git checkout -f master
git reset --hard origin/master

export GIT_COMMIT="$(git rev-parse --short HEAD)"
export BUILD_TIME="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

echo "==> Build + restart containers"
docker compose up -d --build

echo "==> Cleanup old images (keep recent cache)"
docker image prune -f --filter "until=168h" || true

echo "==> Wait for API health"
for i in {1..30}; do
  if curl --fail --silent --show-error http://127.0.0.1:8000/health >/dev/null; then
    echo "Health OK"
    break
  fi
  echo "Waiting... ($i/30)"
  sleep 2
done

# Final assert (if still failing)
curl --fail --silent --show-error http://127.0.0.1:8000/health >/dev/null

# Optional: only check /version if implemented
if curl --fail --silent --show-error http://127.0.0.1:8000/version >/dev/null 2>&1; then
  echo "Version OK"
else
  echo "Note: /version not available (skipping)"
fi

echo "Deploy complete: commit=${GIT_COMMIT} build_time=${BUILD_TIME}"