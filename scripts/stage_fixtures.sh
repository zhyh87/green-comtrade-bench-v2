#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-_purple_output}"
SERVICE="${SERVICE:-green-agent}"

# Ensure green-agent is running before staging fixtures
echo "Checking docker compose status..."
docker compose ps

if ! docker compose ps --status=running | grep -q "$SERVICE"; then
  echo "Service $SERVICE not running, starting..."
  docker compose up -d "$SERVICE"
  sleep 2
fi

# Re-check after start attempt
if ! docker compose ps --status=running | grep -q "$SERVICE"; then
  echo "ERROR: $SERVICE still not running after start attempt" >&2
  echo "--- docker compose ps ---" >&2
  docker compose ps >&2
  echo "--- docker compose logs $SERVICE ---" >&2
  docker compose logs --no-color --tail=200 "$SERVICE" >&2
  exit 1
fi

echo "Service $SERVICE is running"

if [ ! -d "$ROOT" ]; then
  echo "missing fixtures dir: $ROOT" >&2
  exit 1
fi

for dir in "$ROOT"/*; do
  [ -d "$dir" ] || continue
  task_id="$(basename "$dir")"
  docker compose exec -T "$SERVICE" sh -c "mkdir -p /workspace/purple_output/$task_id"
  for file in data.jsonl metadata.json run.log manifest.json; do
    if [ -f "$dir/$file" ]; then
      docker compose exec -T "$SERVICE" sh -c "cat > /workspace/purple_output/$task_id/$file" < "$dir/$file"
    fi
  done
done
