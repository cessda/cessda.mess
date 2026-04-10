#!/usr/bin/env bash
# Wipe ALL data and start fresh: PostgreSQL, export volume, and QLever index.
# After this, run `bash scripts/rebuild_qlever.sh` to restore the knowledge graph.
set -euo pipefail

echo "WARNING: This will destroy all data (PostgreSQL, export volume, and QLever index)."
read -r -p "Are you sure? [y/N] " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

echo "Stopping and removing QLever..."
docker compose -f docker-compose.qlever.yml down -v || true

echo "Stopping and removing core stack volumes..."
docker compose down -v

echo "Starting fresh core stack..."
docker compose up -d
echo "Done. PostgreSQL and API are starting fresh."
echo ""
echo "Next step: once the API is up, rebuild the QLever index:"
echo "  bash scripts/rebuild_qlever.sh"
