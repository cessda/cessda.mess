#!/usr/bin/env bash
# Start the MESS core stack (PostgreSQL + API). Data volumes are preserved.
#
# QLever (SPARQL store) is managed separately — see scripts/rebuild_qlever.sh.
# You must build the QLever index at least once before SPARQL queries work:
#   bash scripts/rebuild_qlever.sh
set -euo pipefail

echo "Starting MESS core stack (preserving data)..."
docker compose up -d
echo "Done. Check status with: docker compose ps"
echo ""
echo "Note: QLever (SPARQL store) is managed separately."
echo "      If not yet running, build the index with: bash scripts/rebuild_qlever.sh"
