#!/usr/bin/env bash
# rebuild_qlever.sh — Export triples from PostgreSQL, rebuild QLever index, start QLever server.
#
# Run this:
#   - On first setup (before SPARQL queries will work)
#   - After clean.sh (to restore the knowledge graph)
#   - Whenever you want the SPARQL store to reflect the latest enriched data
#
# Requires: mess-api and postgres containers must be running (bash scripts/start.sh first).
# Usage: bash scripts/rebuild_qlever.sh
set -euo pipefail

echo "1/4  Stopping QLever server (if running)..."
docker compose -f docker-compose.qlever.yml stop qlever || true

echo "2/4  Exporting triples from PostgreSQL..."
docker compose run --rm mess-api python scripts/export_triples.py

echo "3/4  Building QLever index..."
docker compose -f docker-compose.qlever.yml run --rm indexer

echo "4/4  Starting QLever server..."
docker compose -f docker-compose.qlever.yml up -d qlever

echo "Done. Query the graph at http://localhost:7001"
