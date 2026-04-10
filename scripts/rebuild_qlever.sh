#!/usr/bin/env bash
# rebuild_qlever.sh — Export triples, rebuild QLever index, restart QLever server.
#
# Run this whenever you want to refresh the knowledge graph after new enrichments.
# Requires docker compose (v2) and the mess-api + postgres containers to be running.
#
# Usage: bash scripts/rebuild_qlever.sh
set -euo pipefail

echo "1/4  Stopping QLever server..."
docker compose -f docker-compose.qlever.yml stop qlever

echo "2/4  Exporting triples from PostgreSQL..."
docker compose run --rm mess-api python scripts/export_triples.py

echo "3/4  Building QLever index..."
docker compose -f docker-compose.qlever.yml run --rm indexer

echo "4/4  Starting QLever server..."
docker compose -f docker-compose.qlever.yml up -d qlever

echo "Done.  Query the graph at http://localhost:7001"
