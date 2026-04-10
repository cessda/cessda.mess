#!/usr/bin/env bash
set -euo pipefail

echo "WARNING: This will destroy all data (PostgreSQL + SPARQL store volumes)."
read -r -p "Are you sure? [y/N] " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

echo "Stopping and removing all volumes..."
docker-compose down -v
echo "Starting fresh..."
docker-compose up -d
echo "MESS is starting fresh. Check status with: docker-compose ps"
