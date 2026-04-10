#!/usr/bin/env bash
# Restart the MESS core stack (PostgreSQL + API).
# Pass --qlever to also restart the QLever SPARQL store (does NOT rebuild the index).
set -euo pipefail

echo "Restarting MESS core stack..."
docker compose restart

if [[ "${1:-}" == "--qlever" ]]; then
    echo "Restarting QLever..."
    docker compose -f docker-compose.qlever.yml restart qlever || true
fi

echo "Done."
