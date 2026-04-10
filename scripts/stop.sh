#!/usr/bin/env bash
# Stop the MESS core stack (PostgreSQL + API). Data volumes are preserved.
# Pass --qlever to also stop the QLever SPARQL store.
set -euo pipefail

echo "Stopping MESS core stack (preserving data)..."
docker compose stop

if [[ "${1:-}" == "--qlever" ]]; then
    echo "Stopping QLever..."
    docker compose -f docker-compose.qlever.yml stop qlever || true
fi

echo "Done."
