#!/usr/bin/env bash
set -euo pipefail

echo "Stopping MESS (preserving data)..."
docker-compose stop
echo "Done."
