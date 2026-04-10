#!/usr/bin/env bash
set -euo pipefail

echo "Restarting MESS..."
docker-compose restart
echo "Done."
