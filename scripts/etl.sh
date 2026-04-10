#!/usr/bin/env bash
set -euo pipefail

API_URL="${MESS_API_URL:-http://localhost:8000}"
ADMIN_KEY="${MESS_ADMIN_KEY:-}"

if [[ -z "$ADMIN_KEY" ]]; then
    echo "Error: MESS_ADMIN_KEY environment variable is required."
    exit 1
fi

echo "Triggering ETL pipeline..."
curl -s -f -X POST "${API_URL}/admin/etl" \
    -H "X-Admin-Key: ${ADMIN_KEY}" \
    | python3 -m json.tool

echo ""
echo "ETL pipeline started. Check logs with: docker compose logs -f mess-api"
