#!/usr/bin/env bash
set -euo pipefail

echo "Starting MESS (preserving data)..."
docker-compose up -d
echo "MESS is starting. Check status with: docker-compose ps"
