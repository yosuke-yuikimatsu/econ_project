#!/usr/bin/env bash
set -euo pipefail

mkdir -p /app/data/raw_html /app/data/parsed /app/data/logs
exec "$@"
