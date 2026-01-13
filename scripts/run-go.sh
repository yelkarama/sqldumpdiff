#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR/go"

if [[ $# -lt 2 ]]; then
  OLD="$ROOT_DIR/old.sql"
  NEW="$ROOT_DIR/new.sql"
  OUT="${1:-}"
else
  OLD="$1"
  NEW="$2"
  OUT="${3:-}"
fi

go build -o sqldumpdiff ./cmd/sqldumpdiff

if [[ -n "$OUT" ]]; then
  ./sqldumpdiff "$OLD" "$NEW" "$OUT"
else
  ./sqldumpdiff "$OLD" "$NEW"
fi
