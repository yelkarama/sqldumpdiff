#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR/python"

if [[ $# -lt 2 ]]; then
  OLD="$ROOT_DIR/old.sql"
  NEW="$ROOT_DIR/new.sql"
  OUT="${1:-}"
else
  OLD="$1"
  NEW="$2"
  OUT="${3:-}"
fi

if [[ -n "$OUT" ]]; then
  python3 sqldumpdiff.py "$OLD" "$NEW" "$OUT"
else
  python3 sqldumpdiff.py "$OLD" "$NEW"
fi
