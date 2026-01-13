#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  OLD="$ROOT_DIR/old.sql"
  NEW="$ROOT_DIR/new.sql"
else
  OLD="$1"
  NEW="$2"
fi

echo "== Go =="
time ./scripts/run-go.sh "$OLD" "$NEW" /tmp/delta_go.sql

echo "== Java =="
time ./scripts/run-java.sh "$OLD" "$NEW" /tmp/delta_java.sql

echo "== Python =="
time ./scripts/run-py.sh "$OLD" "$NEW" /tmp/delta_py.sql
