#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR/java"

if [[ $# -lt 2 ]]; then
  OLD="$ROOT_DIR/old.sql"
  NEW="$ROOT_DIR/new.sql"
  OUT="${1:-}"
else
  OLD="$1"
  NEW="$2"
  OUT="${3:-}"
fi

mvn -q -DskipTests package

if [[ -n "$OUT" ]]; then
  java --enable-native-access=ALL-UNNAMED -Xmx4g -jar target/sqldumpdiff-1.0.0.jar "$OLD" "$NEW" "$OUT"
else
  java --enable-native-access=ALL-UNNAMED -Xmx4g -jar target/sqldumpdiff-1.0.0.jar "$OLD" "$NEW"
fi
