#!/bin/bash
# Build script for creating a native executable

set -e

echo "Installing build dependencies..."
uv sync --extra dev

echo "Building executable..."
uv run pyinstaller --clean sqldumpdiff.spec

echo ""
echo "✅ Build complete! Executable is in: dist/sqldumpdiff"
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "   You can test it with: ./dist/sqldumpdiff <old_dump.sql> <new_dump.sql>"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "   You can test it with: ./dist/sqldumpdiff <old_dump.sql> <new_dump.sql>"
else
    echo "   You can test it with: dist\\sqldumpdiff.exe <old_dump.sql> <new_dump.sql>"
fi

