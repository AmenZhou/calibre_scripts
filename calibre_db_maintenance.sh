#!/usr/bin/env bash

# Calibre Database Maintenance Script
# Run this periodically to optimize database performance

CALIBRE_LIBRARY_PATH="$1"

if [ -z "$CALIBRE_LIBRARY_PATH" ]; then
    echo "Usage: $0 /path/to/calibre/library"
    echo "Example: $0 ~/Calibre\ Library"
    exit 1
fi

echo "=== Calibre Database Maintenance ==="
echo "Library: $CALIBRE_LIBRARY_PATH"

# Backup first
echo "[1/4] Creating database backup..."
cp "$CALIBRE_LIBRARY_PATH/metadata.db" "$CALIBRE_LIBRARY_PATH/metadata.db.backup.$(date +%Y%m%d_%H%M%S)"

# Optimize database using restore_database (rebuilds and optimizes)
echo "[2/4] Optimizing main database..."
calibredb --library-path="$CALIBRE_LIBRARY_PATH" restore_database

# Alternative: Direct SQLite vacuum (if restore_database doesn't work)
# echo "[2/4] Vacuuming database directly..."
# sqlite3 "$CALIBRE_LIBRARY_PATH/metadata.db" "VACUUM;"

# Clean up full-text search database
echo "[3/4] Rebuilding full-text search index..."
if [ -f "$CALIBRE_LIBRARY_PATH/full-text-search.db" ]; then
    rm "$CALIBRE_LIBRARY_PATH/full-text-search.db"
    echo "Deleted full-text search database - will be rebuilt automatically"
fi

# Check database integrity
echo "[4/4] Checking database integrity..."
calibredb --library-path="$CALIBRE_LIBRARY_PATH" check_library

echo "=== Database maintenance complete ==="
echo "Note: Restart Calibre to see performance improvements" 