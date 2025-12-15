#!/bin/bash
# Restart migration workers
# Usage: ./restart_workers.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CALIBRE_DIR="/media/haimengzhou/78613a5d-17be-413e-8691-908154970815/calibre library"
CONTAINER="mybookshelf2_app"
USERNAME="admin"
PASSWORD="mypassword123"
WORKERS=4
BATCH_SIZE=10000
USE_SYMLINKS="--use-symlinks"
PARALLEL_UPLOADS=3

echo "=== Stopping existing workers ==="
pkill -9 -f "bulk_migrate_calibre" 2>/dev/null || true
pkill -9 -f "parallel_migrate" 2>/dev/null || true
sleep 2

# Verify they're stopped
RUNNING=$(ps aux | grep -E "bulk_migrate_calibre|parallel_migrate" | grep -v grep | wc -l)
if [ "$RUNNING" -gt 0 ]; then
    echo "⚠️  Warning: Some processes still running, force killing..."
    ps aux | grep -E "bulk_migrate_calibre|parallel_migrate" | grep -v grep | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true
    sleep 1
fi

echo "✓ Workers stopped"
echo ""
echo "=== Starting workers ==="
echo "Workers: $WORKERS"
echo "Batch size: $BATCH_SIZE"
echo "Parallel uploads per worker: $PARALLEL_UPLOADS"
echo ""

nohup python3 parallel_migrate.py "$CALIBRE_DIR" \
    --workers $WORKERS \
    $USE_SYMLINKS \
    --batch-size $BATCH_SIZE \
    --container $CONTAINER \
    --username $USERNAME \
    --password $PASSWORD \
    --parallel-uploads $PARALLEL_UPLOADS \
    >> parallel_migration.log 2>&1 &

sleep 3

# Verify workers started
RUNNING=$(ps aux | grep "bulk_migrate_calibre" | grep -v grep | wc -l)
if [ "$RUNNING" -gt 0 ]; then
    echo "✓ Workers started successfully ($RUNNING processes)"
    echo ""
    echo "Monitor with:"
    echo "  tail -f migration_worker*.log"
    echo "  python3 monitor_migration.py"
else
    echo "⚠️  Warning: No workers detected. Check parallel_migration.log for errors."
    tail -20 parallel_migration.log 2>/dev/null || true
fi






