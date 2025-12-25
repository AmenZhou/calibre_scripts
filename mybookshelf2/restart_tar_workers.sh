#!/bin/bash
# Restart tar upload workers
# Usage: ./restart_tar_workers.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TAR_SOURCE_DIR="/media/haimengzhou/16TB985-CP18TBCD"
CONTAINER="mybookshelf2_app"
USERNAME="admin"
PASSWORD="mypassword123"
WORKERS=4
BATCH_SIZE=1000
PARALLEL_UPLOADS=1

echo "=== Stopping existing tar upload workers ==="
pkill -9 -f "upload_tar_files" 2>/dev/null || true
pkill -9 -f "parallel_upload_tars" 2>/dev/null || true
sleep 2

# Verify they're stopped
RUNNING=$(ps aux | grep -E "upload_tar_files|parallel_upload_tars" | grep -v grep | wc -l)
if [ "$RUNNING" -gt 0 ]; then
    echo "⚠️  Warning: Some processes still running, force killing..."
    ps aux | grep -E "upload_tar_files|parallel_upload_tars" | grep -v grep | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true
    sleep 1
fi

echo "✓ Workers stopped"
echo ""
echo "=== Starting workers ==="
echo "Tar source directory: $TAR_SOURCE_DIR"
echo "Workers: $WORKERS"
echo "Batch size: $BATCH_SIZE"
echo "Parallel uploads per worker: $PARALLEL_UPLOADS"
echo ""

nohup python3 parallel_upload_tars.py "$TAR_SOURCE_DIR" \
    --workers $WORKERS \
    --batch-size $BATCH_SIZE \
    --container $CONTAINER \
    --username $USERNAME \
    --password $PASSWORD \
    --parallel-uploads $PARALLEL_UPLOADS \
    >> parallel_tar_upload.log 2>&1 &

sleep 3

# Verify workers started
RUNNING=$(ps aux | grep "upload_tar_files" | grep -v grep | wc -l)
if [ "$RUNNING" -gt 0 ]; then
    echo "✓ Workers started successfully ($RUNNING processes)"
    echo ""
    echo "Monitor with:"
    echo "  tail -f migration_worker*.log"
    echo "  python3 monitor_migration.py"
    echo "  python3 auto_monitor/monitor.py"
else
    echo "⚠️  Warning: No workers detected. Check parallel_tar_upload.log for errors."
    tail -20 parallel_tar_upload.log 2>/dev/null || true
fi

