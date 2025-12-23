#!/bin/bash
# Restart a specific migration worker without affecting others
# Usage: ./restart_worker.sh <worker_id> [parallel_uploads]
# Example: ./restart_worker.sh 2
# Example: ./restart_worker.sh 2 1

set -e

if [ $# -lt 1 ]; then
    echo "Usage: $0 <worker_id> [parallel_uploads]"
    echo "Example: $0 2"
    echo "Example: $0 2 1"
    exit 1
fi

WORKER_ID=$1
PARALLEL_UPLOADS=${2:-1}  # Default to 1 if not specified (reduced from 2 to lower disk I/O), but allow override
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration
# Workers run on host system (not in container) for better memory isolation
CALIBRE_DIR="/media/haimengzhou/78613a5d-17be-413e-8691-908154970815/calibre library"
CONTAINER="mybookshelf2_app"
USERNAME="admin"
PASSWORD="mypassword123"
BATCH_SIZE=1000
USE_SYMLINKS="--use-symlinks"
# PARALLEL_UPLOADS is now set from command line argument (default: 1, reduced to lower disk I/O)
MEMORY_LIMIT_MB=500  # Memory limit per worker in MB (increased to 500MB to allow multiple workers to run simultaneously without conflicts)

echo "=== Restarting Worker $WORKER_ID ==="
echo ""

# Stop the specific worker (running on host)
# FIX: Use exact pattern match to avoid killing wrong workers (e.g., worker 1 killing worker 10, 12, etc.)
# Pattern must match "--worker-id N" where N is exactly the worker ID (not part of a larger number)
echo "Stopping worker $WORKER_ID..."
# Use grep with exact pattern match, then kill by PID to avoid pattern conflicts
# Pattern: --worker-id followed by whitespace, then exact worker ID, then whitespace or end of line
ps aux | grep "bulk_migrate_calibre" | grep -E -- "--worker-id[[:space:]]+${WORKER_ID}([[:space:]]|$)" | grep -v grep | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true
sleep 1

# Verify it's stopped
RUNNING=$(ps aux | grep "bulk_migrate_calibre" | grep -E -- "--worker-id[[:space:]]+${WORKER_ID}([[:space:]]|$)" | grep -v grep | wc -l)
if [ "$RUNNING" -gt 0 ]; then
    echo "⚠️  Warning: Worker $WORKER_ID still running, force killing..."
    ps aux | grep "bulk_migrate_calibre" | grep -E -- "--worker-id[[:space:]]+${WORKER_ID}([[:space:]]|$)" | grep -v grep | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true
    sleep 1
fi

echo "✓ Worker $WORKER_ID stopped"
echo ""

# Get last processed book ID from progress file (on host filesystem)
PROGRESS_FILE="migration_progress_worker${WORKER_ID}.json"
OFFSET=0

if [ -f "$PROGRESS_FILE" ]; then
    # Try to extract last_processed_book_id from progress file
    OFFSET=$(python3 -c "
import json
import sys
try:
    with open('$PROGRESS_FILE', 'r') as f:
        content = f.read()
        # Handle multiple JSON objects (find last one)
        if content.strip().count('{') > 1:
            last_brace = content.rfind('}')
            if last_brace > 0:
                brace_count = 0
                start_pos = last_brace
                for i in range(last_brace, -1, -1):
                    if content[i] == '}':
                        brace_count += 1
                    elif content[i] == '{':
                        brace_count -= 1
                        if brace_count == 0:
                            start_pos = i
                            break
                content = content[start_pos:last_brace+1]
        progress = json.loads(content)
        offset = progress.get('last_processed_book_id', 0)
        print(offset)
except Exception as e:
    print(0)
" 2>/dev/null || echo "0")
fi

if [ "$OFFSET" -gt 0 ]; then
    echo "Found last_processed_book_id: $OFFSET"
else
    echo "No last_processed_book_id found, using offset 0"
fi

echo ""
echo "=== Starting Worker $WORKER_ID (on host system) ==="
echo "Offset: $OFFSET"
echo "Batch size: $BATCH_SIZE"
echo "Parallel uploads: $PARALLEL_UPLOADS"
echo "Calibre dir: $CALIBRE_DIR"
echo ""

# Start the worker on host with memory limit to prevent OOM kills
# Memory limit is configurable via MEMORY_LIMIT_MB (default: 300MB)
# This prevents workers from being killed by OOM killer
# 300MB is sufficient for most workers (actual usage ~180-200MB)
# Increase if workers are hitting the limit, decrease if system memory is constrained
MEMORY_LIMIT_KB=$((MEMORY_LIMIT_MB * 1024))  # Convert MB to KB

# Use ulimit to set memory limit, or use systemd-run if available
if command -v systemd-run >/dev/null 2>&1; then
    # Use systemd-run with memory limit (more reliable)
    systemd-run --user --scope -p MemoryLimit=${MEMORY_LIMIT_KB}K \
        python3 bulk_migrate_calibre.py "$CALIBRE_DIR" \
        mybookshelf2_app \
        "$USERNAME" \
        "$PASSWORD" \
        --worker-id "$WORKER_ID" \
        --offset "$OFFSET" \
        --batch-size "$BATCH_SIZE" \
        --limit "$BATCH_SIZE" \
        $USE_SYMLINKS \
        --parallel-uploads "$PARALLEL_UPLOADS" \
        > "migration_worker${WORKER_ID}.log" 2>&1 &
else
    # Fallback to ulimit (less reliable but works on most systems)
    (ulimit -v $MEMORY_LIMIT_KB && \
     nohup python3 bulk_migrate_calibre.py "$CALIBRE_DIR" \
        mybookshelf2_app \
        "$USERNAME" \
        "$PASSWORD" \
        --worker-id "$WORKER_ID" \
        --offset "$OFFSET" \
        --batch-size "$BATCH_SIZE" \
        --limit "$BATCH_SIZE" \
        $USE_SYMLINKS \
        --parallel-uploads "$PARALLEL_UPLOADS" \
        > "migration_worker${WORKER_ID}.log" 2>&1) &
fi

sleep 2

# Verify worker started (check on host)
RUNNING=$(ps aux | grep "bulk_migrate_calibre.*worker.*${WORKER_ID}" | grep -v grep | wc -l)
if [ "$RUNNING" -gt 0 ]; then
    echo "✓ Worker $WORKER_ID started successfully on host"
    echo ""
    echo "Monitor with:"
    echo "  tail -f migration_worker${WORKER_ID}.log"
    echo "  python3 monitor_migration.py"
else
    echo "⚠️  Warning: Worker $WORKER_ID not detected. Check migration_worker${WORKER_ID}.log for errors."
    tail -20 "migration_worker${WORKER_ID}.log" 2>/dev/null || true
fi

