#!/bin/bash
# Start auto-monitor in background
# Usage: ./start.sh [--llm-enabled] [--dry-run]
# 
# To enable/disable LLM: Add or remove --llm-enabled flag
# Examples:
#   ./start.sh              # LLM disabled
#   ./start.sh --llm-enabled # LLM enabled

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Change to parent directory so log files can be found
cd "$SCRIPT_DIR/.."

# Check if already running
if [ -f auto_monitor/monitor.pid ]; then
    PID=$(cat auto_monitor/monitor.pid)
    if ps -p $PID > /dev/null 2>&1; then
        echo "Auto-monitor is already running (PID: $PID)"
        echo "Stop it first with: ./auto_monitor/stop.sh"
        exit 1
    else
        # Stale PID file
        rm auto_monitor/monitor.pid
    fi
fi

# Load .env file if it exists (for OPENAI_API_KEY)
if [ -f auto_monitor/.env ]; then
    export $(cat auto_monitor/.env | grep -v '^#' | xargs)
    echo "Loaded environment variables from .env file"
fi

# Start monitor (run from parent directory so log paths work)
# Try to use venv Python if available, otherwise use system Python
if [ -f venv/bin/python3 ]; then
    PYTHON_CMD="venv/bin/python3"
    echo "Using virtual environment Python"
else
    PYTHON_CMD="python3"
    echo "Using system Python"
fi

echo "Starting auto-monitor..."
nohup $PYTHON_CMD auto_monitor/monitor.py "$@" > auto_monitor/monitor.log 2>&1 &
echo $! > auto_monitor/monitor.pid

sleep 2

# Verify it started
if ps -p $(cat auto_monitor/monitor.pid) > /dev/null 2>&1; then
    echo "✅ Auto-monitor started successfully (PID: $(cat auto_monitor/monitor.pid))"
    echo ""
    echo "Monitor with:"
    echo "  tail -f auto_monitor/auto_restart.log"
    echo "  tail -f auto_monitor/monitor.log"
    echo ""
    echo "Stop with:"
    echo "  ./auto_monitor/stop.sh"
else
    echo "⚠️  Warning: Auto-monitor may not have started. Check auto_monitor/monitor.log for errors."
    tail -20 auto_monitor/monitor.log 2>/dev/null || true
    rm -f auto_monitor/monitor.pid
fi

