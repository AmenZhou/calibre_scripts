#!/bin/bash
# Stop auto-monitor
# Usage: ./stop.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Change to parent directory to match start.sh
cd "$SCRIPT_DIR/.."

if [ -f auto_monitor/monitor.pid ]; then
    PID=$(cat auto_monitor/monitor.pid)
    if ps -p $PID > /dev/null 2>&1; then
        echo "Stopping auto-monitor (PID: $PID)..."
        kill $PID 2>/dev/null
        sleep 2
        
        # Force kill if still running
        if ps -p $PID > /dev/null 2>&1; then
            echo "Force killing..."
            kill -9 $PID 2>/dev/null
        fi
        
        rm auto_monitor/monitor.pid
        echo "✅ Auto-monitor stopped"
    else
        echo "Auto-monitor not running (stale PID file)"
        rm auto_monitor/monitor.pid
    fi
else
    echo "Auto-monitor not running (no PID file)"
fi

