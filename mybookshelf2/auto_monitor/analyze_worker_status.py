#!/usr/bin/env python3
"""
Analyze worker status from monitor logs for the past 24 hours
"""
import sys
import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from config import LOG_FILE, HISTORY_FILE, WORKER_LOG_DIR
except ImportError:
    # Fallback paths
    BASE_DIR = Path(__file__).parent.parent
    LOG_FILE = BASE_DIR / "auto_monitor" / "auto_restart.log"
    HISTORY_FILE = BASE_DIR / "auto_monitor" / "auto_fix_history.json"
    WORKER_LOG_DIR = BASE_DIR

def parse_log_timestamp(line: str) -> datetime:
    """Parse timestamp from log line"""
    # Format: 2025-12-10 22:49:15,101
    match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
    return None

def analyze_auto_restart_log(hours: int = 24) -> Dict[str, Any]:
    """Analyze auto_restart.log for the past N hours"""
    cutoff_time = datetime.now() - timedelta(hours=hours)
    
    if not LOG_FILE.exists():
        return {"error": f"Log file not found: {LOG_FILE}"}
    
    worker_events = defaultdict(list)
    stuck_events = []
    fix_events = []
    restart_events = []
    scale_events = []
    
    try:
        with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                timestamp = parse_log_timestamp(line)
                if not timestamp or timestamp < cutoff_time:
                    continue
                
                # Worker stuck detection
                if "detected as stuck" in line:
                    match = re.search(r'Worker (\d+) detected as stuck: (\d+) minutes', line)
                    if match:
                        worker_id = int(match.group(1))
                        minutes_stuck = int(match.group(2))
                        stuck_events.append({
                            "timestamp": timestamp.isoformat(),
                            "worker_id": worker_id,
                            "minutes_stuck": minutes_stuck
                        })
                        worker_events[worker_id].append({
                            "timestamp": timestamp.isoformat(),
                            "event": "stuck",
                            "minutes_stuck": minutes_stuck
                        })
                
                # Fix applied
                if "fixed:" in line or "fix verified successful" in line:
                    match = re.search(r'Worker (\d+)', line)
                    if match:
                        worker_id = int(match.group(1))
                        fix_events.append({
                            "timestamp": timestamp.isoformat(),
                            "worker_id": worker_id,
                            "success": "verified successful" in line
                        })
                        worker_events[worker_id].append({
                            "timestamp": timestamp.isoformat(),
                            "event": "fix_applied",
                            "success": "verified successful" in line
                        })
                
                # Restart events
                if "Applying" in line and "Restart" in line:
                    match = re.search(r'Worker (\d+)', line)
                    if match:
                        worker_id = int(match.group(1))
                        restart_events.append({
                            "timestamp": timestamp.isoformat(),
                            "worker_id": worker_id
                        })
                
                # Scaling events
                if "Scaling" in line:
                    if "DOWN" in line:
                        match = re.search(r'reducing workers from (\d+) to (\d+)', line)
                        if match:
                            scale_events.append({
                                "timestamp": timestamp.isoformat(),
                                "type": "down",
                                "from": int(match.group(1)),
                                "to": int(match.group(2))
                            })
                    elif "UP" in line:
                        match = re.search(r'increasing desired workers to (\d+)', line)
                        if match:
                            scale_events.append({
                                "timestamp": timestamp.isoformat(),
                                "type": "up",
                                "to": int(match.group(1))
                            })
                
                # Auto-restart events
                if "Auto-restarting stopped worker" in line:
                    match = re.search(r'worker (\d+)', line)
                    if match:
                        worker_id = int(match.group(1))
                        worker_events[worker_id].append({
                            "timestamp": timestamp.isoformat(),
                            "event": "auto_restart"
                        })
                
                # Worker stopped (but not restarted due to count limits)
                if "stopped, but current count" in line:
                    match = re.search(r'Worker (\d+) stopped', line)
                    if match:
                        worker_id = int(match.group(1))
                        worker_events[worker_id].append({
                            "timestamp": timestamp.isoformat(),
                            "event": "stopped_not_restarted"
                        })
    
    except Exception as e:
        return {"error": f"Error reading log file: {e}"}
    
    return {
        "worker_events": dict(worker_events),
        "stuck_events": stuck_events,
        "fix_events": fix_events,
        "restart_events": restart_events,
        "scale_events": scale_events,
        "total_stuck_events": len(stuck_events),
        "total_fix_events": len(fix_events),
        "total_restart_events": len(restart_events)
    }

def analyze_fix_history(hours: int = 24) -> Dict[str, Any]:
    """Analyze auto_fix_history.json for the past N hours"""
    cutoff_time = datetime.now() - timedelta(hours=hours)
    
    if not HISTORY_FILE.exists():
        return {"error": f"History file not found: {HISTORY_FILE}"}
    
    try:
        with open(HISTORY_FILE, 'r') as f:
            history = json.load(f)
    except Exception as e:
        return {"error": f"Error reading history file: {e}"}
    
    recent_fixes = []
    worker_fix_counts = defaultdict(int)
    fix_type_counts = defaultdict(int)
    
    for entry in history:
        try:
            timestamp_str = entry.get("timestamp", "")
            if timestamp_str:
                # Parse ISO format timestamp
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00').split('+')[0])
                except ValueError:
                    # Try alternative format
                    timestamp = datetime.strptime(timestamp_str.split('.')[0], '%Y-%m-%dT%H:%M:%S')
                
                if timestamp >= cutoff_time:
                    recent_fixes.append(entry)
                    worker_id = entry.get("worker_id")
                    if worker_id:
                        worker_fix_counts[worker_id] += 1
                    fix_type = entry.get("fix_type", "unknown")
                    fix_type_counts[fix_type] += 1
        except (ValueError, KeyError) as e:
            continue
    
    return {
        "recent_fixes": recent_fixes,
        "worker_fix_counts": dict(worker_fix_counts),
        "fix_type_counts": dict(fix_type_counts),
        "total_fixes": len(recent_fixes)
    }

def get_worker_log_activity(worker_id: int, hours: int = 24) -> Dict[str, Any]:
    """Get activity from worker log file"""
    log_file = WORKER_LOG_DIR / f"migration_worker{worker_id}.log"
    if not log_file.exists():
        return {"exists": False}
    
    cutoff_time = datetime.now() - timedelta(hours=hours)
    
    try:
        # Read last 1000 lines
        with open(log_file, 'rb') as f:
            try:
                f.seek(-50000, 2)  # Last 50KB
            except OSError:
                f.seek(0)
            content = f.read().decode('utf-8', errors='ignore')
            lines = content.split('\n')
        
        recent_lines = []
        upload_count = 0
        error_count = 0
        last_activity = None
        
        for line in lines[-500:]:  # Check last 500 lines
            timestamp = parse_log_timestamp(line)
            if timestamp and timestamp >= cutoff_time:
                recent_lines.append(line)
                if "Successfully uploaded" in line or "Uploading:" in line:
                    upload_count += 1
                if "ERROR" in line:
                    error_count += 1
                if timestamp:
                    if last_activity is None or timestamp > last_activity:
                        last_activity = timestamp
        
        # Get last line for status
        last_line = lines[-1] if lines else ""
        status = "unknown"
        if "Migration complete" in last_line:
            status = "completed"
        elif "Uploading:" in last_line or "Successfully uploaded" in last_line:
            status = "uploading"
        elif "Processed batch" in last_line or "Querying Calibre database" in last_line:
            status = "discovering"
        elif "ERROR" in last_line:
            status = "error"
        elif last_line.strip():
            status = "running"
        
        return {
            "exists": True,
            "status": status,
            "upload_count": upload_count,
            "error_count": error_count,
            "last_activity": last_activity.isoformat() if last_activity else None,
            "recent_lines_count": len(recent_lines),
            "last_line": last_line[:200] if last_line else ""
        }
    except Exception as e:
        return {"exists": True, "error": str(e)}

def get_running_workers() -> List[int]:
    """Get list of currently running worker IDs"""
    import subprocess
    try:
        result = subprocess.run(
            ['pgrep', '-af', '(bulk_migrate_calibre|upload_tar_files).*--worker-id'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            worker_ids = []
            for line in result.stdout.split('\n'):
                match = re.search(r'--worker-id\s+(\d+)', line)
                if match:
                    worker_ids.append(int(match.group(1)))
            return sorted(worker_ids)
    except Exception:
        pass
    return []

def main():
    hours = 24
    print("=" * 80)
    print(f"Worker Status Report - Past {hours} Hours")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    # Get currently running workers
    running_workers = get_running_workers()
    print(f"Currently Running Workers: {running_workers if running_workers else 'None'}")
    print()
    
    # Analyze auto_restart.log
    print("Analyzing auto_restart.log...")
    log_analysis = analyze_auto_restart_log(hours)
    if "error" in log_analysis:
        print(f"  Error: {log_analysis['error']}")
    else:
        print(f"  Stuck Events: {log_analysis['total_stuck_events']}")
        print(f"  Fix Events: {log_analysis['total_fix_events']}")
        print(f"  Restart Events: {log_analysis['total_restart_events']}")
        print()
        
        # Show stuck events by worker
        if log_analysis['stuck_events']:
            print("  Stuck Events by Worker:")
            stuck_by_worker = defaultdict(list)
            for event in log_analysis['stuck_events']:
                stuck_by_worker[event['worker_id']].append(event)
            for worker_id in sorted(stuck_by_worker.keys()):
                events = stuck_by_worker[worker_id]
                print(f"    Worker {worker_id}: {len(events)} stuck event(s)")
                for event in events[-3:]:  # Show last 3
                    print(f"      - {event['timestamp']}: {event['minutes_stuck']} minutes stuck")
        print()
    
    # Analyze fix history
    print("Analyzing fix history...")
    history_analysis = analyze_fix_history(hours)
    if "error" in history_analysis:
        print(f"  Error: {history_analysis['error']}")
    else:
        print(f"  Total Fixes: {history_analysis['total_fixes']}")
        if history_analysis['worker_fix_counts']:
            print("  Fixes by Worker:")
            for worker_id in sorted(history_analysis['worker_fix_counts'].keys()):
                count = history_analysis['worker_fix_counts'][worker_id]
                print(f"    Worker {worker_id}: {count} fix(es)")
        if history_analysis['fix_type_counts']:
            print("  Fixes by Type:")
            for fix_type in sorted(history_analysis['fix_type_counts'].keys()):
                count = history_analysis['fix_type_counts'][fix_type]
                print(f"    {fix_type}: {count}")
    print()
    
    # Analyze individual worker logs
    print("Analyzing individual worker logs...")
    all_worker_ids = set(running_workers)
    # Add workers from events
    if "worker_events" in log_analysis:
        all_worker_ids.update(log_analysis['worker_events'].keys())
    if "worker_fix_counts" in history_analysis:
        all_worker_ids.update(history_analysis['worker_fix_counts'].keys())
    
    worker_statuses = {}
    for worker_id in sorted(all_worker_ids):
        print(f"  Worker {worker_id}:")
        activity = get_worker_log_activity(worker_id, hours)
        worker_statuses[worker_id] = activity
        
        if not activity.get("exists"):
            print(f"    Status: Log file not found")
        elif "error" in activity:
            print(f"    Error: {activity['error']}")
        else:
            status = activity.get("status", "unknown")
            is_running = worker_id in running_workers
            status_display = f"{status} {'(RUNNING)' if is_running else '(STOPPED)'}"
            print(f"    Status: {status_display}")
            print(f"    Uploads (last {hours}h): {activity.get('upload_count', 0)}")
            print(f"    Errors (last {hours}h): {activity.get('error_count', 0)}")
            if activity.get('last_activity'):
                print(f"    Last Activity: {activity['last_activity']}")
            if activity.get('last_line'):
                last_line_preview = activity['last_line'][:100]
                print(f"    Last Log: {last_line_preview}...")
        print()
    
    # Summary
    print("=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"Total Workers Analyzed: {len(all_worker_ids)}")
    print(f"Currently Running: {len(running_workers)}")
    print(f"Total Stuck Events: {log_analysis.get('total_stuck_events', 0)}")
    print(f"Total Fixes Applied: {history_analysis.get('total_fixes', 0)}")
    
    # Workers with issues
    workers_with_issues = []
    for worker_id, events in log_analysis.get('worker_events', {}).items():
        stuck_count = sum(1 for e in events if e.get('event') == 'stuck')
        if stuck_count > 0:
            workers_with_issues.append((worker_id, stuck_count))
    
    if workers_with_issues:
        print("\nWorkers with Stuck Events:")
        for worker_id, count in sorted(workers_with_issues, key=lambda x: x[1], reverse=True):
            print(f"  Worker {worker_id}: {count} stuck event(s)")
    
    print()

if __name__ == "__main__":
    main()

