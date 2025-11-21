#!/usr/bin/env python3
"""
Monitor parallel migration progress
Aggregates progress from all worker progress files and displays real-time dashboard
"""

import json
import time
import os
import glob
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any

def load_progress_file(file_path: Path) -> Dict[str, Any]:
    """Load progress from a worker progress file"""
    if not file_path.exists():
        return {"completed_files": {}, "errors": []}
    
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return {"completed_files": {}, "errors": []}

def get_worker_progress() -> Dict[int, Dict[str, Any]]:
    """Get progress from all worker progress files"""
    progress_files = glob.glob("migration_progress_worker*.json")
    workers = {}
    
    for file_path in progress_files:
        # Extract worker ID from filename
        try:
            worker_id = int(file_path.split('_worker')[1].split('.')[0])
            workers[worker_id] = load_progress_file(Path(file_path))
        except (ValueError, IndexError):
            continue
    
    return workers

def get_worker_log_stats(worker_id: int) -> Dict[str, Any]:
    """Get statistics from worker log file"""
    log_file = Path(f"migration_worker{worker_id}.log")
    if not log_file.exists():
        return {"status": "not_started", "last_activity": None}
    
    try:
        # Read last few lines of log (read in binary mode to handle large files)
        with open(log_file, 'rb') as f:
            # Seek to end and read last 8KB
            try:
                f.seek(-8192, 2)  # Seek to 8KB before end
            except OSError:
                # File is smaller than 8KB, read from start
                f.seek(0)
            lines = f.readlines()
            if not lines:
                return {"status": "empty", "last_activity": None}
            
            # Decode last few lines
            decoded_lines = [line.decode('utf-8', errors='ignore').strip() for line in lines[-20:]]
            last_line = decoded_lines[-1] if decoded_lines else ""
            
            # Check for completion
            if "Migration complete" in last_line:
                # Extract success/error counts
                if "Success:" in last_line and "Errors:" in last_line:
                    try:
                        parts = last_line.split("Success:")[1].split(",")
                        success = int(parts[0].strip())
                        errors = int(parts[1].split("Errors:")[1].strip())
                        return {"status": "completed", "success": success, "errors": errors, "last_activity": last_line}
                    except:
                        pass
                return {"status": "completed", "last_activity": last_line}
            
            # Check for uploading/processing
            if "Uploading:" in last_line or "Successfully uploaded:" in last_line:
                return {"status": "uploading", "last_activity": last_line}
            
            # Check for database query
            if "Fetched" in last_line and "rows" in last_line:
                return {"status": "querying_db", "last_activity": last_line}
            
            # Check for progress
            if "Progress:" in last_line:
                return {"status": "running", "last_activity": last_line}
            
            # Check for errors
            if "ERROR" in last_line:
                return {"status": "error", "last_activity": last_line}
            
            return {"status": "initializing", "last_activity": last_line}
    except Exception as e:
        return {"status": f"error: {str(e)[:30]}", "last_activity": None}

def format_time(seconds: float) -> str:
    """Format seconds into human-readable time"""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"

def display_dashboard(workers: Dict[int, Dict[str, Any]], start_time: datetime):
    """Display migration progress dashboard"""
    os.system('clear' if os.name != 'nt' else 'cls')
    
    print("=" * 80)
    print("  MyBookshelf2 Migration Monitor")
    print("=" * 80)
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Current: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"Elapsed: {format_time(elapsed)}")
    print()
    
    # Aggregate statistics
    total_completed = 0
    total_errors = 0
    total_uploaded = 0
    total_already_exists = 0
    
    print("Worker Status:")
    print("-" * 80)
    
    for worker_id in sorted(workers.keys()):
        progress = workers[worker_id]
        log_stats = get_worker_log_stats(worker_id)
        
        completed = len(progress.get("completed_files", {}))
        errors = len(progress.get("errors", []))
        
        # Count by status
        uploaded = sum(1 for f in progress.get("completed_files", {}).values() 
                      if f.get("status") != "already_exists")
        already_exists = sum(1 for f in progress.get("completed_files", {}).values() 
                            if f.get("status") == "already_exists")
        
        total_completed += completed
        total_errors += errors
        total_uploaded += uploaded
        total_already_exists += already_exists
        
        status_icon = "✓" if log_stats.get("status") == "completed" else "▶" if log_stats.get("status") == "running" else "✗"
        
        print(f"Worker {worker_id}: {status_icon} {log_stats.get('status', 'unknown'):12} | "
              f"Completed: {completed:>7,} | Uploaded: {uploaded:>7,} | "
              f"Already exists: {already_exists:>5,} | Errors: {errors:>4,}")
        
        if log_stats.get("last_activity"):
            activity = log_stats["last_activity"][:60]
            print(f"  Last: {activity}...")
    
    print("-" * 80)
    print(f"TOTAL:  Completed: {total_completed:>7,} | Uploaded: {total_uploaded:>7,} | "
          f"Already exists: {total_already_exists:>5,} | Errors: {total_errors:>4,}")
    
    # Estimate remaining time (if we have progress)
    if total_completed > 0 and elapsed > 0:
        rate = total_completed / elapsed  # books per second
        if rate > 0:
            # Estimate total books (this is approximate)
            # We don't know exact total, so estimate based on progress
            estimated_total = total_completed * 1.1  # Assume 10% remaining
            remaining = estimated_total - total_completed
            if remaining > 0:
                eta_seconds = remaining / rate
                eta = datetime.now() + timedelta(seconds=eta_seconds)
                print(f"\nRate: {rate * 60:.1f} books/minute | "
                      f"ETA: {eta.strftime('%Y-%m-%d %H:%M:%S')} "
                      f"({format_time(eta_seconds)} remaining)")
    
    print()
    print("Press Ctrl+C to exit")
    print("=" * 80)

def main():
    print("Starting migration monitor...")
    print("Press Ctrl+C to exit\n")
    
    start_time = datetime.now()
    
    try:
        while True:
            # Get worker progress from JSON files
            workers = get_worker_progress()
            
            # Also check log files for workers that might not have progress files yet
            for worker_id in range(1, 10):  # Check workers 1-9
                if worker_id not in workers:
                    log_stats = get_worker_log_stats(worker_id)
                    if log_stats.get("status") != "not_started":
                        # Worker exists but no progress file yet
                        workers[worker_id] = {"completed_files": {}, "errors": []}
            
            if not workers:
                print("No worker progress files found. Waiting for workers to start...")
                time.sleep(5)
                continue
            
            display_dashboard(workers, start_time)
            time.sleep(5)  # Update every 5 seconds for more responsive monitoring
            
    except KeyboardInterrupt:
        print("\n\nMonitor stopped.")
    except Exception as e:
        print(f"\n\nError in monitor: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

