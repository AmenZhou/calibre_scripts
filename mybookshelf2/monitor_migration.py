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
            content = f.read().strip()
            # Handle files with multiple JSON objects (corrupted or appended)
            if content.count('{') > 1:
                # Try to parse the last JSON object
                last_brace = content.rfind('}')
                if last_brace > 0:
                    # Find the matching opening brace
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
                    try:
                        return json.loads(content[start_pos:last_brace+1])
                    except:
                        pass
            return json.loads(content)
    except json.JSONDecodeError as e:
        # Try to recover by reading last valid JSON object
        try:
            with open(file_path, 'rb') as f:
                f.seek(0, 2)  # Seek to end
                size = f.tell()
                # Read last 1MB and try to find last valid JSON
                read_size = min(1024 * 1024, size)
                f.seek(max(0, size - read_size))
                content = f.read().decode('utf-8', errors='ignore')
                # Find last complete JSON object
                last_brace = content.rfind('}')
                if last_brace > 0:
                    # Find matching opening brace
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
                    return json.loads(content[start_pos:last_brace+1])
        except:
            pass
        return {"completed_files": {}, "errors": []}
    except Exception as e:
        # Silently return empty progress on other errors
        return {"completed_files": {}, "errors": []}

def get_running_worker_ids() -> set:
    """Get IDs of workers that are actually running"""
    import subprocess
    running_workers = set()
    try:
        # Use pgrep to find processes more reliably
        result = subprocess.run(
            ['pgrep', '-af', 'bulk_migrate_calibre'],
            capture_output=True,
            text=True,
            timeout=5
        )
        for line in result.stdout.split('\n'):
            if '--worker-id' in line:
                # Extract worker-id value
                import re
                match = re.search(r'--worker-id\s+(\d+)', line)
                if match:
                    try:
                        worker_id = int(match.group(1))
                        running_workers.add(worker_id)
                    except ValueError:
                        pass
    except Exception:
        # Fallback to ps aux if pgrep fails
        try:
            result = subprocess.run(
                ['ps', 'aux'],
                capture_output=True,
                text=True,
                timeout=5
            )
            for line in result.stdout.split('\n'):
                if 'bulk_migrate_calibre' in line and '--worker-id' in line:
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part == '--worker-id' and i + 1 < len(parts):
                            try:
                                worker_id = int(parts[i + 1])
                                running_workers.add(worker_id)
                            except ValueError:
                                pass
        except Exception:
            pass
    return running_workers

def get_database_counts() -> Dict[str, int]:
    """Get actual counts from MyBookshelf2 database"""
    import subprocess
    try:
        script = """
import sys
sys.path.insert(0, '/code')
from app import app, db
from app import model

with app.app_context():
    total_ebooks = db.session.query(model.Ebook).count()
    total_sources = db.session.query(model.Source).count()
    print(f'{{"ebooks": {total_ebooks}, "sources": {total_sources}}}')
"""
        result = subprocess.run(
            ['docker', 'exec', 'mybookshelf2_app', 'python3', '-c', script],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            import json
            return json.loads(result.stdout.strip())
    except Exception:
        pass
    return {"ebooks": 0, "sources": 0}

def get_ebooks_with_sources_count() -> int:
    """Get count of ebooks that actually have source files (working books)"""
    import subprocess
    try:
        script = """
import sys
sys.path.insert(0, '/code')
from app import app, db
from app import model

with app.app_context():
    ebooks_with_sources = db.session.query(model.Ebook).join(model.Source).distinct().count()
    print(ebooks_with_sources)
"""
        result = subprocess.run(
            ['docker', 'exec', 'mybookshelf2_app', 'python3', '-c', script],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            return int(result.stdout.strip())
    except Exception:
        pass
    return 0

def get_worker_progress() -> Dict[int, Dict[str, Any]]:
    """Get progress from all worker progress files, but only for running workers"""
    progress_files = glob.glob("migration_progress_worker*.json")
    workers = {}
    running_workers = get_running_worker_ids()
    
    for file_path in progress_files:
        # Extract worker ID from filename
        try:
            worker_id = int(file_path.split('_worker')[1].split('.')[0])
            # Only include workers that are actually running
            if worker_id in running_workers:
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

def display_dashboard(workers: Dict[int, Dict[str, Any]], start_time: datetime, db_counts: Dict[str, int] = None, ebooks_with_sources: int = None):
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
    print(f"TOTAL (from progress files):")
    print(f"  Completed: {total_completed:>7,} | Uploaded: {total_uploaded:>7,} | "
          f"Already exists: {total_already_exists:>5,} | Errors: {total_errors:>4,}")
    
    # Use cached database counts (refreshed every 5 minutes)
    if db_counts is None:
        db_counts = get_database_counts()
    ebooks_total = db_counts.get('ebooks', 0)
    sources_total = db_counts.get('sources', 0)
    
    if ebooks_with_sources is None:
        ebooks_with_sources = get_ebooks_with_sources_count()
    
    print(f"\nTOTAL (from MyBookshelf2 database - refreshed every 5 min):")
    print(f"  Total ebooks: {ebooks_total:>7,} | Sources (files): {sources_total:>7,}")
    if ebooks_with_sources < ebooks_total:
        print(f"  ⚠️  Working ebooks (with files): {ebooks_with_sources:>7,} | Orphaned: {ebooks_total - ebooks_with_sources:>7,}")
    else:
        print(f"  ✓ Working ebooks (with files): {ebooks_with_sources:>7,}")
    
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
    
    # Cache for database counts (refresh every 5 minutes)
    db_counts_cache = None
    db_counts_cache_time = None
    ebooks_with_sources_cache = None
    ebooks_with_sources_cache_time = None
    DB_CACHE_DURATION = 300  # 5 minutes in seconds
    
    try:
        while True:
            # Get worker progress from JSON files
            workers = get_worker_progress()
            
            # Also check log files for running workers that might not have progress files yet
            running_workers = get_running_worker_ids()
            for worker_id in running_workers:
                if worker_id not in workers:
                    log_stats = get_worker_log_stats(worker_id)
                    if log_stats.get("status") != "not_started":
                        # Worker exists but no progress file yet
                        workers[worker_id] = {"completed_files": {}, "errors": []}
            
            # Refresh database counts only if cache is expired (every 5 minutes)
            current_time = time.time()
            if (db_counts_cache is None or 
                db_counts_cache_time is None or 
                (current_time - db_counts_cache_time) >= DB_CACHE_DURATION):
                # Cache expired or not set, refresh
                db_counts_cache = get_database_counts()
                db_counts_cache_time = current_time
                ebooks_with_sources_cache = get_ebooks_with_sources_count()
                ebooks_with_sources_cache_time = current_time
            
            if not workers:
                print("No worker progress files found. Waiting for workers to start...")
                time.sleep(5)
                continue
            
            # Pass cached database counts to display function
            display_dashboard(workers, start_time, db_counts_cache, ebooks_with_sources_cache)
            time.sleep(5)  # Update every 5 seconds for more responsive monitoring
            
    except KeyboardInterrupt:
        print("\n\nMonitor stopped.")
    except Exception as e:
        print(f"\n\nError in monitor: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

