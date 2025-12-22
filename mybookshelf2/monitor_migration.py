#!/usr/bin/env python3
"""
Monitor parallel migration progress
Aggregates progress from all worker progress files and displays real-time dashboard
"""

import json
import time
import os
import glob
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple

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
            # Parse output - JSON might be mixed with warnings/stderr
            # Look for JSON object in stdout (may have warnings before/after)
            stdout = result.stdout.strip()
            # Try to find JSON object in output
            import re
            json_match = re.search(r'\{[^}]*"ebooks"[^}]*\}', stdout)
            if json_match:
                return json.loads(json_match.group(0))
            # Fallback: try parsing entire stdout
            return json.loads(stdout)
    except Exception as e:
        # Log error for debugging instead of silently failing
        import sys
        print(f"Warning: Failed to get database counts: {e}", file=sys.stderr)
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
            # Extract number from output (may have warnings before/after)
            import re
            stdout = result.stdout.strip()
            # Look for a number (the count)
            number_match = re.search(r'\b(\d+)\b', stdout)
            if number_match:
                return int(number_match.group(1))
            # Fallback: try parsing entire stdout as int
            return int(stdout)
    except Exception as e:
        # Log error for debugging instead of silently failing
        import sys
        print(f"Warning: Failed to get ebooks with sources count: {e}", file=sys.stderr)
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

def parse_log_timestamp(line: str) -> Optional[datetime]:
    """Extract timestamp from log line (format: YYYY-MM-DD HH:MM:SS,mmm)"""
    # Match timestamp pattern: 2025-11-27 14:12:34,056
    timestamp_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d{3}'
    match = re.search(timestamp_pattern, line)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
    return None

def get_worker_log_stats(worker_id: int) -> Dict[str, Any]:
    """Get statistics from worker log file, including timestamp"""
    log_file = Path(f"migration_worker{worker_id}.log")
    if not log_file.exists():
        return {"status": "not_started", "last_activity": None, "last_activity_time": None}
    
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
                return {"status": "empty", "last_activity": None, "last_activity_time": None}
            
            # Decode last few lines
            decoded_lines = [line.decode('utf-8', errors='ignore').strip() for line in lines[-20:]]
            
            # Filter out warning messages about progress file format - these aren't meaningful activity
            # Look for the last meaningful activity line (not warnings about file format)
            meaningful_lines = [line for line in decoded_lines 
                              if not ("Progress file contains" in line or 
                                     "multiple JSON objects" in line or
                                     "attempting to parse" in line)]
            
            # Use last meaningful line, or fall back to last line if all are warnings
            last_line = meaningful_lines[-1] if meaningful_lines else decoded_lines[-1] if decoded_lines else ""
            last_activity_time = parse_log_timestamp(last_line)
            
            # Check for completion
            if "Migration complete" in last_line:
                # Extract success/error counts
                if "Success:" in last_line and "Errors:" in last_line:
                    try:
                        parts = last_line.split("Success:")[1].split(",")
                        success = int(parts[0].strip())
                        errors = int(parts[1].split("Errors:")[1].strip())
                        return {"status": "completed", "success": success, "errors": errors, 
                               "last_activity": last_line, "last_activity_time": last_activity_time}
                    except:
                        pass
                return {"status": "completed", "last_activity": last_line, "last_activity_time": last_activity_time}
            
            # Check for uploading/processing
            if "Uploading:" in last_line or "Successfully uploaded:" in last_line:
                return {"status": "uploading", "last_activity": last_line, "last_activity_time": last_activity_time}
            
            # Check for database query or batch processing
            if "Fetched" in last_line and "rows" in last_line:
                return {"status": "querying_db", "last_activity": last_line, "last_activity_time": last_activity_time}
            
            # Check for batch processing (discovery phase)
            if "Processed batch" in last_line or "Querying Calibre database" in last_line or "Found" in last_line and "new files" in last_line:
                return {"status": "discovering", "last_activity": last_line, "last_activity_time": last_activity_time}
            
            # Check for batch completion with all duplicates (Success: 0, Errors: 0)
            # This indicates worker is processing a range where all files are already uploaded
            # This is NOT stuck - worker is making progress through duplicate ranges
            if "Batch" in last_line and "complete" in last_line:
                # Try to extract success/error counts
                if "Success: 0" in last_line and "Errors: 0" in last_line:
                    return {"status": "processing_duplicates", "last_activity": last_line, "last_activity_time": last_activity_time}
                elif "Success:" in last_line and "Errors:" in last_line:
                    # Has some success or errors - normal processing
                    return {"status": "uploading", "last_activity": last_line, "last_activity_time": last_activity_time}
            
            # Check for progress
            if "Progress:" in last_line:
                return {"status": "running", "last_activity": last_line, "last_activity_time": last_activity_time}
            
            # Check for errors
            if "ERROR" in last_line:
                return {"status": "error", "last_activity": last_line, "last_activity_time": last_activity_time}
            
            return {"status": "initializing", "last_activity": last_line, "last_activity_time": last_activity_time}
    except Exception as e:
        return {"status": f"error: {str(e)[:30]}", "last_activity": None, "last_activity_time": None}

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

def get_last_upload_time(worker_id: int) -> Optional[datetime]:
    """Get timestamp of last successful upload from worker log"""
    log_file = Path(f"migration_worker{worker_id}.log")
    if not log_file.exists():
        return None
    
    try:
        # Read last 50KB to find recent uploads
        with open(log_file, 'rb') as f:
            try:
                f.seek(-51200, 2)  # Last 50KB
            except OSError:
                f.seek(0)
            lines = f.readlines()
            
            # Search backwards for "Successfully uploaded" messages
            for line in reversed(lines):
                decoded = line.decode('utf-8', errors='ignore').strip()
                if "Successfully uploaded:" in decoded:
                    timestamp = parse_log_timestamp(decoded)
                    if timestamp:
                        return timestamp
    except Exception:
        pass
    return None

def check_alerts(workers: Dict[int, Dict[str, Any]], alert_threshold_seconds: int = 300) -> Tuple[list, Optional[datetime]]:
    """
    Check for alerts:
    1. Workers stuck for more than threshold
    2. No new uploads for more than threshold
    
    Returns: (list of alert messages, last_upload_time)
    """
    alerts = []
    current_time = datetime.now()
    last_upload_time = None
    
    # Check each worker for stuck status
    for worker_id, progress in workers.items():
        log_stats = get_worker_log_stats(worker_id)
        last_activity_time = log_stats.get("last_activity_time")
        status = log_stats.get("status", "unknown")
        
        # Skip completed workers
        if status == "completed":
            continue
        
        # Check if worker is stuck (no activity for threshold)
        if last_activity_time:
            time_since_activity = (current_time - last_activity_time).total_seconds()
            if time_since_activity > alert_threshold_seconds:
                stuck_minutes = int(time_since_activity // 60)
                alerts.append(f"⚠️  Worker {worker_id} is STUCK - no activity for {stuck_minutes} minutes (status: {status})")
        
        # Track last upload time
        worker_last_upload = get_last_upload_time(worker_id)
        if worker_last_upload:
            if last_upload_time is None or worker_last_upload > last_upload_time:
                last_upload_time = worker_last_upload
    
    # Check if no uploads for threshold
    if last_upload_time:
        time_since_upload = (current_time - last_upload_time).total_seconds()
        if time_since_upload > alert_threshold_seconds:
            no_upload_minutes = int(time_since_upload // 60)
            alerts.append(f"⚠️  NO NEW BOOKS UPLOADED for {no_upload_minutes} minutes (last upload: {last_upload_time.strftime('%H:%M:%S')})")
    elif workers:
        # Workers exist but no uploads found at all
        # Check if any worker has been running for more than threshold
        for worker_id in workers.keys():
            log_stats = get_worker_log_stats(worker_id)
            if log_stats.get("status") not in ["completed", "not_started", "empty"]:
                # Worker is running but no uploads - might be stuck in discovery/pre-processing
                last_activity_time = log_stats.get("last_activity_time")
                if last_activity_time:
                    time_since_activity = (current_time - last_activity_time).total_seconds()
                    if time_since_activity > alert_threshold_seconds:
                        # Already added to alerts above, skip
                        pass
    
    return alerts, last_upload_time

def display_dashboard(workers: Dict[int, Dict[str, Any]], start_time: datetime, db_counts: Dict[str, int] = None, ebooks_with_sources: int = None, alerts: list = None, current_rate: float = None):
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
    
    # Display alerts if any
    if alerts:
        print("🚨 ALERTS:")
        print("-" * 80)
        for alert in alerts:
            print(alert)
        print("-" * 80)
        print()
    
    # Aggregate statistics
    total_completed = 0
    total_errors = 0
    total_uploaded = 0
    total_already_exists = 0
    
    print("Worker Status:")
    print("-" * 80)
    
    # Get worker memory usage for each worker
    worker_memory = {}
    try:
        import psutil
        for worker_id in sorted(workers.keys()):
            try:
                # Find worker process by matching worker-id in command line
                for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'memory_info']):
                    try:
                        cmdline = proc.info['cmdline']
                        if cmdline:
                            cmdline_str = ' '.join(str(c) for c in cmdline)
                            # Match worker ID in command line
                            if 'bulk_migrate_calibre' in cmdline_str and f'--worker-id {worker_id}' in cmdline_str:
                                worker_memory[worker_id] = proc.info['memory_info'].rss / (1024**2)  # MB
                                break
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
            except Exception:
                pass
    except ImportError:
        pass
    except Exception:
        pass
    
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
        
        # Get RAM usage for this worker
        ram_info = ""
        if worker_id in worker_memory:
            ram_mb = worker_memory[worker_id]
            ram_info = f" | RAM: {ram_mb:>6.1f} MB"
        else:
            ram_info = " | RAM:   N/A"
        
        print(f"Worker {worker_id}: {status_icon} {log_stats.get('status', 'unknown'):12} | "
              f"Completed: {completed:>7,} | Uploaded: {uploaded:>7,} | "
              f"Already exists: {already_exists:>5,} | Errors: {errors:>4,}{ram_info}")
        
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
    
    # Memory monitoring
    try:
        import psutil
        mem = psutil.virtual_memory()
        mem_percent = mem.percent
        mem_used_gb = mem.used / (1024**3)
        mem_total_gb = mem.total / (1024**3)
        mem_available_gb = mem.available / (1024**3)
        
        # Calculate total worker memory (already computed above in worker_memory dict)
        worker_mem_total = sum(worker_memory.values()) if worker_memory else 0
        worker_count = len(worker_memory)
        
        print(f"\nSystem Memory:")
        print(f"  Used: {mem_used_gb:.1f} GB / {mem_total_gb:.1f} GB ({mem_percent:.1f}%) | "
              f"Available: {mem_available_gb:.1f} GB")
        if worker_count > 0:
            print(f"  Workers RAM: {worker_mem_total:.1f} MB total ({worker_mem_total/worker_count:.1f} MB avg per worker)")
        
        # Alert if memory is low
        if mem_percent > 80:
            print(f"  ⚠️  WARNING: Memory usage is {mem_percent:.1f}% - consider reducing worker count")
        elif mem_percent > 90:
            print(f"  🚨 CRITICAL: Memory usage is {mem_percent:.1f}% - workers may be killed by OOM")
        
        # Disk I/O monitoring
        try:
            import subprocess
            
            # Find the disk where Calibre library is mounted
            calibre_path = "/media/haimengzhou/78613a5d-17be-413e-8691-908154970815/calibre library"
            calibre_disk = None
            device_name = None
            
            # Get disk partitions to find which device contains the Calibre library
            # Use df command to get accurate device for the path
            try:
                import subprocess
                result = subprocess.run(
                    ['df', calibre_path],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                if result.returncode == 0:
                    lines = result.stdout.strip().split('\n')
                    if len(lines) > 1:
                        # Second line contains the device info
                        parts = lines[1].split()
                        if len(parts) > 0:
                            calibre_disk = parts[0]
                            # Extract device name (e.g., /dev/sde1 -> sde)
                            device_name = calibre_disk.split('/')[-1].rstrip('0123456789')
            except:
                pass
            
            # Fallback to psutil if df failed
            if not device_name:
                partitions = psutil.disk_partitions()
                for partition in partitions:
                    # Check if Calibre path is on this partition
                    if calibre_path.startswith(partition.mountpoint):
                        calibre_disk = partition.device
                        device_name = calibre_disk.split('/')[-1].rstrip('0123456789')
                        break
            
            # Get disk I/O statistics (need to track previous values for rate calculation)
            if not hasattr(display_dashboard, '_prev_io_counters'):
                display_dashboard._prev_io_counters = {}
                display_dashboard._prev_io_time = time.time()
            
            current_time = time.time()
            time_delta = current_time - display_dashboard._prev_io_time
            if time_delta < 1.0:
                time_delta = 1.0  # Minimum 1 second for rate calculation
            
            disk_io_counters = psutil.disk_io_counters(perdisk=True)
            
            # Try to get disk utilization from iostat (more accurate than I/O wait)
            disk_util_percent = None
            
            if device_name and disk_io_counters:
                    
                    # Try to get %util from iostat
                    # Use 2 samples: first is since boot, second is actual current utilization
                    try:
                        result = subprocess.run(
                            ['iostat', '-x', '-d', device_name, '1', '2'],
                            capture_output=True,
                            text=True,
                            timeout=5
                        )
                        if result.returncode == 0:
                            lines = result.stdout.split('\n')
                            # Find the header line to locate %util column
                            header_line = None
                            for i, line in enumerate(lines):
                                if 'Device' in line and '%util' in line:
                                    header_line = i
                                    # Find %util column index
                                    header_parts = line.split()
                                    util_col_idx = None
                                    for idx, col in enumerate(header_parts):
                                        if col == '%util':
                                            util_col_idx = idx
                                            break
                                    break
                            
                            # Find the line with device name and extract %util
                            # iostat outputs 2 samples: first is since boot, second is current
                            # We want the second sample (current utilization)
                            device_lines = []
                            for line in lines:
                                if line.startswith(device_name) and not line.startswith('Device'):
                                    device_lines.append(line)
                            
                            # Use the last (second) sample for current utilization
                            if device_lines:
                                line = device_lines[-1]
                                parts = line.split()
                                if util_col_idx is not None and len(parts) > util_col_idx:
                                    try:
                                        util = float(parts[util_col_idx])
                                        if 0 <= util <= 100:
                                            disk_util_percent = util
                                    except (ValueError, IndexError):
                                        pass
                                # Fallback: %util is always the last column in iostat -x output
                                if disk_util_percent is None and len(parts) >= 10:
                                    try:
                                        util = float(parts[-1])
                                        if 0 <= util <= 100:
                                            disk_util_percent = util
                                    except (ValueError, IndexError):
                                        pass
                    except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError):
                        pass
            
            # If iostat failed, try to find the device in disk_io_counters
            if device_name and device_name in disk_io_counters:
                disk_io = disk_io_counters[device_name]
                
                # Calculate I/O rates (MB/s) by comparing with previous values
                prev_io = display_dashboard._prev_io_counters.get(device_name)
                if prev_io:
                    read_rate_mb = (disk_io.read_bytes - prev_io.read_bytes) / (1024**2) / time_delta
                    write_rate_mb = (disk_io.write_bytes - prev_io.write_bytes) / (1024**2) / time_delta
                    read_ops_rate = (disk_io.read_count - prev_io.read_count) / time_delta
                    write_ops_rate = (disk_io.write_count - prev_io.write_count) / time_delta
                else:
                    read_rate_mb = 0
                    write_rate_mb = 0
                    read_ops_rate = 0
                    write_ops_rate = 0
                
                # Store current values for next iteration
                display_dashboard._prev_io_counters[device_name] = disk_io
                display_dashboard._prev_io_time = current_time
                
                # Get disk usage for the Calibre mount point
                try:
                    disk_usage = psutil.disk_usage(calibre_path)
                    disk_used_gb = disk_usage.used / (1024**3)
                    disk_total_gb = disk_usage.total / (1024**3)
                    disk_percent = disk_usage.percent
                except:
                    disk_used_gb = 0
                    disk_total_gb = 0
                    disk_percent = 0
                
                # Calculate average wait time (if available from iostat, otherwise estimate)
                avg_wait_ms = None
                if disk_util_percent is not None:
                    # Estimate wait time based on utilization and ops rate
                    total_ops = read_ops_rate + write_ops_rate
                    if total_ops > 0:
                        # Rough estimate: higher utilization = higher wait time
                        avg_wait_ms = (disk_util_percent / 100) * 10  # Rough estimate in ms
                
                print(f"\nDisk I/O ({device_name} - Calibre library):")
                if disk_util_percent is not None:
                    print(f"  Utilization: {disk_util_percent:.1f}% | "
                          f"Read: {read_rate_mb:.1f} MB/s ({read_ops_rate:.0f} ops/s) | "
                          f"Write: {write_rate_mb:.1f} MB/s ({write_ops_rate:.0f} ops/s)")
                else:
                    print(f"  Read: {read_rate_mb:.1f} MB/s ({read_ops_rate:.0f} ops/s) | "
                          f"Write: {write_rate_mb:.1f} MB/s ({write_ops_rate:.0f} ops/s)")
                print(f"  Disk Usage: {disk_used_gb:.1f} GB / {disk_total_gb:.1f} GB ({disk_percent:.1f}%)")
                
                # Alert based on utilization or I/O rates
                if disk_util_percent is not None:
                    if disk_util_percent > 90:
                        print(f"  🚨 CRITICAL: Disk utilization is {disk_util_percent:.1f}% - disk is saturated, reduce workers")
                    elif disk_util_percent > 70:
                        print(f"  ⚠️  WARNING: Disk utilization is {disk_util_percent:.1f}% - disk I/O is high")
                elif read_ops_rate + write_ops_rate > 500:
                    print(f"  ⚠️  WARNING: High I/O operations ({read_ops_rate + write_ops_rate:.0f} ops/s) - disk may be busy")
            else:
                # Fallback: try to find sde device or show available devices
                if disk_io_counters:
                    # Try common device names
                    for device in ['sde', 'sdd', 'sdc', 'sdb', 'sda']:
                        if device in disk_io_counters:
                            disk_io = disk_io_counters[device]
                            prev_io = display_dashboard._prev_io_counters.get(device)
                            if prev_io:
                                read_rate_mb = (disk_io.read_bytes - prev_io.read_bytes) / (1024**2) / time_delta
                                write_rate_mb = (disk_io.write_bytes - prev_io.write_bytes) / (1024**2) / time_delta
                                read_ops_rate = (disk_io.read_count - prev_io.read_count) / time_delta
                                write_ops_rate = (disk_io.write_count - prev_io.write_count) / time_delta
                            else:
                                read_rate_mb = 0
                                write_rate_mb = 0
                                read_ops_rate = 0
                                write_ops_rate = 0
                            display_dashboard._prev_io_counters[device] = disk_io
                            
                            # Try to get utilization for this device
                            try:
                                result = subprocess.run(
                                    ['iostat', '-x', '-d', device, '1', '1'],
                                    capture_output=True,
                                    text=True,
                                    timeout=3
                                )
                                if result.returncode == 0:
                                    lines = result.stdout.split('\n')
                                    # Find header to locate %util column
                                    util_col_idx = None
                                    for line in lines:
                                        if 'Device' in line and '%util' in line:
                                            header_parts = line.split()
                                            for idx, col in enumerate(header_parts):
                                                if col == '%util':
                                                    util_col_idx = idx
                                                    break
                                            break
                                    
                                    # Extract %util from device line
                                    for line in lines:
                                        if line.startswith(device):
                                            parts = line.split()
                                            if util_col_idx is not None and len(parts) > util_col_idx:
                                                try:
                                                    util = float(parts[util_col_idx])
                                                    if 0 <= util <= 100:
                                                        disk_util_percent = util
                                                except (ValueError, IndexError):
                                                    pass
                                            elif len(parts) >= 10:
                                                # Fallback: last column
                                                try:
                                                    util = float(parts[-1])
                                                    if 0 <= util <= 100:
                                                        disk_util_percent = util
                                                except (ValueError, IndexError):
                                                    pass
                                            break
                            except:
                                pass
                            
                            print(f"\nDisk I/O ({device}):")
                            if disk_util_percent is not None:
                                print(f"  Utilization: {disk_util_percent:.1f}% | "
                                      f"Read: {read_rate_mb:.1f} MB/s ({read_ops_rate:.0f} ops/s) | "
                                      f"Write: {write_rate_mb:.1f} MB/s ({write_ops_rate:.0f} ops/s)")
                            else:
                                print(f"  Read: {read_rate_mb:.1f} MB/s ({read_ops_rate:.0f} ops/s) | "
                                      f"Write: {write_rate_mb:.1f} MB/s ({write_ops_rate:.0f} ops/s)")
                            
                            if disk_util_percent is not None:
                                if disk_util_percent > 90:
                                    print(f"  🚨 CRITICAL: Disk utilization is {disk_util_percent:.1f}% - disk is saturated")
                                elif disk_util_percent > 70:
                                    print(f"  ⚠️  WARNING: Disk utilization is {disk_util_percent:.1f}% - disk I/O is high")
                            break
        except Exception as e:
            # Silently fail disk I/O monitoring
            pass
    except ImportError:
        # psutil not available, skip memory and disk monitoring
        pass
    except Exception as e:
        # Silently fail memory monitoring
        pass
    
    # Display rate and ETA (use current_rate if available, otherwise calculate from elapsed time)
    if current_rate is not None and current_rate > 0:
        # Use the delta-based rate (more accurate)
        rate_per_min = current_rate * 60
        print(f"\nRate: {rate_per_min:.1f} books/minute (based on recent activity)")
        
        # Estimate ETA based on current rate
        # We don't know exact total, but we can estimate based on typical Calibre library sizes
        # For now, just show the rate without ETA since we don't know the total
        if total_completed > 1000:  # Only show rough ETA if we have meaningful progress
            # Very rough estimate: assume we're processing a large library
            # This is just a placeholder - actual ETA would need total book count
            print(f"  (ETA calculation requires total book count)")
    elif total_completed > 0 and elapsed > 60:  # Only show rate from elapsed time if monitor has been running for at least 1 minute
        # Fallback: calculate rate from elapsed time (less accurate but better than nothing)
        rate = total_completed / elapsed  # books per second
        if rate > 0:
            rate_per_min = rate * 60
            print(f"\nRate: {rate_per_min:.1f} books/minute (based on total elapsed time)")
            print(f"  (Note: This is approximate - rate may vary over time)")
    
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
    
    # Track previous values for rate calculation
    prev_total_completed = 0
    prev_rate_time = start_time
    
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
            
            # Check for alerts (5 minute threshold = 300 seconds)
            alerts, last_upload_time = check_alerts(workers, alert_threshold_seconds=300)
            
            # Calculate current total completed for rate calculation
            current_total_completed = sum(len(w.get("completed_files", {})) for w in workers.values())
            
            # Calculate rate based on change since last update
            current_time = datetime.now()
            time_delta = (current_time - prev_rate_time).total_seconds()
            if time_delta > 0 and prev_total_completed > 0:
                # Only calculate rate if we have previous data and meaningful time delta
                completed_delta = current_total_completed - prev_total_completed
                if completed_delta >= 0:  # Only if progress increased
                    rate_per_second = completed_delta / time_delta
                else:
                    rate_per_second = 0
            else:
                rate_per_second = None
            
            # Update previous values for next iteration
            prev_total_completed = current_total_completed
            prev_rate_time = current_time
            
            # Pass cached database counts, alerts, and rate to display function
            display_dashboard(workers, start_time, db_counts_cache, ebooks_with_sources_cache, alerts, rate_per_second)
            time.sleep(5)  # Update every 5 seconds for more responsive monitoring
            
    except KeyboardInterrupt:
        print("\n\nMonitor stopped.")
    except Exception as e:
        print(f"\n\nError in monitor: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

