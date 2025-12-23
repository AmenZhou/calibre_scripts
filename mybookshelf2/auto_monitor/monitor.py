#!/usr/bin/env python3
"""
Auto-Monitor for MyBookshelf2 Migration Workers

Monitors workers for stuck conditions and automatically applies fixes.
Can use LLM to analyze and debug issues.
"""
import sys
import time
import json
import re
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Set, Tuple, List
import glob

# Add parent directory to path to import monitor_migration functions
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from monitor_migration import (
        get_running_worker_ids,
        get_worker_progress,
        get_last_upload_time,
        get_worker_log_stats,
        parse_log_timestamp
    )
except ImportError:
    print("Error: Could not import from monitor_migration.py")
    print("Make sure monitor_migration.py is in the parent directory")
    sys.exit(1)

# Handle imports for both script and module usage
try:
    from .config import (
        STUCK_THRESHOLD_SECONDS, COOLDOWN_SECONDS, CHECK_INTERVAL_SECONDS,
        LOG_FILE, WORKER_LOG_DIR, LOG_LINES_TO_ANALYZE, OPENAI_API_KEY,
        MAX_FIX_ATTEMPTS, SUCCESS_VERIFICATION_SECONDS, ESCALATION_ACTION,
        TARGET_WORKER_COUNT, MIN_WORKER_COUNT, MAX_WORKER_COUNT,
        DISK_IO_SATURATED_THRESHOLD, DISK_IO_HIGH_THRESHOLD, DISK_IO_NORMAL_THRESHOLD,
        DISK_IO_SCALE_DOWN_COOLDOWN, DISK_IO_SCALE_UP_COOLDOWN, CALIBRE_LIBRARY_PATH,
        HISTORY_FILE, RECURRING_ROOT_CAUSE_THRESHOLD, DISCOVERY_THRESHOLD_SECONDS,
        WARNING_THRESHOLD_RATIO, EARLY_INTERVENTION_THRESHOLD_RATIO
    )
    from .llm_debugger import analyze_worker_with_llm
    from .fix_applier import apply_restart, apply_code_fix, apply_config_fix, save_fix_to_history, verify_history_entry
except ImportError:
    # Running as script, use absolute imports
    sys.path.insert(0, str(Path(__file__).parent))
    from config import (
        STUCK_THRESHOLD_SECONDS, COOLDOWN_SECONDS, CHECK_INTERVAL_SECONDS,
        LOG_FILE, WORKER_LOG_DIR, LOG_LINES_TO_ANALYZE, OPENAI_API_KEY,
        MAX_FIX_ATTEMPTS, SUCCESS_VERIFICATION_SECONDS, ESCALATION_ACTION,
        TARGET_WORKER_COUNT, MIN_WORKER_COUNT, MAX_WORKER_COUNT,
        DISK_IO_SATURATED_THRESHOLD, DISK_IO_HIGH_THRESHOLD, DISK_IO_NORMAL_THRESHOLD,
        DISK_IO_SCALE_DOWN_COOLDOWN, DISK_IO_SCALE_UP_COOLDOWN, CALIBRE_LIBRARY_PATH,
        HISTORY_FILE, RECURRING_ROOT_CAUSE_THRESHOLD, DISCOVERY_THRESHOLD_SECONDS,
        WARNING_THRESHOLD_RATIO, EARLY_INTERVENTION_THRESHOLD_RATIO
    )
    from llm_debugger import analyze_worker_with_llm
    from fix_applier import apply_restart, apply_code_fix, apply_config_fix, save_fix_to_history, verify_history_entry


# Setup logging
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Track last fix time per worker (for cooldown)
worker_last_fix_time: Dict[int, datetime] = {}

# Track fix attempts per worker (for escalation)
worker_fix_attempts: Dict[int, list] = {}  # List of fix attempts with timestamps and success status

# Track workers that are paused (after max attempts)
paused_workers: Set[int] = set()

# LLM caching for optimization
llm_cache: Dict[Tuple[int, str], Tuple[datetime, Dict[str, Any]]] = {}
worker_last_llm_analysis: Dict[int, datetime] = {}

# Track worker scaling state
desired_worker_count: int = TARGET_WORKER_COUNT  # Current desired worker count
last_scale_down_time: Optional[datetime] = None  # Last time we scaled down
last_scale_up_time: Optional[datetime] = None  # Last time we scaled up

# Track worker health metrics over time
worker_health_history: Dict[int, List[Dict[str, Any]]] = {}  # List of health scores over time


def get_fix_attempt_count(worker_id: int, within_hours: int = 24) -> int:
    """Get number of fix attempts for a worker within the specified time window"""
    if worker_id not in worker_fix_attempts:
        return 0
    
    cutoff_time = datetime.now() - timedelta(hours=within_hours)
    attempts = [a for a in worker_fix_attempts[worker_id] 
                if datetime.fromisoformat(a["timestamp"]) > cutoff_time]
    return len(attempts)


def verify_fix_success(worker_id: int, fix_time: datetime) -> bool:
    """Verify if a fix was successful by checking if worker recovered"""
    # Wait a bit for worker to recover (but don't wait too long in the check)
    time.sleep(min(SUCCESS_VERIFICATION_SECONDS, 30))  # Cap at 30 seconds for responsiveness
    
    # Check if worker is still stuck
    diagnostics = check_worker_stuck(worker_id, STUCK_THRESHOLD_SECONDS, False)
    if diagnostics is None:
        # Worker is no longer stuck - fix was successful
        return True
    
    # Check if there was any new activity/upload after the fix
    last_upload = get_last_upload_time(worker_id)
    if last_upload and last_upload > fix_time:
        return True
    
    return False


def get_worker_logs(worker_id: int, lines: int = LOG_LINES_TO_ANALYZE) -> str:
    """Get recent log lines from worker log file"""
    log_file = WORKER_LOG_DIR / f"migration_worker{worker_id}.log"
    if not log_file.exists():
        return ""
    
    try:
        with open(log_file, 'rb') as f:
            # Read last N lines
            try:
                f.seek(-min(LOG_LINES_TO_ANALYZE * 100, 0), 2)  # Approximate: 100 chars per line
            except OSError:
                f.seek(0)
            content = f.read().decode('utf-8', errors='ignore')
            lines_list = content.split('\n')
            return '\n'.join(lines_list[-lines:])
    except Exception as e:
        logger.error(f"Error reading log for worker {worker_id}: {e}")
        return ""


def extract_error_patterns(logs: str) -> list:
    """Extract error patterns from logs"""
    patterns = []
    
    # Common error patterns
    error_keywords = [
        r'ERROR.*?(\w+Error|Exception|Failed)',
        r'API 500 error',
        r'Connection.*?failed',
        r'File name too long',
        r'NUL.*?character',
        r'Timeout',
        r'Traceback'
    ]
    
    for pattern in error_keywords:
        matches = re.findall(pattern, logs, re.IGNORECASE)
        if matches:
            patterns.extend(matches[:5])  # Limit to 5 per pattern
    
    return list(set(patterns))  # Remove duplicates


def extract_book_id_range(logs: str) -> str:
    """Extract book.id range from logs to detect infinite loops"""
    # Look for patterns like "book.id > 53213" or "Processed batch: book.id > X"
    matches = re.findall(r'book\.id\s*[><=]\s*(\d+)', logs, re.IGNORECASE)
    if matches:
        # Return the most recent one
        return f"book.id > {matches[-1]}"
    
    # Also check for "last_processed_book_id"
    matches = re.findall(r'last_processed_book_id[:\s]+(\d+)', logs, re.IGNORECASE)
    if matches:
        return f"last_processed_book_id: {matches[-1]}"
    
    return "unknown"


def check_worker_no_progress(worker_id: int, logs: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Check if worker is making progress by looking for multiple indicators with weighted scoring.
    
    Returns:
        Tuple of (is_no_progress: bool, progress_metrics: dict)
        progress_metrics contains detailed information about progress indicators
    """
    # Check last 500 lines for progress indicators
    recent_logs = '\n'.join(logs.split('\n')[-500:])
    all_logs = logs
    
    progress_metrics = {
        "upload_activity": 0,
        "file_discovery": 0,
        "batch_processing": 0,
        "database_activity": 0,
        "file_processing": 0,
        "total_score": 0
    }
    
    # Weighted scoring system
    weights = {
        "upload": 30,  # Uploads are strong progress indicator
        "file_discovery": 25,  # Finding files is good progress
        "batch_processing": 20,  # Batch processing shows activity
        "database": 15,  # Database queries show discovery
        "file_processing": 10  # File processing is lighter activity
    }
    
    # 1. Look for upload messages (highest weight)
    upload_patterns = [
        r"Successfully uploaded",
        r"Uploading:",
        r"Uploaded:",
        r"\[UPLOAD\]\s+Batch.*progress",
        r"files processed.*Success:.*Errors:"
    ]
    upload_matches = sum(1 for pattern in upload_patterns if re.search(pattern, recent_logs, re.IGNORECASE))
    if upload_matches > 0:
        progress_metrics["upload_activity"] = min(upload_matches * 10, 100)  # Cap at 100
        progress_metrics["total_score"] += weights["upload"]
        logger.debug(f"Worker {worker_id} upload activity: {upload_matches} matches")
    
    # 2. Look for file discovery (high weight)
    found_new_files = re.findall(r'Found\s+(\d+)\s+new\s+files', recent_logs, re.IGNORECASE)
    if found_new_files:
        new_files_counts = [int(x) for x in found_new_files]
        total_new_files = sum(new_files_counts)
        if total_new_files > 0:
            progress_metrics["file_discovery"] = min(total_new_files, 100)  # Cap at 100
            progress_metrics["total_score"] += weights["file_discovery"]
            logger.debug(f"Worker {worker_id} found {total_new_files} new files")
    
    # Also check "Found X new files so far" during batch processing
    found_so_far = re.findall(r'Found\s+(\d+)\s+new\s+files\s+so\s+far', recent_logs, re.IGNORECASE)
    if found_so_far:
        counts = [int(x) for x in found_so_far]
        total_so_far = sum(counts)
        if total_so_far > 0:
            progress_metrics["file_discovery"] = max(progress_metrics["file_discovery"], min(total_so_far, 100))
            if progress_metrics["total_score"] == 0 or "file_discovery" not in str(progress_metrics):
                progress_metrics["total_score"] += weights["file_discovery"]
    
    # 3. Look for batch processing (medium weight)
    batch_pattern = r'Processed batch.*?book\.id\s*>\s*(\d+)'
    batch_matches = re.findall(batch_pattern, recent_logs, re.IGNORECASE)
    if batch_matches:
        progress_metrics["batch_processing"] = len(batch_matches)
        progress_metrics["total_score"] += weights["batch_processing"]
        logger.debug(f"Worker {worker_id} processed {len(batch_matches)} batches")
    
    # Also check for rows processed
    rows_matches = re.findall(r'Processed batch.*rows\s*=\s*(\d+)', recent_logs, re.IGNORECASE)
    if rows_matches:
        total_rows = sum(int(x) for x in rows_matches if x.isdigit())
        if total_rows > 0:
            progress_metrics["batch_processing"] = max(progress_metrics["batch_processing"], min(total_rows // 100, 100))
            if progress_metrics["total_score"] < weights["batch_processing"]:
                progress_metrics["total_score"] += weights["batch_processing"]
    
    # 4. Look for database activity (medium weight)
    db_patterns = [
        r"Querying Calibre database",
        r"book\.id\s*>",
        r"Fetched.*rows",
        r"SELECT.*FROM.*books"
    ]
    db_matches = sum(1 for pattern in db_patterns if re.search(pattern, recent_logs, re.IGNORECASE))
    if db_matches > 0:
        progress_metrics["database_activity"] = min(db_matches * 5, 100)
        progress_metrics["total_score"] += weights["database"]
        logger.debug(f"Worker {worker_id} database activity: {db_matches} matches")
    
    # 5. Look for file processing activity (lower weight)
    processing_patterns = [
        r"Scanning.*files",
        r"Reading.*metadata",
        r"Preparing.*upload",
        r"Processing.*book",
        r"Progress:.*book\.id"
    ]
    processing_matches = sum(1 for pattern in processing_patterns if re.search(pattern, recent_logs, re.IGNORECASE))
    if processing_matches > 0:
        progress_metrics["file_processing"] = min(processing_matches * 3, 100)
        progress_metrics["total_score"] += weights["file_processing"]
        logger.debug(f"Worker {worker_id} file processing: {processing_matches} matches")
    
    # Calculate file processing rate (files per hour) from all logs
    # Look for upload timestamps to calculate rate
    upload_timestamps = []
    for line in all_logs.split('\n'):
        if re.search(r'Successfully uploaded|Uploaded:', line, re.IGNORECASE):
            timestamp = parse_log_timestamp(line)
            if timestamp:
                upload_timestamps.append(timestamp)
    
    if len(upload_timestamps) >= 2:
        time_span = (upload_timestamps[-1] - upload_timestamps[0]).total_seconds() / 3600  # hours
        if time_span > 0:
            files_per_hour = len(upload_timestamps) / time_span
            progress_metrics["upload_rate_files_per_hour"] = files_per_hour
            logger.debug(f"Worker {worker_id} upload rate: {files_per_hour:.1f} files/hour")
    
    # Determine if worker has no progress
    # Worker is considered to have no progress if total_score is very low (< 20% of max possible)
    max_possible_score = sum(weights.values())
    score_threshold = max_possible_score * 0.2  # 20% of max score
    
    is_no_progress = progress_metrics["total_score"] < score_threshold
    
    if is_no_progress:
        logger.debug(f"Worker {worker_id} shows no progress (score: {progress_metrics['total_score']}/{max_possible_score})")
    else:
        logger.debug(f"Worker {worker_id} shows progress (score: {progress_metrics['total_score']}/{max_possible_score})")
    
    return is_no_progress, progress_metrics


def check_worker_stuck(worker_id: int, stuck_threshold: int = STUCK_THRESHOLD_SECONDS, llm_enabled: bool = False) -> Optional[Dict[str, Any]]:
    """
    Check if a worker is stuck and return diagnostic information.
    Uses context-aware thresholds based on worker state and error rates.
    
    Returns:
        Dictionary with stuck status and diagnostics, or None if not stuck
    """
    # Get last upload time
    last_upload = get_last_upload_time(worker_id)
    log_stats = get_worker_log_stats(worker_id)
    status = log_stats.get("status", "unknown")
    last_activity = log_stats.get("last_activity_time")
    
    # Get logs early to check for errors and context
    logs = get_worker_logs(worker_id, lines=500)
    
    # Calculate error rate (errors per hour) from recent logs
    error_count = len(re.findall(r'ERROR|Exception|Failed|Traceback', logs, re.IGNORECASE))
    # Estimate time span of logs (rough approximation: 1 line per second average)
    log_lines = len(logs.split('\n'))
    estimated_hours = max(log_lines / 3600, 0.1)  # At least 0.1 hours
    error_rate = error_count / estimated_hours
    
    # Apply context-aware threshold adjustments
    adjusted_threshold = stuck_threshold
    
    # Workers with high error rate: reduce threshold by 25%
    if error_rate > 10:  # More than 10 errors per hour
        adjusted_threshold = stuck_threshold * 0.75
        logger.debug(f"Worker {worker_id} has high error rate ({error_rate:.1f}/hour) - using reduced threshold: {adjusted_threshold/60:.1f} min")
    
    # Workers processing large files: extend threshold by 50% (detected by slow upload rate)
    # This will be checked later when we have progress metrics
    
    if last_upload:
        # Worker has uploaded before - check time since last upload
        time_since_upload = (datetime.now() - last_upload).total_seconds()
        if time_since_upload < adjusted_threshold:
            return None
        
        # IMPORTANT: Even if no upload in threshold time, check if worker is making progress
        # Workers might be processing large batches or waiting for I/O without uploading
        logs = get_worker_logs(worker_id, lines=500)
        no_progress, progress_metrics = check_worker_no_progress(worker_id, logs)
        
        if not no_progress:
            # Worker is making progress (processing batches, querying DB, finding files)
            # Not stuck even if no recent upload
            logger.debug(f"Worker {worker_id} has no upload in {int(time_since_upload // 60)} min, but is making progress (score: {progress_metrics.get('total_score', 0)}) - not stuck")
            return None
        
        # Check if worker is just slow (has some progress but low rate)
        upload_rate = progress_metrics.get("upload_rate_files_per_hour", 0)
        if upload_rate > 0 and upload_rate < 1.0:  # Less than 1 file per hour
            # Worker is making progress but very slowly - might be processing large files
            # Use context-aware threshold: extend by 50% for slow workers
            extended_threshold = adjusted_threshold * 1.5
            if time_since_upload < extended_threshold:
                logger.debug(f"Worker {worker_id} is slow ({upload_rate:.2f} files/hour) but making progress - using extended threshold: {extended_threshold/60:.1f} min")
                return None
        
        # Worker has no upload AND no progress - it's stuck
        minutes_stuck = int(time_since_upload // 60)
    else:
        # Worker has never uploaded - check if stuck in discovery/initialization
        if not last_activity:
            return None
        
        # Check how long worker has been in current status
        # For workers in "initializing", "discovering", or "processing_duplicates", use a longer threshold
        # "processing_duplicates" means worker is finding files but they're all already uploaded
        # This is normal progress, not stuck
        if status == "processing_duplicates":
            # Worker is processing duplicates - this is normal, not stuck
            # Check if it's been doing this for too long (e.g., >60 minutes)
            # This might indicate worker needs to skip ahead to a different book.id range
            time_since_activity = (datetime.now() - last_activity).total_seconds()
            if time_since_activity > 3600:  # 60 minutes
                # Worker has been processing duplicates for over an hour
                # This might indicate it needs to skip ahead
                logger.debug(f"Worker {worker_id} has been processing duplicates for {int(time_since_activity/60)} min - may need to skip ahead")
                # Don't mark as stuck, but could suggest skipping ahead
            return None  # Not stuck, just processing duplicates
        
            # For workers in "initializing" or "discovering", use a longer threshold
            if status in ["initializing", "discovering"]:
                # Import discovery threshold from config
                try:
                    from .config import DISCOVERY_THRESHOLD_SECONDS
                except ImportError:
                    from config import DISCOVERY_THRESHOLD_SECONDS
                
                # Use discovery threshold (20 minutes by default) for workers in discovery phase
                discovery_threshold = DISCOVERY_THRESHOLD_SECONDS
                logger.debug(f"Worker {worker_id} in {status} status - using discovery threshold: {discovery_threshold/60} minutes")
            
            # First check if worker is making any progress (finding new files, uploading, processing batches)
            logs = get_worker_logs(worker_id, lines=500)
            no_progress, progress_metrics = check_worker_no_progress(worker_id, logs)
            
            if not no_progress:
                # Worker is making progress, not stuck - even if no uploads yet
                logger.debug(f"Worker {worker_id} is making progress during discovery (score: {progress_metrics.get('total_score', 0)}), not stuck")
                return None
            
            # Worker is not making progress - check how long it's been running
            logger.debug(f"Worker {worker_id} is NOT making progress - checking uptime (threshold: {discovery_threshold/60} min)")
            # Check process uptime to see how long worker has been running
            import subprocess
            try:
                # Find process by worker-id in command line
                result = subprocess.run(
                    ['pgrep', '-af', f'bulk_migrate_calibre.*--worker-id[[:space:]]+{worker_id}([[:space:]]|$)'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                logger.debug(f"Worker {worker_id} pgrep result: returncode={result.returncode}, stdout={result.stdout[:100] if result.stdout else None}")
                if result.returncode == 0 and result.stdout:
                    # Get process PID
                    pid_match = re.search(r'^(\d+)', result.stdout.strip())
                    if pid_match:
                        pid = pid_match.group(1)
                        # Get process start time using ps
                        ps_result = subprocess.run(
                            ['ps', '-o', 'etime=', '-p', pid],
                            capture_output=True,
                            text=True,
                            timeout=5
                        )
                        if ps_result.returncode == 0 and ps_result.stdout.strip():
                            # Parse elapsed time (format: [[DD-]hh:]mm:ss)
                            etime = ps_result.stdout.strip()
                            # Convert to minutes
                            parts = etime.split(':')
                            if len(parts) == 3:  # Could be DD-hh:mm:ss or hh:mm:ss
                                first_part = parts[0]
                                if '-' in first_part:  # DD-hh:mm:ss format
                                    days = int(first_part.split('-')[0])
                                    hours = int(parts[1])
                                    mins = int(parts[2].split(':')[0]) if ':' in parts[2] else int(parts[2])
                                    total_minutes = days * 1440 + hours * 60 + mins
                                else:  # hh:mm:ss format
                                    hours = int(parts[0])
                                    mins = int(parts[1])
                                    secs = int(parts[2]) if parts[2] else 0
                                    total_minutes = hours * 60 + mins
                            elif len(parts) == 2:  # mm:ss
                                total_minutes = int(parts[0])
                            else:
                                total_minutes = 0
                            
                            # If process has been running longer than discovery threshold and no progress, it's stuck
                            logger.debug(f"Worker {worker_id} process uptime: {total_minutes} minutes, threshold: {discovery_threshold/60} minutes, no_progress={no_progress}")
                            if total_minutes >= (discovery_threshold / 60):
                                minutes_stuck = total_minutes
                                logger.info(f"Worker {worker_id} detected as stuck: {minutes_stuck} minutes uptime, no progress detected (threshold: {discovery_threshold/60} min)")
                            else:
                                logger.debug(f"Worker {worker_id} not stuck yet: {total_minutes} < {discovery_threshold/60} minutes - allowing more time for discovery")
                                return None
                        else:
                            # Fallback: use last activity with threshold
                            time_since_activity = (datetime.now() - last_activity).total_seconds()
                            if time_since_activity < discovery_threshold:
                                return None
                            minutes_stuck = int(time_since_activity // 60)
                    else:
                        # Fallback: use last activity with threshold
                        time_since_activity = (datetime.now() - last_activity).total_seconds()
                        if time_since_activity < discovery_threshold:
                            return None
                        minutes_stuck = int(time_since_activity // 60)
                else:
                    # Process not found - use last activity
                    time_since_activity = (datetime.now() - last_activity).total_seconds()
                    if time_since_activity < discovery_threshold:
                        return None
                    minutes_stuck = int(time_since_activity // 60)
            except Exception as e:
                # Fallback: use last activity with threshold
                logger.warning(f"Worker {worker_id} exception in process check: {e}")
                # If we already determined no_progress, and exception occurred, 
                # still check if worker has been running long enough
                # Use a simpler check: if no progress and status is initializing/discovering for >10 min, it's stuck
                time_since_activity = (datetime.now() - last_activity).total_seconds()
                # For workers with no progress, use process start time or last_activity, whichever is longer
                # Since we can't get process time due to exception, use last_activity but with longer threshold
                # If worker has been in this status for >30 minutes with no progress, consider it stuck
                extended_threshold = 1800  # 30 minutes for exception case
                if time_since_activity < extended_threshold:
                    logger.debug(f"Worker {worker_id} not stuck (exception fallback): {time_since_activity} < {extended_threshold} seconds")
                    return None
                minutes_stuck = int(time_since_activity // 60)
                logger.info(f"Worker {worker_id} detected as stuck (exception fallback): {minutes_stuck} minutes, no progress")
        else:
            # Other statuses - use normal threshold based on last activity
            time_since_activity = (datetime.now() - last_activity).total_seconds()
            if time_since_activity < stuck_threshold:
                return None
            minutes_stuck = int(time_since_activity // 60)
    
    # Worker is stuck - collect diagnostic data
    try:
        # Use logs we already have, or get fresh ones if not available
        if 'logs' not in locals() or not logs:
            logs = get_worker_logs(worker_id)
        error_patterns = extract_error_patterns(logs)
        book_id_range = extract_book_id_range(logs)
        log_stats = get_worker_log_stats(worker_id)
        
        # Get progress metrics for diagnostics (reuse if already calculated)
        if 'progress_metrics' not in locals():
            _, progress_metrics = check_worker_no_progress(worker_id, logs)
        
        # Calculate error rate if not already done
        if 'error_rate' not in locals():
            error_count = len(re.findall(r'ERROR|Exception|Failed|Traceback', logs, re.IGNORECASE))
            log_lines = len(logs.split('\n'))
            estimated_hours = max(log_lines / 3600, 0.1)
            error_rate = error_count / estimated_hours
        
        # Use adjusted_threshold if available, otherwise use stuck_threshold
        threshold_used = adjusted_threshold if 'adjusted_threshold' in locals() else stuck_threshold
        
        diagnostics = {
            "worker_id": worker_id,
            "minutes_stuck": minutes_stuck,
            "last_upload_time": last_upload.isoformat() if last_upload else None,
            "book_id_range": book_id_range,
            "error_patterns": error_patterns,
            "status": log_stats.get("status", "unknown"),
            "error_rate_per_hour": error_rate,
            "progress_metrics": progress_metrics,
            "adjusted_threshold_minutes": threshold_used / 60,
            "logs": logs[-2000:] if len(logs) > 2000 else logs  # Limit log size for LLM
        }
        
        logger.info(f"Worker {worker_id} diagnostics collected: {minutes_stuck} minutes stuck, status={diagnostics['status']}")
        return diagnostics
    except Exception as e:
        logger.error(f"Error collecting diagnostics for worker {worker_id}: {e}", exc_info=True)
        # Return basic diagnostics even if collection fails
        return {
            "worker_id": worker_id,
            "minutes_stuck": minutes_stuck,
            "last_upload_time": last_upload.isoformat() if last_upload else None,
            "book_id_range": "unknown",
            "error_patterns": [f"Diagnostic collection error: {str(e)}"],
            "status": status,
            "logs": ""
        }


def check_worker_warning(worker_id: int, stuck_threshold: int = STUCK_THRESHOLD_SECONDS) -> Optional[Dict[str, Any]]:
    """
    Check if a worker is approaching stuck threshold and return warning/intervention level.
    
    Returns:
        Dictionary with warning level and diagnostics, or None if not approaching threshold
        Warning levels: "warning" (67% threshold), "intervention_needed" (83% threshold)
    """
    # Get last upload time
    last_upload = get_last_upload_time(worker_id)
    log_stats = get_worker_log_stats(worker_id)
    status = log_stats.get("status", "unknown")
    last_activity = log_stats.get("last_activity_time")
    
    # Determine actual threshold to use
    if status in ["initializing", "discovering"]:
        try:
            from .config import DISCOVERY_THRESHOLD_SECONDS
        except ImportError:
            from config import DISCOVERY_THRESHOLD_SECONDS
        actual_threshold = DISCOVERY_THRESHOLD_SECONDS
    else:
        actual_threshold = stuck_threshold
    
    # Calculate warning and intervention thresholds
    warning_threshold = actual_threshold * WARNING_THRESHOLD_RATIO
    intervention_threshold = actual_threshold * EARLY_INTERVENTION_THRESHOLD_RATIO
    
    time_since_activity = None
    if last_upload:
        time_since_activity = (datetime.now() - last_upload).total_seconds()
    elif last_activity:
        time_since_activity = (datetime.now() - last_activity).total_seconds()
    else:
        return None  # No activity to measure
    
    # Check if we're at warning or intervention level
    if time_since_activity >= intervention_threshold:
        # At intervention level (83% of threshold)
        level = "intervention_needed"
        minutes_until_stuck = (actual_threshold - time_since_activity) / 60
    elif time_since_activity >= warning_threshold:
        # At warning level (67% of threshold)
        level = "warning"
        minutes_until_stuck = (actual_threshold - time_since_activity) / 60
    else:
        return None  # Not yet at warning level
    
    # Get basic diagnostics
    logs = get_worker_logs(worker_id, lines=200)  # Fewer lines for warning checks
    error_patterns = extract_error_patterns(logs)
    
    return {
        "worker_id": worker_id,
        "warning_level": level,
        "minutes_until_stuck": int(minutes_until_stuck),
        "time_since_activity_seconds": time_since_activity,
        "threshold_seconds": actual_threshold,
        "status": status,
        "error_patterns": error_patterns[:3],  # Limit to 3 for warnings
        "last_upload_time": last_upload.isoformat() if last_upload else None
    }


def perform_early_intervention(worker_id: int, warning_info: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """
    Perform light intervention when worker is at 83% threshold.
    
    Returns:
        Dictionary with intervention result
    """
    result = {
        "worker_id": worker_id,
        "intervention_type": "early",
        "success": False,
        "actions_taken": []
    }
    
    if dry_run:
        result["message"] = f"[DRY RUN] Would perform early intervention for worker {worker_id}"
        result["success"] = True
        return result
    
    try:
        # Check disk I/O utilization
        disk_util = get_disk_io_utilization()
        if disk_util and disk_util >= DISK_IO_HIGH_THRESHOLD:
            logger.info(f"⚠️  Worker {worker_id} approaching stuck threshold - disk I/O is {disk_util:.1f}% (high)")
            result["actions_taken"].append(f"Detected high disk I/O: {disk_util:.1f}%")
            # Note: Actual scaling is handled by scale_workers_based_on_disk_io()
        
        # Check for error patterns
        error_patterns = warning_info.get("error_patterns", [])
        if error_patterns:
            logger.info(f"⚠️  Worker {worker_id} approaching stuck threshold - errors detected: {', '.join(error_patterns[:3])}")
            result["actions_taken"].append(f"Detected errors: {', '.join(error_patterns[:3])}")
        
        # Check worker resource usage (if possible)
        try:
            import subprocess
            result_pgrep = subprocess.run(
                ['pgrep', '-af', f'bulk_migrate_calibre.*--worker-id[[:space:]]+{worker_id}([[:space:]]|$)'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result_pgrep.returncode == 0 and result_pgrep.stdout:
                pid_match = re.search(r'^(\d+)', result_pgrep.stdout.strip())
                if pid_match:
                    pid = pid_match.group(1)
                    # Get CPU and memory usage
                    try:
                        import psutil
                        process = psutil.Process(int(pid))
                        cpu_percent = process.cpu_percent(interval=0.1)
                        memory_mb = process.memory_info().rss / 1024 / 1024
                        logger.debug(f"Worker {worker_id} resource usage: CPU={cpu_percent:.1f}%, Memory={memory_mb:.1f}MB")
                        result["actions_taken"].append(f"Resource usage: CPU={cpu_percent:.1f}%, Memory={memory_mb:.1f}MB")
                    except (ImportError, psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
        except Exception as e:
            logger.debug(f"Could not check resource usage for worker {worker_id}: {e}")
        
        # Log intervention
        logger.info(f"🔍 Early intervention for worker {worker_id}: {len(result['actions_taken'])} action(s) taken")
        result["success"] = True
        result["message"] = f"Early intervention performed: {', '.join(result['actions_taken'])}"
        
    except Exception as e:
        logger.error(f"Error performing early intervention for worker {worker_id}: {e}", exc_info=True)
        result["message"] = f"Error: {str(e)}"
    
    return result


def check_recurring_pattern_from_diagnostics(diagnostics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Check for recurring patterns based on diagnostics data (before LLM analysis).
    Analyzes ALL history entries (not just recent) to find patterns.
    Uses error patterns, book_id_range, status, and progress metrics to detect similar issues.
    
    Args:
        diagnostics: Diagnostics dictionary with error_patterns, book_id_range, etc.
    
    Returns:
        Dictionary with:
        - is_recurring: boolean
        - occurrence_count: number of times similar pattern appeared
        - last_occurrence: timestamp of last occurrence (ISO format)
        - suggest_code_fix: boolean (True if occurrence_count >= threshold)
        - pattern_summary: summary of recurring patterns
        - most_common_root_cause: most frequently occurring root cause
    """
    result = {
        "is_recurring": False,
        "occurrence_count": 0,
        "last_occurrence": None,
        "suggest_code_fix": False,
        "pattern_summary": {},
        "most_common_root_cause": None
    }
    
    if not HISTORY_FILE.exists():
        return result
    
    try:
        with open(HISTORY_FILE, 'r') as f:
            history = json.load(f)
            if not isinstance(history, list):
                history = [history]
    except Exception:
        return result
    
    error_patterns = diagnostics.get("error_patterns", [])
    book_id_range = diagnostics.get("book_id_range", "unknown")
    worker_id = diagnostics.get("worker_id")
    status = diagnostics.get("status", "unknown")
    error_rate = diagnostics.get("error_rate_per_hour", 0)
    
    # Find similar patterns in ALL history (not just recent)
    matches = []
    root_cause_counts = {}
    pattern_groups = {
        "error_pattern": [],
        "book_id_range": [],
        "status": [],
        "worker_id": []
    }
    
    for entry in history:
        entry_diagnostics = entry.get("diagnostics", {})
        entry_error_patterns = entry_diagnostics.get("error_patterns", [])
        entry_book_range = entry_diagnostics.get("book_id_range", "unknown")
        entry_worker_id = entry.get("worker_id")
        entry_status = entry_diagnostics.get("status", "unknown")
        # Handle case where llm_analysis might be None
        llm_analysis = entry.get("llm_analysis")
        entry_root_cause = entry.get("llm_root_cause") or (llm_analysis.get("root_cause", "") if llm_analysis and isinstance(llm_analysis, dict) else "")
        
        match_score = 0
        match_reasons = []
        
        # Match on similar error patterns (weight: 3)
        if error_patterns and entry_error_patterns:
            common_patterns = set(error_patterns).intersection(set(entry_error_patterns))
            if len(common_patterns) >= 1:
                match_score += len(common_patterns) * 3
                match_reasons.append(f"error_patterns({len(common_patterns)})")
                pattern_groups["error_pattern"].append(entry)
        
        # Match on same worker_id with similar book_id_range (weight: 2)
        if worker_id and entry_worker_id == worker_id:
            if book_id_range != "unknown" and entry_book_range == book_id_range:
                match_score += 2
                match_reasons.append("book_id_range")
                pattern_groups["book_id_range"].append(entry)
            
            # Also match on same worker with same status (weight: 1)
            if status == entry_status and status != "unknown":
                match_score += 1
                match_reasons.append("status")
                pattern_groups["status"].append(entry)
            
            pattern_groups["worker_id"].append(entry)
        
        # Match on similar error rate (within 50% - weight: 1)
        entry_error_rate = entry_diagnostics.get("error_rate_per_hour", 0)
        if error_rate > 0 and entry_error_rate > 0:
            rate_diff = abs(error_rate - entry_error_rate) / max(error_rate, entry_error_rate)
            if rate_diff < 0.5:  # Within 50%
                match_score += 1
                match_reasons.append("error_rate")
        
        # If match score is high enough, consider it a match
        if match_score >= 2:  # At least 2 points
            matches.append({
                "timestamp": entry.get("timestamp", ""),
                "worker_id": entry_worker_id,
                "match_type": ", ".join(match_reasons),
                "match_score": match_score,
                "root_cause": entry_root_cause
            })
            
            # Track root causes
            if entry_root_cause and entry_root_cause != "Unknown":
                root_cause_counts[entry_root_cause] = root_cause_counts.get(entry_root_cause, 0) + 1
    
    if matches:
        result["is_recurring"] = True
        result["occurrence_count"] = len(matches)
        
        # Get most recent occurrence
        matches.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        result["last_occurrence"] = matches[0].get("timestamp")
        result["suggest_code_fix"] = len(matches) >= RECURRING_ROOT_CAUSE_THRESHOLD
        
        # Build pattern summary
        result["pattern_summary"] = {
            "total_matches": len(matches),
            "by_worker": len([m for m in matches if m.get("worker_id") == worker_id]),
            "by_error_pattern": len(pattern_groups["error_pattern"]),
            "by_book_id_range": len(pattern_groups["book_id_range"]),
            "by_status": len(pattern_groups["status"]),
            "match_types": {}
        }
        
        # Count match types
        for match in matches:
            match_type = match.get("match_type", "unknown")
            result["pattern_summary"]["match_types"][match_type] = result["pattern_summary"]["match_types"].get(match_type, 0) + 1
        
        # Find most common root cause
        if root_cause_counts:
            result["most_common_root_cause"] = max(root_cause_counts.items(), key=lambda x: x[1])[0]
            result["most_common_root_cause_count"] = root_cause_counts[result["most_common_root_cause"]]
    
    return result


def hash_error_signature(diagnostics: Dict[str, Any]) -> str:
    """Create hash from error signature for caching"""
    import hashlib
    error_patterns = ','.join(sorted(diagnostics.get('error_patterns', [])))
    book_id_range = diagnostics.get('book_id_range', 'unknown')
    status = diagnostics.get('status', 'unknown')
    last_upload = str(diagnostics.get('last_upload_time', ''))
    signature = f"{error_patterns}|{book_id_range}|{status}|{last_upload}"
    return hashlib.md5(signature.encode()).hexdigest()


def check_recurring_root_cause(root_cause: str) -> Dict[str, Any]:
    """
    Check if a root cause has appeared before in the fix history.
    Analyzes ALL history entries (not just recent) to build root cause database.
    Uses fuzzy matching to detect similar root causes and tracks success rates.
    
    Args:
        root_cause: Current root cause description
    
    Returns:
        Dictionary with:
        - is_recurring: boolean
        - occurrence_count: number of times this root cause appeared
        - last_occurrence: timestamp of last occurrence (ISO format)
        - suggest_code_fix: boolean (True if occurrence_count >= threshold)
        - success_rate: success rate of fixes for this root cause (0.0-1.0)
        - time_span_days: number of days between first and last occurrence
        - root_cause_database: summary of all similar root causes
    """
    result = {
        "is_recurring": False,
        "occurrence_count": 0,
        "last_occurrence": None,
        "suggest_code_fix": False,
        "success_rate": 0.0,
        "time_span_days": 0,
        "root_cause_database": {}
    }
    
    if not root_cause or root_cause == "Unknown":
        return result
    
    if not HISTORY_FILE.exists():
        return result
    
    try:
        with open(HISTORY_FILE, 'r') as f:
            history = json.load(f)
            if not isinstance(history, list):
                history = [history]
    except Exception:
        return result
    
    # Normalize root cause for comparison
    def normalize_text(text: str) -> set:
        """Extract keywords from text for fuzzy matching"""
        if not text:
            return set()
        # Lowercase, remove punctuation, split into words
        normalized = re.sub(r'[^\w\s]', ' ', text.lower())
        # Extract meaningful words (length > 2, not common stop words)
        stop_words = {'the', 'is', 'at', 'which', 'on', 'a', 'an', 'as', 'are', 'was', 'were', 'been', 'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'should', 'could', 'may', 'might', 'must', 'can', 'this', 'that', 'these', 'those', 'to', 'of', 'in', 'for', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during', 'including', 'against', 'among', 'throughout', 'despite', 'towards', 'upon', 'concerning'}
        words = [w for w in normalized.split() if len(w) > 2 and w not in stop_words]
        return set(words)
    
    current_keywords = normalize_text(root_cause)
    if not current_keywords:
        return result
    
    # Find similar root causes in ALL history (not just recent)
    matches = []
    root_cause_stats = {}  # Track stats per unique root cause
    
    for entry in history:
        historical_root_cause = entry.get("llm_root_cause") or entry.get("llm_analysis", {}).get("root_cause", "")
        if not historical_root_cause or historical_root_cause == "Unknown":
            continue
        
        historical_keywords = normalize_text(historical_root_cause)
        if not historical_keywords:
            continue
        
        # Check if they share at least 3 keywords (fuzzy matching)
        common_keywords = current_keywords.intersection(historical_keywords)
        if len(common_keywords) >= 3:
            # Calculate similarity score
            similarity = len(common_keywords) / max(len(current_keywords), len(historical_keywords))
            
            matches.append({
                "root_cause": historical_root_cause,
                "timestamp": entry.get("timestamp", ""),
                "common_keywords": len(common_keywords),
                "similarity": similarity,
                "worker_id": entry.get("worker_id"),
                "fix_type": entry.get("fix_type", "unknown"),
                "success": entry.get("success", False),
                "verified_success": entry.get("verified_success", False)
            })
            
            # Track stats per root cause
            if historical_root_cause not in root_cause_stats:
                root_cause_stats[historical_root_cause] = {
                    "count": 0,
                    "success_count": 0,
                    "verified_success_count": 0,
                    "fix_types": {},
                    "worker_ids": set(),
                    "timestamps": []
                }
            
            stats = root_cause_stats[historical_root_cause]
            stats["count"] += 1
            if entry.get("success", False):
                stats["success_count"] += 1
            if entry.get("verified_success", False):
                stats["verified_success_count"] += 1
            
            fix_type = entry.get("fix_type", "unknown")
            stats["fix_types"][fix_type] = stats["fix_types"].get(fix_type, 0) + 1
            
            worker_id = entry.get("worker_id")
            if worker_id:
                stats["worker_ids"].add(worker_id)
            
            timestamp = entry.get("timestamp", "")
            if timestamp:
                try:
                    ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00').split('+')[0])
                    stats["timestamps"].append(ts)
                except:
                    pass
    
    if matches:
        result["is_recurring"] = True
        result["occurrence_count"] = len(matches)
        
        # Get most recent occurrence
        matches.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        result["last_occurrence"] = matches[0].get("timestamp")
        result["suggest_code_fix"] = len(matches) >= RECURRING_ROOT_CAUSE_THRESHOLD
        
        # Calculate success rate
        total_successes = sum(1 for m in matches if m.get("success", False))
        result["success_rate"] = total_successes / len(matches) if matches else 0.0
        
        # Calculate time span
        timestamps = [datetime.fromisoformat(m["timestamp"].replace('Z', '+00:00').split('+')[0]) 
                     for m in matches if m.get("timestamp")]
        if len(timestamps) >= 2:
            time_span = max(timestamps) - min(timestamps)
            result["time_span_days"] = time_span.days
        
        # Build root cause database
        result["root_cause_database"] = {}
        for root_cause_text, stats in root_cause_stats.items():
            result["root_cause_database"][root_cause_text] = {
                "occurrence_count": stats["count"],
                "success_rate": stats["success_count"] / stats["count"] if stats["count"] > 0 else 0.0,
                "verified_success_rate": stats["verified_success_count"] / stats["count"] if stats["count"] > 0 else 0.0,
                "fix_types": stats["fix_types"],
                "affected_workers": len(stats["worker_ids"]),
                "worker_ids": list(stats["worker_ids"])
            }
    
    return result


def calculate_worker_health(worker_id: int) -> Dict[str, Any]:
    """
    Calculate worker health score (0-100) based on multiple metrics.
    
    Returns:
        Dictionary with:
        - health_score: 0-100 (higher is better)
        - health_level: "healthy", "warning", or "critical"
        - metrics: individual metric scores
        - trend: "improving", "declining", or "stable"
    """
    result = {
        "worker_id": worker_id,
        "health_score": 50,  # Default to middle
        "health_level": "warning",
        "metrics": {},
        "trend": "stable"
    }
    
    try:
        # Get worker status
        log_stats = get_worker_log_stats(worker_id)
        last_upload = get_last_upload_time(worker_id)
        logs = get_worker_logs(worker_id, lines=500)
        
        # Calculate metrics with weights
        metrics = {}
        
        # 1. Upload rate (30% weight)
        upload_rate_score = 0
        if last_upload:
            time_since_upload = (datetime.now() - last_upload).total_seconds() / 3600  # hours
            # Score based on time since last upload
            if time_since_upload < 0.5:  # Less than 30 minutes
                upload_rate_score = 100
            elif time_since_upload < 1.0:  # Less than 1 hour
                upload_rate_score = 80
            elif time_since_upload < 2.0:  # Less than 2 hours
                upload_rate_score = 60
            elif time_since_upload < 4.0:  # Less than 4 hours
                upload_rate_score = 40
            else:
                upload_rate_score = 20
        else:
            # No uploads yet - check if worker is in discovery
            status = log_stats.get("status", "unknown")
            if status in ["initializing", "discovering"]:
                upload_rate_score = 50  # Neutral for discovery phase
            else:
                upload_rate_score = 30  # Low if not in discovery and no uploads
        
        # Calculate files per hour from logs
        upload_timestamps = []
        for line in logs.split('\n'):
            if re.search(r'Successfully uploaded|Uploaded:', line, re.IGNORECASE):
                timestamp = parse_log_timestamp(line)
                if timestamp:
                    upload_timestamps.append(timestamp)
        
        files_per_hour = 0
        if len(upload_timestamps) >= 2:
            time_span = (upload_timestamps[-1] - upload_timestamps[0]).total_seconds() / 3600
            if time_span > 0:
                files_per_hour = len(upload_timestamps) / time_span
        
        metrics["upload_rate"] = {
            "score": upload_rate_score,
            "files_per_hour": files_per_hour,
            "time_since_upload_hours": (datetime.now() - last_upload).total_seconds() / 3600 if last_upload else None
        }
        
        # 2. Error rate (25% weight)
        error_count = len(re.findall(r'ERROR|Exception|Failed|Traceback', logs, re.IGNORECASE))
        log_lines = len(logs.split('\n'))
        estimated_hours = max(log_lines / 3600, 0.1)
        errors_per_hour = error_count / estimated_hours
        
        # Score based on error rate (lower is better)
        if errors_per_hour == 0:
            error_rate_score = 100
        elif errors_per_hour < 1:
            error_rate_score = 80
        elif errors_per_hour < 5:
            error_rate_score = 60
        elif errors_per_hour < 10:
            error_rate_score = 40
        else:
            error_rate_score = 20
        
        metrics["error_rate"] = {
            "score": error_rate_score,
            "errors_per_hour": errors_per_hour,
            "total_errors": error_count
        }
        
        # 3. Progress rate (25% weight)
        _, progress_metrics = check_worker_no_progress(worker_id, logs)
        progress_score = min(progress_metrics.get("total_score", 0) * 10, 100)  # Scale to 0-100
        
        metrics["progress_rate"] = {
            "score": progress_score,
            "progress_indicators": progress_metrics
        }
        
        # 4. Time since last activity (20% weight)
        last_activity = log_stats.get("last_activity_time")
        activity_score = 100
        if last_activity:
            time_since_activity = (datetime.now() - last_activity).total_seconds() / 3600  # hours
            if time_since_activity < 0.5:
                activity_score = 100
            elif time_since_activity < 1.0:
                activity_score = 80
            elif time_since_activity < 2.0:
                activity_score = 60
            elif time_since_activity < 4.0:
                activity_score = 40
            else:
                activity_score = 20
        else:
            activity_score = 30  # No activity recorded
        
        metrics["last_activity"] = {
            "score": activity_score,
            "hours_since_activity": (datetime.now() - last_activity).total_seconds() / 3600 if last_activity else None
        }
        
        # Calculate weighted health score
        weights = {
            "upload_rate": 0.30,
            "error_rate": 0.25,
            "progress_rate": 0.25,
            "last_activity": 0.20
        }
        
        health_score = (
            upload_rate_score * weights["upload_rate"] +
            error_rate_score * weights["error_rate"] +
            progress_score * weights["progress_rate"] +
            activity_score * weights["last_activity"]
        )
        
        # Determine health level
        if health_score >= 80:
            health_level = "healthy"
        elif health_score >= 50:
            health_level = "warning"
        else:
            health_level = "critical"
        
        # Calculate trend (compare to previous health scores)
        trend = "stable"
        if worker_id in worker_health_history:
            history = worker_health_history[worker_id]
            if len(history) >= 2:
                recent_scores = [h["health_score"] for h in history[-5:]]  # Last 5 scores
                if len(recent_scores) >= 2:
                    avg_recent = sum(recent_scores) / len(recent_scores)
                    # Get older scores (before recent ones)
                    older_entries = history[:-len(recent_scores)][-5:] if len(history) > len(recent_scores) else []
                    if older_entries:
                        older_scores = [h["health_score"] for h in older_entries]
                        avg_older = sum(older_scores) / len(older_scores)
                    else:
                        avg_older = avg_recent
                    if avg_recent > avg_older + 5:
                        trend = "improving"
                    elif avg_recent < avg_older - 5:
                        trend = "declining"
        
        result["health_score"] = round(health_score, 1)
        result["health_level"] = health_level
        result["metrics"] = metrics
        result["trend"] = trend
        
        # Store in history (keep last 20 entries)
        if worker_id not in worker_health_history:
            worker_health_history[worker_id] = []
        worker_health_history[worker_id].append({
            "timestamp": datetime.now().isoformat(),
            "health_score": result["health_score"],
            "health_level": health_level
        })
        if len(worker_health_history[worker_id]) > 20:
            worker_health_history[worker_id] = worker_health_history[worker_id][-20:]
        
    except Exception as e:
        logger.error(f"Error calculating health for worker {worker_id}: {e}", exc_info=True)
        result["health_level"] = "unknown"
        result["error"] = str(e)
    
    return result


def auto_fix_worker(worker_id: int, diagnostics: Dict[str, Any], llm_enabled: bool = False, dry_run: bool = False) -> Dict[str, Any]:
    """
    Automatically fix a stuck worker.
    
    Returns:
        Dictionary with fix result
    """
    # Check if worker is paused (after max attempts)
    if worker_id in paused_workers:
        logger.warning(f"Worker {worker_id} is paused (max fix attempts reached). Skipping.")
        return {
            "worker_id": worker_id,
            "success": False,
            "message": "Worker paused after max fix attempts",
            "escalated": True
        }
    
    # Check cooldown
    if worker_id in worker_last_fix_time:
        time_since_last_fix = (datetime.now() - worker_last_fix_time[worker_id]).total_seconds()
        if time_since_last_fix < COOLDOWN_SECONDS:
            remaining = int((COOLDOWN_SECONDS - time_since_last_fix) / 60)
            logger.info(f"Worker {worker_id} in cooldown period ({remaining} minutes remaining)")
            return {
                "worker_id": worker_id,
                "success": False,
                "message": f"In cooldown period ({remaining} minutes remaining)"
            }
    
    # Check if we've exceeded max fix attempts
    attempt_count = get_fix_attempt_count(worker_id, within_hours=24)
    if attempt_count >= MAX_FIX_ATTEMPTS:
        logger.error(f"⚠️  Worker {worker_id} has exceeded max fix attempts ({attempt_count}/{MAX_FIX_ATTEMPTS})")
        logger.error(f"   Escalation action: {ESCALATION_ACTION}")
        
        # Handle escalation
        if ESCALATION_ACTION == "alert_and_pause":
            paused_workers.add(worker_id)
            logger.error(f"   Worker {worker_id} PAUSED - will not attempt further fixes")
            logger.error(f"   Manual intervention required!")
        elif ESCALATION_ACTION == "stop_worker":
            # Stop the worker entirely
            try:
                import subprocess
                subprocess.run(["pkill", "-9", "-f", f"bulk_migrate_calibre.*--worker-id[[:space:]]+{worker_id}([[:space:]]|$)"], 
                             timeout=10)
                logger.error(f"   Worker {worker_id} STOPPED")
            except Exception as e:
                logger.error(f"   Failed to stop worker {worker_id}: {e}")
        
        return {
            "worker_id": worker_id,
            "success": False,
            "message": f"Max fix attempts ({MAX_FIX_ATTEMPTS}) exceeded",
            "escalated": True,
            "escalation_action": ESCALATION_ACTION
        }
    
    # Check for recurring patterns from diagnostics before LLM analysis
    # This helps provide context to LLM about recurring issues
    preliminary_recurring_info = check_recurring_pattern_from_diagnostics(diagnostics)
    if preliminary_recurring_info["is_recurring"]:
        logger.info(f"⚠️  Preliminary recurring pattern detected for worker {worker_id}: "
                  f"appeared {preliminary_recurring_info['occurrence_count']} time(s) before")
        if preliminary_recurring_info["suggest_code_fix"]:
            logger.info(f"   Suggesting code_fix (threshold: {RECURRING_ROOT_CAUSE_THRESHOLD} occurrences)")
        
        # Add preliminary recurring info to diagnostics for LLM context
        diagnostics["recurring_root_cause"] = preliminary_recurring_info["is_recurring"]
        diagnostics["root_cause_occurrence_count"] = preliminary_recurring_info["occurrence_count"]
        diagnostics["suggest_code_fix_for_recurring"] = preliminary_recurring_info["suggest_code_fix"]
    
    # We'll check again after LLM analysis with the actual root cause
    recurring_info = None
    
    # Use LLM to analyze if enabled
    llm_analysis = None
    if llm_enabled:
        # Check cache first
        cache_key = (worker_id, hash_error_signature(diagnostics))
        if cache_key in llm_cache:
            cached_time, cached_result = llm_cache[cache_key]
            time_since_cache = (datetime.now() - cached_time).total_seconds()
            if time_since_cache < 900:  # 15 minutes
                logger.debug(f"Using cached LLM analysis for worker {worker_id} (cached {int(time_since_cache/60)} min ago)")
                llm_analysis = cached_result
            else:
                # Cache expired, remove and continue
                del llm_cache[cache_key]
                llm_analysis = None
        
        # Check if recently analyzed (within cooldown period)
        if llm_analysis is None and worker_id in worker_last_llm_analysis:
            time_since_analysis = (datetime.now() - worker_last_llm_analysis[worker_id]).total_seconds()
            if time_since_analysis < 600:  # 10 minutes
                logger.debug(f"Worker {worker_id} analyzed {int(time_since_analysis/60)} min ago, skipping LLM (in cooldown)")
                llm_analysis = None
        
        # If no cached result, analyze with LLM
        if llm_analysis is None:
            logger.info(f"Analyzing worker {worker_id} with LLM...")
            llm_analysis = analyze_worker_with_llm(
                worker_id,
                diagnostics["logs"],
                diagnostics
            )
            # Store in cache and track analysis time
            if llm_analysis:
                llm_cache[cache_key] = (datetime.now(), llm_analysis)
                worker_last_llm_analysis[worker_id] = datetime.now()
        
        # Check for recurring root cause after LLM analysis
        if llm_analysis:
            root_cause = llm_analysis.get('root_cause', 'Unknown')
            if root_cause and root_cause != "Unknown":
                recurring_info = check_recurring_root_cause(root_cause)
                
                # Merge with preliminary recurring info (use the higher occurrence count)
                if preliminary_recurring_info["is_recurring"]:
                    if recurring_info["is_recurring"]:
                        # Use the maximum occurrence count from both
                        recurring_info["occurrence_count"] = max(
                            recurring_info["occurrence_count"],
                            preliminary_recurring_info["occurrence_count"]
                        )
                        recurring_info["suggest_code_fix"] = recurring_info["occurrence_count"] >= RECURRING_ROOT_CAUSE_THRESHOLD
                    else:
                        # Use preliminary info if LLM-based check didn't find recurrence
                        recurring_info = preliminary_recurring_info
                
                if recurring_info["is_recurring"]:
                    logger.info(f"⚠️  Recurring root cause detected for worker {worker_id}: "
                              f"appeared {recurring_info['occurrence_count']} time(s) before")
                    logger.info(f"   Root cause: {root_cause}")
                    logger.info(f"   Success rate: {recurring_info.get('success_rate', 0.0):.1%}")
                    if recurring_info.get('time_span_days', 0) > 0:
                        logger.info(f"   Time span: {recurring_info['time_span_days']} days")
                    if recurring_info["suggest_code_fix"]:
                        logger.info(f"   Suggesting code_fix (threshold: {RECURRING_ROOT_CAUSE_THRESHOLD} occurrences)")
                    
                    # Log root cause database summary
                    root_cause_db = recurring_info.get("root_cause_database", {})
                    if root_cause_db:
                        logger.info(f"   Root cause database: {len(root_cause_db)} similar root cause(s) found")
                        for rc_text, rc_stats in list(root_cause_db.items())[:3]:  # Show top 3
                            logger.info(f"     - '{rc_text[:60]}...': {rc_stats['occurrence_count']} occurrences, "
                                      f"{rc_stats['success_rate']:.1%} success rate, "
                                      f"{rc_stats['affected_workers']} worker(s)")
                
                # Add recurring info to diagnostics for potential re-analysis
                diagnostics["recurring_root_cause"] = recurring_info["is_recurring"]
                diagnostics["root_cause_occurrence_count"] = recurring_info["occurrence_count"]
                diagnostics["suggest_code_fix_for_recurring"] = recurring_info["suggest_code_fix"]
                diagnostics["root_cause_keywords"] = root_cause
            else:
                # If no root cause from LLM, use preliminary recurring info
                if preliminary_recurring_info["is_recurring"]:
                    recurring_info = preliminary_recurring_info
        
        if llm_analysis:
            root_cause = llm_analysis.get('root_cause', 'Unknown')
            fix_type = llm_analysis.get('fix_type', 'restart')
            confidence = llm_analysis.get('confidence', 0.0)
            fix_description = llm_analysis.get('fix_description', '')
            
            logger.info(f"🤖 LLM Analysis Complete for Worker {worker_id}:")
            logger.info(f"   Root Cause: {root_cause}")
            logger.info(f"   Recommended Fix: {fix_type}")
            logger.info(f"   Confidence: {confidence:.2f}")
            if fix_description:
                logger.info(f"   Fix Description: {fix_description[:200]}..." if len(fix_description) > 200 else f"   Fix Description: {fix_description}")
            
            # Log code changes if it's a code fix
            if fix_type == "code_fix":
                code_changes = llm_analysis.get("code_changes", "")
                if code_changes:
                    logger.info(f"   Code Changes: {len(code_changes)} characters")
                    # Log first few lines of code changes
                    code_lines = code_changes.split('\n')[:5]
                    for i, line in enumerate(code_lines, 1):
                        logger.info(f"      {i}: {line[:100]}..." if len(line) > 100 else f"      {i}: {line}")
                    if len(code_changes.split('\n')) > 5:
                        logger.info(f"      ... ({len(code_changes.split('\n')) - 5} more lines)")
            
            # Log config changes if it's a config fix
            elif fix_type == "config_fix":
                config_changes = llm_analysis.get("config_changes", {})
                if config_changes:
                    logger.info(f"   Config Changes: {config_changes}")
            
            # Override fix_type if recurring issue suggests code_fix but LLM recommended restart
            if recurring_info and recurring_info.get("suggest_code_fix") and llm_analysis:
                if llm_analysis.get("fix_type") == "restart":
                    logger.warning(f"⚠️  Overriding LLM recommendation: recurring issue detected (occurred {recurring_info['occurrence_count']} times), forcing code_fix")
                    llm_analysis["fix_type"] = "code_fix"
                    # If LLM didn't provide code_changes, we'll need to handle this in the fix application logic
                    if not llm_analysis.get("code_changes"):
                        logger.warning(f"   ⚠️  LLM did not provide code_changes. Will attempt to re-analyze or use fallback.")
    
    # Determine fix type
    if llm_analysis and llm_analysis.get("fix_type") == "code_fix":
        # Check if code_changes are missing (e.g., after override)
        if not llm_analysis.get("code_changes") or not llm_analysis.get("code_changes", "").strip():
            if recurring_info and recurring_info.get("suggest_code_fix") and llm_enabled:
                # Re-analyze with stronger prompt emphasizing code_fix requirement
                logger.warning(f"⚠️  Code changes missing after override. Re-analyzing with code_fix requirement...")
                # Update diagnostics with recurring info for re-analysis
                diagnostics["recurring_root_cause"] = recurring_info["is_recurring"]
                diagnostics["root_cause_occurrence_count"] = recurring_info["occurrence_count"]
                diagnostics["suggest_code_fix_for_recurring"] = True
                diagnostics["root_cause_keywords"] = llm_analysis.get("root_cause", "Unknown")
                diagnostics["code_fix_required"] = True  # Explicit flag for re-analysis
                diagnostics["previous_root_cause"] = llm_analysis.get("root_cause", "Unknown")
                
                # Re-analyze with updated context (bypass cache for re-analysis)
                re_analysis = analyze_worker_with_llm(
                    worker_id,
                    diagnostics["logs"],
                    diagnostics
                )
                
                if re_analysis and re_analysis.get("code_changes"):
                    logger.info(f"✅ Re-analysis provided code changes")
                    llm_analysis["code_changes"] = re_analysis.get("code_changes", "")
                    llm_analysis["fix_description"] = re_analysis.get("fix_description", llm_analysis.get("fix_description", ""))
                else:
                    logger.error(f"❌ Re-analysis failed to provide code changes. Falling back to restart.")
                    # Fall back to restart if we can't get code changes
                    llm_analysis["fix_type"] = "restart"
            else:
                logger.error(f"❌ Code fix requested but no code_changes provided and cannot re-analyze. Falling back to restart.")
                llm_analysis["fix_type"] = "restart"
        
        # Apply code fix if we have code_changes
        if llm_analysis.get("fix_type") == "code_fix":
            code_changes = llm_analysis.get("code_changes", "")
            
            # Validate code_changes before applying
            validation_errors = []
            if not code_changes or not code_changes.strip():
                validation_errors.append("Code changes are empty")
            elif len(code_changes.strip()) < 20:
                validation_errors.append(f"Code changes too short ({len(code_changes)} chars)")
            elif not any(keyword in code_changes for keyword in ['def ', 'if ', 'for ', '=', 'return', 'import', 'class ']):
                validation_errors.append("Code changes don't contain code keywords (may be just description)")
            
            if validation_errors:
                logger.error(f"❌ Code fix validation failed for worker {worker_id}: {', '.join(validation_errors)}")
                logger.error(f"   Code changes preview: {code_changes[:200]}...")
                # Request re-analysis with stricter prompt
                if llm_enabled and not dry_run:
                    logger.warning(f"⚠️  Requesting re-analysis with stricter code fix requirements...")
                    diagnostics["code_fix_required"] = True
                    diagnostics["code_fix_validation_failed"] = True
                    diagnostics["validation_errors"] = validation_errors
                    diagnostics["previous_code_changes"] = code_changes[:500]  # Store preview for context
                    
                    re_analysis = analyze_worker_with_llm(
                        worker_id,
                        diagnostics["logs"],
                        diagnostics
                    )
                    
                    if re_analysis and re_analysis.get("code_changes"):
                        # Validate the re-analysis result
                        new_code_changes = re_analysis.get("code_changes", "")
                        if new_code_changes and len(new_code_changes.strip()) >= 20:
                            logger.info(f"✅ Re-analysis provided valid code changes")
                            llm_analysis["code_changes"] = new_code_changes
                            code_changes = new_code_changes
                        else:
                            logger.error(f"❌ Re-analysis still failed validation. Falling back to restart.")
                            llm_analysis["fix_type"] = "restart"
                    else:
                        logger.error(f"❌ Re-analysis failed. Falling back to restart.")
                        llm_analysis["fix_type"] = "restart"
                else:
                    # Can't re-analyze, fall back to restart
                    logger.error(f"❌ Cannot re-analyze (llm_enabled={llm_enabled}, dry_run={dry_run}). Falling back to restart.")
                    llm_analysis["fix_type"] = "restart"
            
            # Apply code fix if validation passed
            if llm_analysis.get("fix_type") == "code_fix":
                logger.info(f"🔧 Applying LLM Code Fix for Worker {worker_id}...")
                fix_result = apply_code_fix(
                    llm_analysis.get("fix_description", ""),
                    code_changes,
                    dry_run
                )
            # Add LLM details to fix result
            fix_result["llm_root_cause"] = llm_analysis.get("root_cause", "Unknown")
            fix_result["llm_confidence"] = llm_analysis.get("confidence", 0.0)
            fix_result["llm_code_changes"] = llm_analysis.get("code_changes", "")
            # Add recurring root cause info
            if recurring_info:
                fix_result["recurring_root_cause"] = recurring_info["is_recurring"]
                fix_result["root_cause_occurrence_count"] = recurring_info["occurrence_count"]
        elif llm_analysis.get("fix_type") == "restart":
            # Fallback to restart (e.g., after failed code_fix override)
            logger.info(f"🔄 Applying Restart (fallback after code_fix attempt failed) for Worker {worker_id}...")
            fix_result = apply_restart(worker_id, parallel_uploads=1, dry_run=dry_run)
            fix_result["llm_root_cause"] = llm_analysis.get("root_cause", "Unknown")
            fix_result["llm_confidence"] = llm_analysis.get("confidence", 0.0)
            if recurring_info:
                fix_result["recurring_root_cause"] = recurring_info["is_recurring"]
                fix_result["root_cause_occurrence_count"] = recurring_info["occurrence_count"]
    elif llm_analysis and llm_analysis.get("fix_type") == "config_fix":
        # Apply config fix
        logger.info(f"🔧 Applying LLM Config Fix for Worker {worker_id}...")
        config_changes = llm_analysis.get("config_changes", {"parallel_uploads": 1})
        if not config_changes:
            config_changes = {"parallel_uploads": 1}  # Default safe value
        fix_result = apply_config_fix(worker_id, config_changes, dry_run)
        # Add LLM details to fix result
        fix_result["llm_root_cause"] = llm_analysis.get("root_cause", "Unknown")
        fix_result["llm_confidence"] = llm_analysis.get("confidence", 0.0)
        fix_result["llm_config_changes"] = config_changes
        # Add recurring root cause info
        if recurring_info:
            fix_result["recurring_root_cause"] = recurring_info["is_recurring"]
            fix_result["root_cause_occurrence_count"] = recurring_info["occurrence_count"]
    else:
        # Default: restart worker
        if llm_analysis:
            logger.info(f"🔄 Applying LLM Recommended Restart for Worker {worker_id}...")
        else:
            logger.info(f"🔄 Applying Default Restart for Worker {worker_id}...")
        fix_result = apply_restart(worker_id, parallel_uploads=1, dry_run=dry_run)
        if llm_analysis:
            fix_result["llm_root_cause"] = llm_analysis.get("root_cause", "Unknown")
            fix_result["llm_confidence"] = llm_analysis.get("confidence", 0.0)
            # Add recurring root cause info
            if recurring_info:
                fix_result["recurring_root_cause"] = recurring_info["is_recurring"]
                fix_result["root_cause_occurrence_count"] = recurring_info["occurrence_count"]
    
    # Record fix attempt
    fix_time = datetime.now()
    if worker_id not in worker_fix_attempts:
        worker_fix_attempts[worker_id] = []
    
    worker_fix_attempts[worker_id].append({
        "timestamp": fix_time.isoformat(),
        "success": fix_result.get("success", False),
        "fix_type": fix_result.get("fix_type", "restart"),
        "message": fix_result.get("message", "")
    })
    
    # Keep only last 10 attempts per worker
    if len(worker_fix_attempts[worker_id]) > 10:
        worker_fix_attempts[worker_id] = worker_fix_attempts[worker_id][-10:]
    
    # Update cooldown
    if fix_result.get("success"):
        worker_last_fix_time[worker_id] = fix_time
        
        # Verify fix success after a short delay
        if not dry_run:
            logger.info(f"Verifying fix success for worker {worker_id}...")
            success = verify_fix_success(worker_id, fix_time)
            if success:
                logger.info(f"✅ Worker {worker_id} fix verified successful - worker recovered")
                # Reset attempt count on successful fix
                if worker_id in worker_fix_attempts:
                    worker_fix_attempts[worker_id] = []
                if worker_id in paused_workers:
                    paused_workers.remove(worker_id)
            else:
                logger.warning(f"⚠️  Worker {worker_id} fix applied but worker still stuck")
                fix_result["verified_success"] = False
        else:
            fix_result["verified_success"] = None  # Not verified in dry-run
    else:
        fix_result["verified_success"] = False
    
    # Add diagnostics and attempt count to result
    fix_result["diagnostics"] = diagnostics
    fix_result["llm_analysis"] = llm_analysis
    fix_result["attempt_count"] = get_fix_attempt_count(worker_id, within_hours=24)
    fix_result["max_attempts"] = MAX_FIX_ATTEMPTS
    
    # Enhanced logging for LLM fixes
    if llm_analysis:
        fix_result["llm_applied"] = True
        fix_result["llm_root_cause"] = llm_analysis.get("root_cause", "Unknown")
        fix_result["llm_confidence"] = llm_analysis.get("confidence", 0.0)
        fix_result["llm_fix_description"] = llm_analysis.get("fix_description", "")
        
        # Log comprehensive LLM fix summary
        logger.info("=" * 80)
        logger.info(f"📋 LLM Fix Summary for Worker {worker_id}:")
        logger.info(f"   Root Cause: {llm_analysis.get('root_cause', 'Unknown')}")
        logger.info(f"   Fix Type: {fix_result.get('fix_type', 'unknown')}")
        logger.info(f"   Confidence: {llm_analysis.get('confidence', 0.0):.2f}")
        logger.info(f"   Fix Success: {fix_result.get('success', False)}")
        
        if fix_result.get("fix_type") == "code_fix":
            code_changes = llm_analysis.get("code_changes", "")
            if code_changes:
                logger.info(f"   Code Changes Size: {len(code_changes)} characters")
                logger.info(f"   Code Changes Preview:")
                for i, line in enumerate(code_changes.split('\n')[:10], 1):
                    logger.info(f"      {i:2d}: {line[:100]}")
                if len(code_changes.split('\n')) > 10:
                    logger.info(f"      ... ({len(code_changes.split('\n')) - 10} more lines)")
            if fix_result.get("changes_applied"):
                logger.info(f"   Changes Applied: {fix_result.get('changes_applied')}")
        
        elif fix_result.get("fix_type") == "config_fix":
            config_changes = fix_result.get("llm_config_changes") or llm_analysis.get("config_changes", {})
            if config_changes:
                logger.info(f"   Config Changes: {config_changes}")
        
        logger.info(f"   Fix Message: {fix_result.get('message', 'N/A')}")
        logger.info("=" * 80)
    else:
        fix_result["llm_applied"] = False
    
    # Save to history
    try:
        save_fix_to_history(fix_result)
        # Verify entry was saved (only for non-dry-run)
        if not dry_run:
            verify_history_entry(fix_result)
    except Exception as e:
        logger.error(f"Failed to save fix to history for worker {worker_id}: {e}", exc_info=True)
        # Don't fail the fix if history save fails, but log it
    
    return fix_result


def get_expected_worker_ids() -> Set[int]:
    """
    Get IDs of workers that should be running (have progress files).
    Workers with progress files are expected to be running.
    """
    expected_workers = set()
    try:
        import glob
        from pathlib import Path
        
        # Check for progress files
        progress_files = glob.glob(str(WORKER_LOG_DIR / "migration_progress_worker*.json"))
        for file_path in progress_files:
            try:
                # Extract worker ID from filename: migration_progress_worker2.json -> 2
                filename = Path(file_path).name
                if "worker" in filename:
                    parts = filename.split("worker")
                    if len(parts) > 1:
                        worker_id_str = parts[1].split(".")[0]
                        worker_id = int(worker_id_str)
                        expected_workers.add(worker_id)
            except (ValueError, IndexError):
                continue
    except Exception as e:
        logger.debug(f"Error getting expected workers: {e}")
    
    return expected_workers


def get_disk_io_utilization() -> Optional[float]:
    """
    Get disk I/O utilization percentage for the Calibre library disk.
    Returns None if unable to determine.
    """
    try:
        import subprocess
        import psutil
        
        # Find the disk where Calibre library is mounted
        calibre_path = CALIBRE_LIBRARY_PATH
        device_name = None
        
        # Use df command to get accurate device for the path
        try:
            result = subprocess.run(
                ['df', calibre_path],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                if len(lines) > 1:
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
                if calibre_path.startswith(partition.mountpoint):
                    calibre_disk = partition.device
                    device_name = calibre_disk.split('/')[-1].rstrip('0123456789')
                    break
        
        if not device_name:
            return None
        
        # Get disk utilization from iostat
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
                util_col_idx = None
                for i, line in enumerate(lines):
                    if 'Device' in line and '%util' in line:
                        header_parts = line.split()
                        for idx, col in enumerate(header_parts):
                            if col == '%util':
                                util_col_idx = idx
                                break
                        break
                
                # Find the line with device name and extract %util
                # iostat outputs 2 samples: first is since boot, second is current
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
                                return util
                        except ValueError:
                            pass
        except Exception:
            pass
        
        return None
    except Exception as e:
        logger.debug(f"Error getting disk I/O utilization: {e}")
        return None


def kill_worker(worker_id: int) -> bool:
    """
    Kill a worker process.
    Returns True if successful, False otherwise.
    """
    try:
        import subprocess
        
        # Use pkill to kill the worker
        result = subprocess.run(
            ['pkill', '-9', '-f', f'bulk_migrate_calibre.*--worker-id[[:space:]]+{worker_id}([[:space:]]|$)'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        # Wait a moment and verify it's gone
        time.sleep(1)
        running_workers = get_running_worker_ids()
        
        if worker_id not in running_workers:
            logger.info(f"✅ Worker {worker_id} killed successfully")
            return True
        else:
            logger.warning(f"⚠️  Worker {worker_id} still running after kill attempt")
            return False
            
    except Exception as e:
        logger.error(f"Error killing worker {worker_id}: {e}", exc_info=True)
        return False


def scale_workers_based_on_disk_io(llm_enabled: bool = False, dry_run: bool = False):
    """
    Adjust worker count based on disk I/O utilization.
    Uses LLM to analyze if disk I/O is the root cause of worker issues.
    """
    global desired_worker_count, last_scale_down_time, last_scale_up_time
    
    try:
        # Get current disk I/O utilization
        disk_util = get_disk_io_utilization()
        
        if disk_util is None:
            logger.debug("Could not determine disk I/O utilization, skipping scaling")
            return
        
        # Get current running workers
        running_workers = get_running_worker_ids()
        current_count = len(running_workers)
        
        logger.debug(f"Disk I/O utilization: {disk_util:.1f}%, Current workers: {current_count}, Desired: {desired_worker_count}")
        
        # Check if disk is saturated
        if disk_util >= DISK_IO_SATURATED_THRESHOLD:
            # Disk is saturated - only scale down if workers are stuck AND disk I/O is the root cause
            if current_count > MIN_WORKER_COUNT:
                # Check cooldown
                if last_scale_down_time:
                    time_since_scale_down = (datetime.now() - last_scale_down_time).total_seconds()
                    if time_since_scale_down < DISK_IO_SCALE_DOWN_COOLDOWN:
                        remaining = int((DISK_IO_SCALE_DOWN_COOLDOWN - time_since_scale_down) / 60)
                        logger.debug(f"Scale-down cooldown active ({remaining} min remaining)")
                        return
                
                # First, check if there are any stuck workers
                stuck_workers = []
                for worker_id in running_workers:
                    diagnostics = check_worker_stuck(worker_id, STUCK_THRESHOLD_SECONDS, llm_enabled=False)
                    if diagnostics:
                        stuck_workers.append((worker_id, diagnostics))
                
                if not stuck_workers:
                    # No stuck workers - do not scale down even if disk I/O is saturated
                    logger.debug(f"Disk I/O {disk_util:.1f}% saturated, but no stuck workers - not scaling down")
                    return
                
                # Skip LLM analysis - when disk I/O is >= 90% and workers are stuck, disk I/O is clearly the root cause
                # Fallback logic already handles this correctly, so LLM analysis is redundant
                should_scale_down = True
                logger.info(f"⚠️  Disk I/O {disk_util:.1f}% saturated and {len(stuck_workers)} worker(s) stuck - scaling down (skipping LLM, disk I/O clearly saturated)")
                
                if should_scale_down:
                    # Scale down by 1 worker
                    new_desired_count = max(MIN_WORKER_COUNT, current_count - 1)
                    desired_worker_count = new_desired_count
                    
                    if new_desired_count < current_count:
                        logger.warning(f"📉 Scaling DOWN: Disk I/O {disk_util:.1f}% (saturated) - reducing workers from {current_count} to {new_desired_count}")
                        
                        if not dry_run:
                            # Kill the worker with highest ID (least priority)
                            worker_to_kill = max(running_workers)
                            if kill_worker(worker_to_kill):
                                last_scale_down_time = datetime.now()
                                logger.info(f"✅ Scaled down: Killed worker {worker_to_kill}")
                            else:
                                logger.warning(f"⚠️  Failed to kill worker {worker_to_kill} for scaling down")
                        else:
                            logger.info(f"[DRY RUN] Would kill worker {max(running_workers)} to scale down")
        
        elif disk_util < DISK_IO_NORMAL_THRESHOLD:
            # Disk I/O is normal - consider scaling up
            if current_count < desired_worker_count and desired_worker_count < MAX_WORKER_COUNT:
                # Check cooldown
                if last_scale_up_time:
                    time_since_scale_up = (datetime.now() - last_scale_up_time).total_seconds()
                    if time_since_scale_up < DISK_IO_SCALE_UP_COOLDOWN:
                        remaining = int((DISK_IO_SCALE_UP_COOLDOWN - time_since_scale_up) / 60)
                        logger.debug(f"Scale-up cooldown active ({remaining} min remaining)")
                        return
                
                # Scale up by 1 worker
                new_desired_count = min(MAX_WORKER_COUNT, desired_worker_count + 1)
                desired_worker_count = new_desired_count
                
                logger.info(f"📈 Scaling UP: Disk I/O {disk_util:.1f}% (normal) - increasing desired workers to {new_desired_count}")
                
                if not dry_run:
                    # Find next available worker ID
                    expected_workers = get_expected_worker_ids()
                    next_worker_id = 1
                    while next_worker_id in running_workers or next_worker_id in expected_workers:
                        next_worker_id += 1
                    
                    # Restart the worker
                    fix_result = apply_restart(next_worker_id, parallel_uploads=1, dry_run=dry_run)
                    if fix_result.get("success"):
                        last_scale_up_time = datetime.now()
                        logger.info(f"✅ Scaled up: Started worker {next_worker_id}")
                        # Save to history
                        fix_result["reason"] = "Scaled up worker due to low disk I/O"
                        fix_result["scale_action"] = "scale_up"
                        try:
                            save_fix_to_history(fix_result)
                        except Exception as e:
                            logger.error(f"Failed to save scale-up restart to history: {e}")
                    else:
                        logger.warning(f"⚠️  Failed to start worker {next_worker_id} for scaling up")
                else:
                    logger.info(f"[DRY RUN] Would start new worker to scale up")
        
    except Exception as e:
        logger.error(f"Error in scale_workers_based_on_disk_io: {e}", exc_info=True)


def check_and_restart_stopped_workers(llm_enabled: bool = False, dry_run: bool = False):
    """
    Check for workers that should be running but aren't, and restart them.
    Only restarts if it won't exceed desired_worker_count.
    """
    try:
        # Get expected workers (have progress files)
        expected_workers = get_expected_worker_ids()
        
        # Get actually running workers
        running_workers = get_running_worker_ids()
        current_count = len(running_workers)
        
        # Find stopped workers (expected but not running)
        stopped_workers = expected_workers - running_workers
        
        if stopped_workers:
            logger.warning(f"⚠️  Detected {len(stopped_workers)} stopped worker(s): {sorted(stopped_workers)}")
            
            for worker_id in sorted(stopped_workers):
                # Check if worker completed successfully (don't restart completed workers)
                log_stats = get_worker_log_stats(worker_id)
                worker_status = log_stats.get("status", "unknown")
                if worker_status == "completed":
                    logger.info(f"✅ Worker {worker_id} completed successfully (status: completed). Skipping restart.")
                    continue
                
                # Check if restarting would exceed desired worker count
                if current_count >= desired_worker_count:
                    logger.info(f"⏭️  Worker {worker_id} stopped, but current count ({current_count}) >= desired ({desired_worker_count}). Skipping restart to avoid exceeding target.")
                    continue
                
                # Check cooldown (don't restart too frequently)
                if worker_id in worker_last_fix_time:
                    time_since_last_fix = (datetime.now() - worker_last_fix_time[worker_id]).total_seconds()
                    if time_since_last_fix < COOLDOWN_SECONDS:
                        remaining = int((COOLDOWN_SECONDS - time_since_last_fix) / 60)
                        logger.info(f"Worker {worker_id} in cooldown ({remaining} min remaining), skipping restart")
                        continue
                
                # Check if worker is paused (max attempts exceeded)
                if worker_id in paused_workers:
                    logger.warning(f"Worker {worker_id} is paused (max fix attempts exceeded), skipping auto-restart")
                    continue
                
                logger.info(f"🔄 Auto-restarting stopped worker {worker_id}...")
                
                if dry_run:
                    logger.info(f"[DRY RUN] Would restart worker {worker_id}")
                    continue
                
                # Restart the worker
                try:
                    fix_result = apply_restart(worker_id, parallel_uploads=1, dry_run=dry_run)
                    
                    if fix_result.get("success"):
                        logger.info(f"✅ Worker {worker_id} auto-restarted successfully")
                        worker_last_fix_time[worker_id] = datetime.now()
                        current_count += 1  # Update count after successful restart
                        
                        # Record the restart
                        if worker_id not in worker_fix_attempts:
                            worker_fix_attempts[worker_id] = []
                        worker_fix_attempts[worker_id].append({
                            "timestamp": datetime.now().isoformat(),
                            "success": True,
                            "fix_type": "restart",
                            "message": "Auto-restarted stopped worker",
                            "auto_restart": True
                        })
                        
                        # Save to history
                        fix_result["auto_restart"] = True
                        fix_result["reason"] = "Worker was not running but has progress file"
                        try:
                            save_fix_to_history(fix_result)
                            if not dry_run:
                                verify_history_entry(fix_result)
                        except Exception as e:
                            logger.error(f"Failed to save auto-restart to history for worker {worker_id}: {e}")
                    else:
                        logger.warning(f"⚠️  Failed to auto-restart worker {worker_id}: {fix_result.get('message')}")
                except Exception as e:
                    logger.error(f"Error auto-restarting worker {worker_id}: {e}", exc_info=True)
        
    except Exception as e:
        logger.error(f"Error checking stopped workers: {e}", exc_info=True)


def ensure_minimum_workers(llm_enabled: bool = False, dry_run: bool = False):
    """
    Ensure at least MIN_WORKER_COUNT workers are running if desired_worker_count > 0.
    This is a fallback mechanism to start workers when there are none running,
    regardless of disk I/O conditions (which might prevent normal scaling).
    """
    global desired_worker_count
    
    try:
        # Get current running workers
        running_workers = get_running_worker_ids()
        current_count = len(running_workers)
        
        # Only start workers if:
        # 1. No workers are currently running
        # 2. Desired worker count is > 0 (we want at least some workers)
        # 3. Current count is below minimum
        if current_count == 0 and desired_worker_count > 0 and current_count < MIN_WORKER_COUNT:
            logger.warning(f"⚠️  No workers running! Desired: {desired_worker_count}, Minimum: {MIN_WORKER_COUNT}")
            
            # Check if recently started workers completed immediately with 0 files
            # This indicates the Calibre library might be empty or misconfigured
            expected_workers = get_expected_worker_ids()
            recent_completed_with_zero = False
            
            # Check workers 1-10 to find any that recently completed with 0 files
            # Read log files directly to avoid path issues
            for worker_id in range(1, 11):
                try:
                    log_file = WORKER_LOG_DIR / f"migration_worker{worker_id}.log"
                    if not log_file.exists():
                        continue
                    
                    # Read the last few lines of the log file directly
                    with open(log_file, 'rb') as f:
                        try:
                            f.seek(-2000, 2)  # Last 2KB
                        except OSError:
                            f.seek(0)
                        content = f.read().decode('utf-8', errors='ignore')
                        log_lines = content.split('\n')
                        
                        # Check last 20 lines for completion with 0 files
                        for line in reversed(log_lines[-20:]):
                            if "Found 0 new ebook files" in line or ("No more new files to process" in line and "Migration complete" in content):
                                # Check if completion was recent
                                timestamp = parse_log_timestamp(line)
                                if timestamp:
                                    time_since_completion = (datetime.now() - timestamp).total_seconds()
                                    if time_since_completion < 300:  # Within 5 minutes
                                        recent_completed_with_zero = True
                                        logger.warning(f"⚠️  Worker {worker_id} recently completed with 0 files ({(time_since_completion/60):.1f} min ago) - Calibre library may be empty or misconfigured")
                                        break
                        if recent_completed_with_zero:
                            break
                except Exception as e:
                    logger.debug(f"Error checking worker {worker_id} for zero-file completion: {e}")
                    continue  # Skip workers that don't have logs yet
            
            if recent_completed_with_zero:
                logger.warning("⚠️  Skipping worker start - recent workers completed immediately with 0 files.")
                logger.warning("⚠️  Please check Calibre library path and ensure files exist to process.")
                return
            
            # Start workers up to desired_worker_count (not just 1)
            workers_to_start = min(desired_worker_count, MAX_WORKER_COUNT)
            logger.info(f"🚀 Starting {workers_to_start} worker(s) to reach desired count ({desired_worker_count})...")
            
            if not dry_run:
                started_count = 0
                for i in range(workers_to_start):
                    # Find next available worker ID
                    next_worker_id = 1
                    while next_worker_id in running_workers or next_worker_id in expected_workers:
                        next_worker_id += 1
                    
                    # Start the worker
                    fix_result = apply_restart(next_worker_id, parallel_uploads=1, dry_run=dry_run)
                    if fix_result.get("success"):
                        logger.info(f"✅ Started worker {next_worker_id} ({i+1}/{workers_to_start})")
                        started_count += 1
                        running_workers.add(next_worker_id)  # Update running workers set
                        # Save to history
                        fix_result["reason"] = f"Started worker to reach desired count ({desired_worker_count})"
                        fix_result["scale_action"] = "ensure_minimum"
                        try:
                            save_fix_to_history(fix_result)
                        except Exception as e:
                            logger.error(f"Failed to save worker start to history: {e}")
                    else:
                        logger.warning(f"⚠️  Failed to start worker {next_worker_id}: {fix_result.get('message')}")
                        # If we can't start this worker, try next one
                        expected_workers.add(next_worker_id)  # Mark as expected to skip it
                
                if started_count > 0:
                    logger.info(f"✅ Started {started_count} worker(s) (desired: {desired_worker_count})")
            else:
                logger.info(f"[DRY RUN] Would start {workers_to_start} worker(s) (desired: {desired_worker_count})")
        
    except Exception as e:
        logger.error(f"Error ensuring minimum workers: {e}", exc_info=True)


def monitor_loop(llm_enabled: bool = False, dry_run: bool = False, check_interval: int = CHECK_INTERVAL_SECONDS, stuck_threshold: int = STUCK_THRESHOLD_SECONDS):
    """Main monitoring loop"""
    logger.info("=" * 80)
    logger.info("Auto-Monitor Started")
    logger.info(f"LLM Enabled: {llm_enabled}")
    logger.info(f"Dry Run: {dry_run}")
    logger.info(f"Check Interval: {check_interval} seconds")
    logger.info(f"Stuck Threshold: {stuck_threshold / 60} minutes")
    logger.info("=" * 80)
    
    while True:
        try:
            # First, check disk I/O and scale workers if needed
            scale_workers_based_on_disk_io(llm_enabled, dry_run)
            
            # Then, check for stopped workers and restart them (up to desired count)
            check_and_restart_stopped_workers(llm_enabled, dry_run)
            
            # Ensure minimum workers are running (fallback if no workers at all)
            ensure_minimum_workers(llm_enabled, dry_run)
            
            # Get running workers
            running_workers = get_running_worker_ids()
            
            # Ensure we don't exceed desired worker count
            if len(running_workers) > desired_worker_count:
                excess = len(running_workers) - desired_worker_count
                logger.warning(f"⚠️  {excess} excess worker(s) detected, killing highest ID workers...")
                sorted_workers = sorted(running_workers, reverse=True)
                for worker_id in sorted_workers[:excess]:
                    if not dry_run:
                        kill_worker(worker_id)
                    else:
                        logger.info(f"[DRY RUN] Would kill excess worker {worker_id}")
            
            if not running_workers:
                logger.debug("No workers running, waiting...")
                time.sleep(check_interval)
                continue
            
            # Calculate health scores for all workers
            worker_health_scores = {}
            for worker_id in running_workers:
                health = calculate_worker_health(worker_id)
                worker_health_scores[worker_id] = health
                
                # Log health status periodically (every 5 minutes)
                if health["health_level"] == "critical":
                    logger.warning(f"⚠️  Worker {worker_id} health: CRITICAL (score: {health['health_score']:.1f}, trend: {health['trend']})")
                elif health["health_level"] == "warning":
                    logger.info(f"⚠️  Worker {worker_id} health: WARNING (score: {health['health_score']:.1f}, trend: {health['trend']})")
            
            # Sort workers by health (prioritize critical workers)
            workers_by_priority = sorted(running_workers, key=lambda w: worker_health_scores.get(w, {}).get("health_score", 50))
            
            # Check each worker for stuck conditions (prioritize critical health workers)
            for worker_id in workers_by_priority:
                health = worker_health_scores.get(worker_id, {})
                health_level = health.get("health_level", "unknown")
                
                # Check exponential backoff for repeatedly stuck workers
                recent_attempts = get_fix_attempt_count(worker_id, within_hours=2)  # Check last 2 hours
                if recent_attempts > 0 and worker_id in worker_last_fix_time:
                    # Calculate exponential backoff: 1st = 1x, 2nd = 2x, 3rd = 4x, etc.
                    backoff_multiplier = 2 ** (recent_attempts - 1)
                    backoff_cooldown = COOLDOWN_SECONDS * backoff_multiplier
                    time_since_last_fix = (datetime.now() - worker_last_fix_time[worker_id]).total_seconds()
                    
                    if time_since_last_fix < backoff_cooldown:
                        remaining_min = int((backoff_cooldown - time_since_last_fix) / 60)
                        logger.debug(f"Worker {worker_id} in exponential backoff ({recent_attempts} recent attempts, {remaining_min} min remaining) - skipping check")
                        continue
                
                # Adjust check frequency based on health
                # Critical workers: check every cycle
                # Warning workers: check every cycle
                # Healthy workers: can skip if recently checked (but we check all anyway)
                
                # First, check for warnings (proactive monitoring)
                warning_info = check_worker_warning(worker_id, stuck_threshold)
                if warning_info:
                    warning_level = warning_info.get("warning_level")
                    minutes_until_stuck = warning_info.get("minutes_until_stuck", 0)
                    
                    if warning_level == "intervention_needed":
                        # At 83% threshold - perform early intervention
                        logger.warning(f"⚠️  Worker {worker_id} approaching stuck threshold ({minutes_until_stuck} min until stuck) - performing early intervention")
                        intervention_result = perform_early_intervention(worker_id, warning_info, dry_run)
                        if intervention_result.get("success"):
                            logger.info(f"✅ Early intervention for worker {worker_id}: {intervention_result.get('message')}")
                        # Continue to check if actually stuck (intervention may not have helped)
                    elif warning_level == "warning":
                        # At 67% threshold - log warning
                        logger.warning(f"⚠️  Worker {worker_id} approaching stuck threshold ({minutes_until_stuck} min until stuck) - monitoring closely")
                        logger.info(f"  Status: {warning_info.get('status')}")
                        if warning_info.get('error_patterns'):
                            logger.info(f"  Error patterns: {', '.join(warning_info['error_patterns'][:3])}")
                        # Don't intervene yet, just monitor
                
                # Check if worker is actually stuck (at 100% threshold)
                diagnostics = check_worker_stuck(worker_id, stuck_threshold, llm_enabled)
                
                if diagnostics:
                    logger.warning(f"Worker {worker_id} is STUCK: no uploads for {diagnostics['minutes_stuck']} minutes")
                    logger.info(f"  Status: {diagnostics['status']}")
                    if health and isinstance(health, dict):
                        logger.info(f"  Health: {health_level} (score: {health.get('health_score', 0):.1f}, trend: {health.get('trend', 'unknown')})")
                    else:
                        logger.info(f"  Health: {health_level} (calculation failed)")
                    logger.info(f"  Book ID range: {diagnostics['book_id_range']}")
                    if diagnostics.get('error_patterns'):
                        logger.info(f"  Error patterns: {', '.join(diagnostics['error_patterns'][:5])}")
                    
                    # Log backoff status if applicable
                    if recent_attempts > 0:
                        logger.info(f"  ⚠️  Worker {worker_id} has {recent_attempts} recent fix attempt(s) - using exponential backoff")
                    
                    # Add health metrics to diagnostics
                    if health and isinstance(health, dict):
                        diagnostics["health_score"] = health.get("health_score", 0)
                        diagnostics["health_level"] = health_level
                        diagnostics["health_trend"] = health.get("trend", "unknown")
                    else:
                        diagnostics["health_score"] = 0
                        diagnostics["health_level"] = "unknown"
                        diagnostics["health_trend"] = "unknown"
                    
                    # Auto-fix
                    fix_result = auto_fix_worker(worker_id, diagnostics, llm_enabled, dry_run)
                    
                    if fix_result.get("success"):
                        logger.info(f"✅ Worker {worker_id} fixed: {fix_result.get('message')}")
                    else:
                        logger.warning(f"⚠️  Worker {worker_id} fix failed: {fix_result.get('message')}")
            
            # Sleep before next check
            time.sleep(check_interval)
            
        except KeyboardInterrupt:
            logger.info("Auto-monitor stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in monitor loop: {e}", exc_info=True)
            time.sleep(check_interval)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Auto-monitor for MyBookshelf2 migration workers")
    parser.add_argument("--llm-enabled", action="store_true", default=False,
                       help="Enable LLM-powered debugging (default: False, disabled)")
    parser.add_argument("--dry-run", action="store_true", help="Test mode: detect but don't apply fixes")
    parser.add_argument("--check-interval", type=int, default=CHECK_INTERVAL_SECONDS,
                       help=f"Seconds between checks (default: {CHECK_INTERVAL_SECONDS})")
    parser.add_argument("--threshold", type=int, default=int(STUCK_THRESHOLD_SECONDS / 60),
                       help=f"Minutes before considering worker stuck (default: {int(STUCK_THRESHOLD_SECONDS / 60)})")
    
    args = parser.parse_args()
    
    # LLM is disabled by default (action="store_true" means False unless --llm-enabled flag is provided)
    # Explicitly ensure it's False if not set
    llm_enabled = getattr(args, 'llm_enabled', False)
    
    # Calculate threshold in seconds
    stuck_threshold_seconds = (args.threshold or int(STUCK_THRESHOLD_SECONDS / 60)) * 60
    
    # Check LLM availability (only if explicitly enabled)
    if llm_enabled:
        try:
            import openai
            if not OPENAI_API_KEY:
                logger.warning("⚠️  LLM enabled but OPENAI_API_KEY not set. LLM features will be disabled.")
                llm_enabled = False
            else:
                logger.info("✅ LLM enabled with OpenAI API")
        except ImportError:
            logger.warning("⚠️  LLM enabled but 'openai' package not installed. Install with: pip install openai")
            llm_enabled = False
    else:
        logger.info("ℹ️  LLM disabled by default (use --llm-enabled to enable)")
    
    # Start monitoring
    monitor_loop(
        llm_enabled=llm_enabled,
        dry_run=args.dry_run,
        check_interval=args.check_interval,
        stuck_threshold=stuck_threshold_seconds
    )


if __name__ == "__main__":
    main()

