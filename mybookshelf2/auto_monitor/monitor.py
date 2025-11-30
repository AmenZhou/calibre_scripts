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
from typing import Dict, Any, Optional, Set
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
        HISTORY_FILE, RECURRING_ROOT_CAUSE_THRESHOLD
    )
    from .llm_debugger import analyze_worker_with_llm
    from .fix_applier import apply_restart, apply_code_fix, apply_config_fix, save_fix_to_history
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
        HISTORY_FILE, RECURRING_ROOT_CAUSE_THRESHOLD
    )
    from llm_debugger import analyze_worker_with_llm
    from fix_applier import apply_restart, apply_code_fix, apply_config_fix, save_fix_to_history


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

# Track worker scaling state
desired_worker_count: int = TARGET_WORKER_COUNT  # Current desired worker count
last_scale_down_time: Optional[datetime] = None  # Last time we scaled down
last_scale_up_time: Optional[datetime] = None  # Last time we scaled up


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


def check_worker_no_progress(worker_id: int, logs: str) -> bool:
    """
    Check if worker is making progress by looking for:
    - New files found in recent batches
    - Successful uploads
    - Progress in processing (Processed batch messages)
    - Database query activity
    
    Returns True if worker is NOT making progress (no new books found, no uploads, no batch processing)
    """
    # Check last 300 lines for progress indicators (increased from 200 to catch more activity)
    recent_logs = '\n'.join(logs.split('\n')[-300:])
    
    # Look for "Found X new files" messages in recent logs
    found_new_files = re.findall(r'Found\s+(\d+)\s+new\s+files', recent_logs, re.IGNORECASE)
    if found_new_files:
        # Check if any batch found new files
        new_files_counts = [int(x) for x in found_new_files]
        if any(count > 0 for count in new_files_counts):
            logger.debug(f"Worker {worker_id} is finding new files: {new_files_counts}")
            return False  # Worker is finding new files
    
    # Look for upload messages
    if "Successfully uploaded" in recent_logs or "Uploading:" in recent_logs or "Uploaded:" in recent_logs:
        logger.debug(f"Worker {worker_id} is uploading files")
        return False  # Worker is uploading
    
    # Look for "Processed batch" messages - this indicates active discovery/processing
    if "Processed batch" in recent_logs:
        # Extract batch processing info
        batch_pattern = r'Processed batch.*?book\.id\s*>\s*(\d+)'
        batch_matches = re.findall(batch_pattern, recent_logs, re.IGNORECASE)
        if batch_matches:
            logger.debug(f"Worker {worker_id} is processing batches: {len(batch_matches)} batches found")
            return False  # Worker is actively processing batches (discovery in progress)
        
        # Also check for "rows=" pattern in Processed batch messages
        if re.search(r'Processed batch.*rows\s*=\s*\d+', recent_logs, re.IGNORECASE):
            logger.debug(f"Worker {worker_id} is processing database rows")
            return False  # Worker is processing database rows
    
    # Look for database query activity (indicates discovery is happening)
    if "Querying Calibre database" in recent_logs or "book.id >" in recent_logs:
        logger.debug(f"Worker {worker_id} is querying database")
        return False  # Worker is querying database (discovery in progress)
    
    # Look for "Found X new files so far" messages (during batch processing)
    found_so_far = re.findall(r'Found\s+(\d+)\s+new\s+files\s+so\s+far', recent_logs, re.IGNORECASE)
    if found_so_far:
        counts = [int(x) for x in found_so_far]
        if any(count > 0 for count in counts):
            logger.debug(f"Worker {worker_id} is accumulating files: {counts}")
            return False  # Worker is finding files during batch processing
    
    # If we get here, worker is not making progress
    logger.debug(f"Worker {worker_id} shows no progress indicators")
    return True


def check_worker_stuck(worker_id: int, stuck_threshold: int = STUCK_THRESHOLD_SECONDS, llm_enabled: bool = False) -> Optional[Dict[str, Any]]:
    """
    Check if a worker is stuck and return diagnostic information.
    
    Returns:
        Dictionary with stuck status and diagnostics, or None if not stuck
    """
    # Get last upload time
    last_upload = get_last_upload_time(worker_id)
    log_stats = get_worker_log_stats(worker_id)
    status = log_stats.get("status", "unknown")
    last_activity = log_stats.get("last_activity_time")
    
    if last_upload:
        # Worker has uploaded before - check time since last upload
        time_since_upload = (datetime.now() - last_upload).total_seconds()
        if time_since_upload < stuck_threshold:
            return None
        
        minutes_stuck = int(time_since_upload // 60)
    else:
        # Worker has never uploaded - check if stuck in discovery/initialization
        if not last_activity:
            return None
        
        # Check how long worker has been in current status
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
            no_progress = check_worker_no_progress(worker_id, logs)
            
            if not no_progress:
                # Worker is making progress, not stuck - even if no uploads yet
                logger.debug(f"Worker {worker_id} is making progress during discovery, not stuck")
                return None
            
            # Worker is not making progress - check how long it's been running
            logger.debug(f"Worker {worker_id} is NOT making progress - checking uptime (threshold: {discovery_threshold/60} min)")
            # Check process uptime to see how long worker has been running
            import subprocess
            import re
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
        logs = get_worker_logs(worker_id)
        error_patterns = extract_error_patterns(logs)
        book_id_range = extract_book_id_range(logs)
        log_stats = get_worker_log_stats(worker_id)
        
        diagnostics = {
            "worker_id": worker_id,
            "minutes_stuck": minutes_stuck,
            "last_upload_time": last_upload.isoformat() if last_upload else None,
            "book_id_range": book_id_range,
            "error_patterns": error_patterns,
            "status": log_stats.get("status", "unknown"),
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


def check_recurring_root_cause(root_cause: str) -> Dict[str, Any]:
    """
    Check if a root cause has appeared before in the fix history.
    Uses fuzzy matching to detect similar root causes.
    
    Args:
        root_cause: Current root cause description
    
    Returns:
        Dictionary with:
        - is_recurring: boolean
        - occurrence_count: number of times this root cause appeared
        - last_occurrence: timestamp of last occurrence (ISO format)
        - suggest_code_fix: boolean (True if occurrence_count >= threshold)
    """
    result = {
        "is_recurring": False,
        "occurrence_count": 0,
        "last_occurrence": None,
        "suggest_code_fix": False
    }
    
    if not root_cause or root_cause == "Unknown":
        return result
    
    if not HISTORY_FILE.exists():
        return result
    
    try:
        with open(HISTORY_FILE, 'r') as f:
            history = json.load(f)
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
        stop_words = {'the', 'is', 'at', 'which', 'on', 'a', 'an', 'as', 'are', 'was', 'were', 'been', 'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'should', 'could', 'may', 'might', 'must', 'can', 'this', 'that', 'these', 'those', 'to', 'of', 'in', 'for', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during', 'including', 'against', 'among', 'throughout', 'despite', 'towards', 'upon', 'concerning', 'to', 'of', 'and', 'or', 'but', 'if', 'because', 'as', 'while', 'when', 'where', 'so', 'than', 'then', 'no', 'not', 'only', 'also', 'just', 'more', 'most', 'very', 'too', 'much', 'many', 'some', 'any', 'all', 'each', 'every', 'both', 'few', 'other', 'another', 'such', 'same', 'different', 'own', 'new', 'old', 'good', 'bad', 'big', 'small', 'long', 'short', 'high', 'low', 'first', 'last', 'next', 'previous', 'early', 'late', 'young', 'old', 'right', 'wrong', 'true', 'false', 'yes', 'no', 'ok', 'okay', 'well', 'better', 'best', 'worse', 'worst', 'same', 'different', 'similar', 'different', 'like', 'unlike', 'same', 'equal', 'unequal', 'greater', 'less', 'more', 'most', 'least', 'few', 'many', 'much', 'little', 'enough', 'too', 'very', 'quite', 'rather', 'pretty', 'fairly', 'really', 'actually', 'probably', 'possibly', 'maybe', 'perhaps', 'certainly', 'definitely', 'absolutely', 'exactly', 'approximately', 'about', 'around', 'nearly', 'almost', 'quite', 'rather', 'pretty', 'fairly', 'very', 'too', 'so', 'such', 'how', 'what', 'when', 'where', 'why', 'who', 'which', 'whose', 'whom', 'whether', 'if', 'unless', 'until', 'while', 'as', 'since', 'because', 'although', 'though', 'even', 'though', 'despite', 'in', 'spite', 'of', 'instead', 'of', 'rather', 'than', 'as', 'well', 'as', 'both', 'and', 'either', 'or', 'neither', 'nor', 'not', 'only', 'but', 'also', 'whether', 'or', 'not', 'so', 'that', 'such', 'that', 'in', 'order', 'that', 'so', 'as', 'to', 'in', 'case', 'provided', 'that', 'as', 'long', 'as', 'as', 'soon', 'as', 'no', 'sooner', 'than', 'hardly', 'when', 'scarcely', 'when', 'barely', 'when', 'by', 'the', 'time', 'the', 'moment', 'that', 'every', 'time', 'each', 'time', 'the', 'first', 'time', 'the', 'last', 'time', 'the', 'next', 'time', 'the', 'previous', 'time', 'the', 'second', 'time', 'the', 'third', 'time', 'the', 'fourth', 'time', 'the', 'fifth', 'time', 'the', 'sixth', 'time', 'the', 'seventh', 'time', 'the', 'eighth', 'time', 'the', 'ninth', 'time', 'the', 'tenth', 'time'}
        words = [w for w in normalized.split() if len(w) > 2 and w not in stop_words]
        return set(words)
    
    current_keywords = normalize_text(root_cause)
    if not current_keywords:
        return result
    
    # Find similar root causes in history
    matches = []
    for entry in history:
        historical_root_cause = entry.get("llm_root_cause") or entry.get("llm_analysis", {}).get("root_cause", "")
        if not historical_root_cause or historical_root_cause == "Unknown":
            continue
        
        historical_keywords = normalize_text(historical_root_cause)
        if not historical_keywords:
            continue
        
        # Check if they share at least 3 keywords
        common_keywords = current_keywords.intersection(historical_keywords)
        if len(common_keywords) >= 3:
            matches.append({
                "root_cause": historical_root_cause,
                "timestamp": entry.get("timestamp", ""),
                "common_keywords": len(common_keywords)
            })
    
    if matches:
        result["is_recurring"] = True
        result["occurrence_count"] = len(matches)
        # Get most recent occurrence
        matches.sort(key=lambda x: x["timestamp"], reverse=True)
        result["last_occurrence"] = matches[0]["timestamp"]
        result["suggest_code_fix"] = len(matches) >= RECURRING_ROOT_CAUSE_THRESHOLD
    
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
    
    # Check for recurring root cause before LLM analysis
    # We'll check again after LLM analysis with the actual root cause
    recurring_info = None
    
    # Use LLM to analyze if enabled
    llm_analysis = None
    if llm_enabled:
        logger.info(f"Analyzing worker {worker_id} with LLM...")
        llm_analysis = analyze_worker_with_llm(
            worker_id,
            diagnostics["logs"],
            diagnostics
        )
        
        # Check for recurring root cause after LLM analysis
        if llm_analysis:
            root_cause = llm_analysis.get('root_cause', 'Unknown')
            if root_cause and root_cause != "Unknown":
                recurring_info = check_recurring_root_cause(root_cause)
                if recurring_info["is_recurring"]:
                    logger.info(f"⚠️  Recurring root cause detected for worker {worker_id}: "
                              f"appeared {recurring_info['occurrence_count']} time(s) before")
                    if recurring_info["suggest_code_fix"]:
                        logger.info(f"   Suggesting code_fix (threshold: {RECURRING_ROOT_CAUSE_THRESHOLD} occurrences)")
                
                # Add recurring info to diagnostics for potential re-analysis
                diagnostics["recurring_root_cause"] = recurring_info["is_recurring"]
                diagnostics["root_cause_occurrence_count"] = recurring_info["occurrence_count"]
                diagnostics["suggest_code_fix_for_recurring"] = recurring_info["suggest_code_fix"]
                diagnostics["root_cause_keywords"] = root_cause
                
                # If recurring and LLM suggested restart, we could re-analyze with recurring info
                # But for now, we'll just log it and let the prompt handle it
        
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
    
    # Determine fix type
    if llm_analysis and llm_analysis.get("fix_type") == "code_fix":
        # Apply code fix
        logger.info(f"🔧 Applying LLM Code Fix for Worker {worker_id}...")
        fix_result = apply_code_fix(
            llm_analysis.get("fix_description", ""),
            llm_analysis.get("code_changes", ""),
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
    save_fix_to_history(fix_result)
    
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
                
                # We have stuck workers - use LLM to analyze if disk I/O is the root cause
                # But if disk I/O is >= 90% and workers are stuck, scale down regardless of LLM result
                should_scale_down = False
                llm_confirmed = False
                
                if llm_enabled:
                    # Analyze with LLM to determine if disk I/O is the issue
                    logger.info(f"🔍 Analyzing {len(stuck_workers)} stuck worker(s) with LLM to determine if disk I/O is the root cause...")
                    
                    for worker_id, diagnostics in stuck_workers:
                        # Collect logs for LLM analysis
                        log_file = WORKER_LOG_DIR / f"migration_worker{worker_id}.log"
                        logs = ""
                        if log_file.exists():
                            try:
                                with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                                    lines = f.readlines()
                                    logs = ''.join(lines[-LOG_LINES_TO_ANALYZE:])
                            except Exception:
                                pass
                        
                        # Add disk I/O context
                        diagnostics['disk_io_utilization'] = disk_util
                        diagnostics['disk_io_saturated'] = True
                        
                        # Analyze with LLM
                        llm_analysis = analyze_worker_with_llm(worker_id, logs, diagnostics)
                        
                        if llm_analysis:
                            root_cause = llm_analysis.get('root_cause', '').lower()
                            fix_type = llm_analysis.get('fix_type', '').lower()
                            if ('disk' in root_cause or 'i/o' in root_cause or 'io' in root_cause or 'saturat' in root_cause) or fix_type == 'scale_down':
                                should_scale_down = True
                                llm_confirmed = True
                                logger.info(f"✅ LLM confirmed: Disk I/O saturation is the root cause for worker {worker_id}")
                                break
                            else:
                                logger.info(f"ℹ️  LLM analysis: Root cause is not disk I/O for worker {worker_id}: {root_cause[:100]}")
                
                # Fallback: If disk I/O is >= 90% and workers are stuck, scale down even if LLM didn't confirm
                # This handles cases where LLM returns "Unknown" but disk I/O is clearly the issue
                if not should_scale_down and disk_util >= DISK_IO_SATURATED_THRESHOLD and stuck_workers:
                    should_scale_down = True
                    logger.warning(f"⚠️  Disk I/O {disk_util:.1f}% saturated and {len(stuck_workers)} worker(s) stuck - scaling down (LLM {'returned Unknown' if llm_enabled else 'disabled'}, but disk I/O clearly saturated)")
                elif not llm_enabled:
                    # Without LLM, assume disk I/O is the cause if workers are stuck and disk is saturated
                    should_scale_down = True
                    logger.info(f"⚠️  Disk I/O {disk_util:.1f}% saturated and {len(stuck_workers)} worker(s) stuck - scaling down (LLM disabled, assuming disk I/O is cause)")
                
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
                        save_fix_to_history(fix_result)
                    else:
                        logger.warning(f"⚠️  Failed to auto-restart worker {worker_id}: {fix_result.get('message')}")
                except Exception as e:
                    logger.error(f"Error auto-restarting worker {worker_id}: {e}", exc_info=True)
        
    except Exception as e:
        logger.error(f"Error checking stopped workers: {e}", exc_info=True)


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
            
            # Check each worker for stuck conditions
            for worker_id in running_workers:
                diagnostics = check_worker_stuck(worker_id, stuck_threshold, llm_enabled)
                
                if diagnostics:
                    logger.warning(f"Worker {worker_id} is STUCK: no uploads for {diagnostics['minutes_stuck']} minutes")
                    logger.info(f"  Status: {diagnostics['status']}")
                    logger.info(f"  Book ID range: {diagnostics['book_id_range']}")
                    if diagnostics['error_patterns']:
                        logger.info(f"  Error patterns: {', '.join(diagnostics['error_patterns'][:5])}")
                    
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
    parser.add_argument("--llm-enabled", action="store_true", help="Enable LLM-powered debugging")
    parser.add_argument("--dry-run", action="store_true", help="Test mode: detect but don't apply fixes")
    parser.add_argument("--check-interval", type=int, default=CHECK_INTERVAL_SECONDS,
                       help=f"Seconds between checks (default: {CHECK_INTERVAL_SECONDS})")
    parser.add_argument("--threshold", type=int, default=int(STUCK_THRESHOLD_SECONDS / 60),
                       help=f"Minutes before considering worker stuck (default: {int(STUCK_THRESHOLD_SECONDS / 60)})")
    
    args = parser.parse_args()
    
    # Calculate threshold in seconds
    stuck_threshold_seconds = (args.threshold or int(STUCK_THRESHOLD_SECONDS / 60)) * 60
    
    # Check LLM availability
    if args.llm_enabled:
        try:
            import openai
            if not OPENAI_API_KEY:
                logger.warning("⚠️  LLM enabled but OPENAI_API_KEY not set. LLM features will be disabled.")
                args.llm_enabled = False
            else:
                logger.info("✅ LLM enabled with OpenAI API")
        except ImportError:
            logger.warning("⚠️  LLM enabled but 'openai' package not installed. Install with: pip install openai")
            args.llm_enabled = False
    else:
        logger.info("ℹ️  LLM disabled (use --llm-enabled to enable)")
    
    # Start monitoring
    monitor_loop(
        llm_enabled=args.llm_enabled,
        dry_run=args.dry_run,
        check_interval=args.check_interval,
        stuck_threshold=stuck_threshold_seconds
    )


if __name__ == "__main__":
    main()

