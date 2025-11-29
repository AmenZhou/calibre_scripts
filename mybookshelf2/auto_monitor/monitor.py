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
        MAX_FIX_ATTEMPTS, SUCCESS_VERIFICATION_SECONDS, ESCALATION_ACTION
    )
    from .llm_debugger import analyze_worker_with_llm
    from .fix_applier import apply_restart, apply_code_fix, apply_config_fix, save_fix_to_history
except ImportError:
    # Running as script, use absolute imports
    sys.path.insert(0, str(Path(__file__).parent))
    from config import (
        STUCK_THRESHOLD_SECONDS, COOLDOWN_SECONDS, CHECK_INTERVAL_SECONDS,
        LOG_FILE, WORKER_LOG_DIR, LOG_LINES_TO_ANALYZE, OPENAI_API_KEY,
        MAX_FIX_ATTEMPTS, SUCCESS_VERIFICATION_SECONDS, ESCALATION_ACTION
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
    - Progress in processing
    
    Returns True if worker is NOT making progress (no new books found, no uploads)
    """
    # Check last 200 lines for progress indicators
    recent_logs = '\n'.join(logs.split('\n')[-200:])
    
    # Look for "Found X new files" messages in recent logs
    found_new_files = re.findall(r'Found\s+(\d+)\s+new\s+files', recent_logs, re.IGNORECASE)
    if found_new_files:
        # Check if any batch found new files
        new_files_counts = [int(x) for x in found_new_files]
        if any(count > 0 for count in new_files_counts):
            return False  # Worker is finding new files
    
    # Look for upload messages
    if "Successfully uploaded" in recent_logs or "Uploading:" in recent_logs:
        return False  # Worker is uploading
    
    # Look for "Processed batch" messages - if processing batches, might be making progress
    if "Processed batch" in recent_logs:
        # Check if batches are finding files
        batch_messages = re.findall(r'Processed batch.*Found\s+(\d+)\s+new', recent_logs, re.IGNORECASE)
        if batch_messages and any(int(x) > 0 for x in batch_messages):
            return False  # Batches are finding files
    
    # If we get here, worker is not making progress
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
        # For workers in "initializing" or "discovering", check if they've been in this state too long
        if status in ["initializing", "discovering"]:
            # For workers stuck in discovery/init, use a longer threshold (10 minutes)
            # Even if they have recent activity, if they've been running >10 min with no uploads, they're stuck
            discovery_threshold = max(stuck_threshold, 600)  # At least 10 minutes
            
            # First check if worker is making any progress (finding new files, uploading)
            logs = get_worker_logs(worker_id, lines=500)
            no_progress = check_worker_no_progress(worker_id, logs)
            
            if not no_progress:
                # Worker is making progress, not stuck
                logger.debug(f"Worker {worker_id} is making progress, not stuck")
                return None
            
            # Worker is not making progress - check how long it's been running
            logger.debug(f"Worker {worker_id} is NOT making progress - checking uptime")
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
                            
                            # If process has been running >10 min and no progress, it's stuck
                            logger.debug(f"Worker {worker_id} process uptime: {total_minutes} minutes, threshold: {discovery_threshold/60} minutes, no_progress={no_progress}")
                            if total_minutes > (discovery_threshold / 60):
                                minutes_stuck = total_minutes
                                logger.info(f"Worker {worker_id} detected as stuck: {minutes_stuck} minutes uptime, no progress detected")
                            else:
                                logger.debug(f"Worker {worker_id} not stuck yet: {total_minutes} < {discovery_threshold/60} minutes")
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
    
    # Use LLM to analyze if enabled
    llm_analysis = None
    if llm_enabled:
        logger.info(f"Analyzing worker {worker_id} with LLM...")
        llm_analysis = analyze_worker_with_llm(
            worker_id,
            diagnostics["logs"],
            diagnostics
        )
        
        if llm_analysis:
            logger.info(f"LLM analysis: {llm_analysis.get('root_cause', 'Unknown')}")
            logger.info(f"LLM recommended fix: {llm_analysis.get('fix_type', 'restart')}")
    
    # Determine fix type
    if llm_analysis and llm_analysis.get("fix_type") == "code_fix":
        # Apply code fix
        fix_result = apply_code_fix(
            llm_analysis.get("fix_description", ""),
            llm_analysis.get("code_changes", ""),
            dry_run
        )
    elif llm_analysis and llm_analysis.get("fix_type") == "config_fix":
        # Apply config fix
        config_changes = {"parallel_uploads": 1}  # Default safe value
        fix_result = apply_config_fix(worker_id, config_changes, dry_run)
    else:
        # Default: restart worker
        fix_result = apply_restart(worker_id, parallel_uploads=1, dry_run=dry_run)
    
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
    
    # Save to history
    save_fix_to_history(fix_result)
    
    return fix_result


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
            # Get running workers
            running_workers = get_running_worker_ids()
            
            if not running_workers:
                logger.debug("No workers running, waiting...")
                time.sleep(check_interval)
                continue
            
            # Check each worker
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
                logger.warning("LLM enabled but OPENAI_API_KEY not set. LLM features will be disabled.")
                args.llm_enabled = False
        except ImportError:
            logger.warning("LLM enabled but 'openai' package not installed. Install with: pip install openai")
            args.llm_enabled = False
    
    # Start monitoring
    monitor_loop(
        llm_enabled=args.llm_enabled,
        dry_run=args.dry_run,
        check_interval=args.check_interval,
        stuck_threshold=stuck_threshold_seconds
    )


if __name__ == "__main__":
    main()

