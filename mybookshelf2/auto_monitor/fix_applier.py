"""
Apply fixes to workers: restarts, code changes, config updates
"""
import subprocess
import shutil
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import re

# Handle imports for both script and module usage
try:
    from .config import (
        RESTART_SCRIPT, BULK_MIGRATE_SCRIPT, BACKUP_DIR,
        HISTORY_FILE, ENABLE_CODE_FIXES, ENABLE_CONFIG_FIXES,
        REQUIRE_BACKUP, VALIDATE_SYNTAX
    )
except ImportError:
    from config import (
        RESTART_SCRIPT, BULK_MIGRATE_SCRIPT, BACKUP_DIR,
        HISTORY_FILE, ENABLE_CODE_FIXES, ENABLE_CONFIG_FIXES,
        REQUIRE_BACKUP, VALIDATE_SYNTAX
    )


def apply_restart(worker_id: int, parallel_uploads: int = 1, dry_run: bool = False) -> Dict[str, Any]:
    """
    Restart a worker using restart_worker.sh script.
    
    Returns:
        Dictionary with success status and details
    """
    result = {
        "worker_id": worker_id,
        "fix_type": "restart",
        "timestamp": datetime.now().isoformat(),
        "success": False,
        "message": ""
    }
    
    if dry_run:
        result["message"] = f"[DRY RUN] Would restart worker {worker_id} with {parallel_uploads} parallel uploads"
        result["success"] = True
        return result
    
    try:
        # Call restart_worker.sh script
        script_path = str(RESTART_SCRIPT.absolute())
        cmd = [script_path, str(worker_id), str(parallel_uploads)]
        
        process = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=RESTART_SCRIPT.parent
        )
        
        if process.returncode == 0:
            result["success"] = True
            result["message"] = f"Worker {worker_id} restarted successfully"
        else:
            result["message"] = f"Restart failed: {process.stderr}"
            
    except subprocess.TimeoutExpired:
        result["message"] = "Restart timeout"
    except Exception as e:
        result["message"] = f"Error restarting worker: {str(e)}"
    
    return result


def apply_code_fix(fix_description: str, code_changes: str, dry_run: bool = False) -> Dict[str, Any]:
    """
    Apply code changes to bulk_migrate_calibre.py.
    
    Args:
        fix_description: Description of the fix
        code_changes: Code changes to apply (can be full code block or diff)
        dry_run: If True, don't actually apply changes
    
    Returns:
        Dictionary with success status and details
    """
    result = {
        "fix_type": "code_fix",
        "timestamp": datetime.now().isoformat(),
        "success": False,
        "message": "",
        "backup_file": None
    }
    
    if not ENABLE_CODE_FIXES:
        result["message"] = "Code fixes are disabled in config"
        return result
    
    if dry_run:
        result["message"] = f"[DRY RUN] Would apply code fix: {fix_description}"
        result["success"] = True
        return result
    
    # Create backup
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = BACKUP_DIR / f"bulk_migrate_calibre.py.backup.{timestamp}"
    
    try:
        # Backup original file
        if REQUIRE_BACKUP:
            shutil.copy2(BULK_MIGRATE_SCRIPT, backup_file)
            result["backup_file"] = str(backup_file)
        
        # For now, code fixes require manual review
        # In a full implementation, this would parse the code_changes
        # and apply them using search_replace or similar
        result["message"] = f"Code fix requires manual review. Backup created: {backup_file}"
        result["success"] = False  # Mark as requiring manual intervention
        
        # TODO: Implement automatic code fix application
        # This would involve:
        # 1. Parsing code_changes (could be diff format or full code blocks)
        # 2. Finding the location in the file
        # 3. Applying the change
        # 4. Validating syntax
        # 5. Rolling back if invalid
        
    except Exception as e:
        result["message"] = f"Error applying code fix: {str(e)}"
        # Restore backup if creation failed
        if backup_file and backup_file.exists():
            try:
                shutil.copy2(backup_file, BULK_MIGRATE_SCRIPT)
            except:
                pass
    
    return result


def apply_config_fix(worker_id: int, config_changes: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """
    Apply configuration changes (e.g., change parallel uploads, batch size).
    This is done by restarting the worker with new parameters.
    
    Args:
        worker_id: Worker ID
        config_changes: Dictionary with config changes (e.g., {"parallel_uploads": 2})
        dry_run: If True, don't actually apply changes
    
    Returns:
        Dictionary with success status and details
    """
    result = {
        "worker_id": worker_id,
        "fix_type": "config_fix",
        "timestamp": datetime.now().isoformat(),
        "success": False,
        "message": ""
    }
    
    if not ENABLE_CONFIG_FIXES:
        result["message"] = "Config fixes are disabled in config"
        return result
    
    # Extract parallel_uploads if specified
    parallel_uploads = config_changes.get("parallel_uploads", 1)
    
    if dry_run:
        result["message"] = f"[DRY RUN] Would apply config fix: {config_changes}"
        result["success"] = True
        return result
    
    # Apply config fix by restarting with new parameters
    return apply_restart(worker_id, parallel_uploads, dry_run)


def save_fix_to_history(fix_result: Dict[str, Any]) -> None:
    """Save fix result to history file"""
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Load existing history
    history = []
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, 'r') as f:
                content = f.read()
                # Handle multiple JSON objects
                if content.strip().count('{') > 1:
                    # Extract last valid JSON object
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
                        try:
                            history = json.loads(content[start_pos:last_brace+1])
                            if not isinstance(history, list):
                                history = [history]
                        except:
                            history = []
                else:
                    history = json.loads(content)
                    if not isinstance(history, list):
                        history = [history]
        except:
            history = []
    
    # Append new fix
    history.append(fix_result)
    
    # Save (keep last 1000 entries)
    if len(history) > 1000:
        history = history[-1000:]
    
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)

