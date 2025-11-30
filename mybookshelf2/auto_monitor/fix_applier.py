"""
Apply fixes to workers: restarts, code changes, config updates
"""
import subprocess
import shutil
import json
import ast
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
import re
import logging

logger = logging.getLogger(__name__)

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


def validate_python_syntax(file_path: Path) -> Tuple[bool, Optional[str]]:
    """
    Validate Python syntax of a file.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            source = f.read()
        
        # Try to compile the AST (primary validation)
        try:
            ast.parse(source, filename=str(file_path))
        except SyntaxError as e:
            return False, f"Syntax error at line {e.lineno}: {e.msg}"
        
        # Also try py_compile for additional validation (optional, may fail on permissions)
        try:
            import py_compile
            # Use a temporary directory for bytecode to avoid permission issues
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.pyc', delete=True) as tmp:
                py_compile.compile(str(file_path), tmp.name, doraise=True)
        except (PermissionError, OSError) as e:
            # Permission errors are OK - AST validation passed
            logger.warning(f"py_compile validation skipped due to permissions: {e}")
        except py_compile.PyCompileError as e:
            return False, f"Compile error: {str(e)}"
        
        return True, None
    except Exception as e:
        return False, f"Validation error: {str(e)}"


def parse_code_changes(code_changes: str) -> Dict[str, Any]:
    """
    Parse code changes from LLM response.
    Supports multiple formats:
    1. Full code block (function/method replacement)
    2. Diff format (with line numbers)
    3. Context-based replacement (old_string -> new_string)
    
    Returns:
        Dictionary with parsed change information
    """
    parsed = {
        "format": "unknown",
        "old_code": None,
        "new_code": None,
        "line_numbers": None,
        "function_name": None
    }
    
    if not code_changes or not code_changes.strip():
        return parsed
    
    # Try to extract diff format (with line numbers)
    diff_match = re.search(r'@@[^\n]+\n([\s\S]+?)(?=@@|$)', code_changes)
    if diff_match:
        parsed["format"] = "diff"
        # Extract line numbers from diff header
        line_match = re.search(r'@@\s*-(\d+)(?:,(\d+))?\s*\+(\d+)(?:,(\d+))?', code_changes)
        if line_match:
            parsed["line_numbers"] = {
                "old_start": int(line_match.group(1)),
                "old_count": int(line_match.group(2)) if line_match.group(2) else 1,
                "new_start": int(line_match.group(3)),
                "new_count": int(line_match.group(4)) if line_match.group(4) else 1
            }
    
    # Try to extract function/method definition
    func_match = re.search(r'def\s+(\w+)\s*\([^)]*\):', code_changes)
    if func_match:
        parsed["function_name"] = func_match.group(1)
        parsed["format"] = "function_replacement"
        parsed["new_code"] = code_changes.strip()
    
    # Try to extract context-based replacement (old_string -> new_string)
    if "old_string" in code_changes.lower() or "replace" in code_changes.lower():
        # Look for patterns like "Replace X with Y" or "old_string: ... new_string: ..."
        old_match = re.search(r'(?:old|before|original)[\s:]+(.*?)(?=(?:new|after|replacement)|$)', code_changes, re.IGNORECASE | re.DOTALL)
        new_match = re.search(r'(?:new|after|replacement)[\s:]+(.*?)$', code_changes, re.IGNORECASE | re.DOTALL)
        if old_match and new_match:
            parsed["format"] = "context_replacement"
            parsed["old_code"] = old_match.group(1).strip()
            parsed["new_code"] = new_match.group(1).strip()
    
    # If no specific format detected, treat as full code block
    if parsed["format"] == "unknown":
        parsed["format"] = "full_block"
        parsed["new_code"] = code_changes.strip()
    
    return parsed


def find_code_location(file_content: str, parsed_changes: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    """
    Find the location in the file where changes should be applied.
    
    Returns:
        Tuple of (start_line, end_line) or None if not found
    """
    lines = file_content.split('\n')
    
    # If function name is specified, find the function
    if parsed_changes.get("function_name"):
        func_name = parsed_changes["function_name"]
        for i, line in enumerate(lines):
            if re.match(rf'^\s*def\s+{func_name}\s*\(', line):
                # Find the end of the function (next def/class at same or lower indentation)
                start_line = i
                indent = len(line) - len(line.lstrip())
                for j in range(i + 1, len(lines)):
                    if lines[j].strip() and not lines[j].startswith(' ') and not lines[j].startswith('\t'):
                        # Empty line or top-level statement
                        if lines[j].strip().startswith('def ') or lines[j].strip().startswith('class '):
                            return (start_line, j)
                return (start_line, len(lines))
    
    # If line numbers are specified, use them
    if parsed_changes.get("line_numbers"):
        line_nums = parsed_changes["line_numbers"]
        start = line_nums["old_start"] - 1  # Convert to 0-based
        end = start + line_nums["old_count"]
        return (start, end)
    
    # If old_code is specified, find it in the file
    if parsed_changes.get("old_code"):
        old_code = parsed_changes["old_code"]
        # Try exact match first
        if old_code in file_content:
            start_pos = file_content.find(old_code)
            start_line = file_content[:start_pos].count('\n')
            end_line = start_line + old_code.count('\n') + 1
            return (start_line, end_line)
        
        # Try fuzzy match (find similar code block)
        old_lines = old_code.split('\n')
        for i in range(len(lines) - len(old_lines) + 1):
            if lines[i:i+len(old_lines)] == old_lines:
                return (i, i + len(old_lines))
    
    return None


def apply_code_fix(fix_description: str, code_changes: str, dry_run: bool = False) -> Dict[str, Any]:
    """
    Apply code changes to bulk_migrate_calibre.py with strict safety checks.
    
    Args:
        fix_description: Description of the fix
        code_changes: Code changes to apply (can be full code block, diff, or context-based)
        dry_run: If True, don't actually apply changes
    
    Returns:
        Dictionary with success status and details
    """
    result = {
        "fix_type": "code_fix",
        "timestamp": datetime.now().isoformat(),
        "success": False,
        "message": "",
        "backup_file": None,
        "validation_passed": False
    }
    
    if not ENABLE_CODE_FIXES:
        result["message"] = "Code fixes are disabled in config"
        return result
    
    if dry_run:
        result["message"] = f"[DRY RUN] Would apply code fix: {fix_description}"
        result["success"] = True
        return result
    
    if not code_changes or not code_changes.strip():
        result["message"] = "No code changes provided"
        return result
    
    # Create backup
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = BACKUP_DIR / f"bulk_migrate_calibre.py.backup.{timestamp}"
    
    try:
        # Step 1: Backup original file (MANDATORY)
        if not REQUIRE_BACKUP:
            logger.warning("REQUIRE_BACKUP is False, but enforcing backup for safety")
        shutil.copy2(BULK_MIGRATE_SCRIPT, backup_file)
        result["backup_file"] = str(backup_file)
        logger.info(f"Backup created: {backup_file}")
        
        # Step 2: Read current file
        with open(BULK_MIGRATE_SCRIPT, 'r', encoding='utf-8') as f:
            file_content = f.read()
        
        # Step 3: Parse code changes
        parsed_changes = parse_code_changes(code_changes)
        logger.info(f"Parsed code changes format: {parsed_changes['format']}")
        
        # Step 4: Find location to apply changes
        location = find_code_location(file_content, parsed_changes)
        
        if not location and parsed_changes["format"] != "full_block":
            result["message"] = f"Could not find location to apply code changes. Format: {parsed_changes['format']}"
            logger.error(result["message"])
            # Restore backup
            shutil.copy2(backup_file, BULK_MIGRATE_SCRIPT)
            return result
        
        # Step 5: Apply changes
        lines = file_content.split('\n')
        new_lines = lines.copy()
        
        if parsed_changes["format"] == "full_block" and not location:
            # Append new code at the end (safest for full blocks without context)
            result["message"] = "Full code block provided but no location found. Appending at end of file."
            logger.warning(result["message"])
            new_lines.append("")
            new_lines.extend(parsed_changes["new_code"].split('\n'))
        elif location:
            start_line, end_line = location
            new_code_lines = parsed_changes["new_code"].split('\n') if parsed_changes["new_code"] else []
            # Replace the specified range
            new_lines = lines[:start_line] + new_code_lines + lines[end_line:]
            logger.info(f"Replacing lines {start_line+1}-{end_line} with {len(new_code_lines)} new lines")
        else:
            result["message"] = "Could not determine how to apply code changes"
            logger.error(result["message"])
            shutil.copy2(backup_file, BULK_MIGRATE_SCRIPT)
            return result
        
        # Step 6: Write modified file to temporary location
        temp_file = BULK_MIGRATE_SCRIPT.with_suffix('.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(new_lines))
        
        # Step 7: Validate syntax (MANDATORY)
        if VALIDATE_SYNTAX:
            is_valid, error_msg = validate_python_syntax(temp_file)
            if not is_valid:
                result["message"] = f"Syntax validation failed: {error_msg}"
                result["validation_passed"] = False
                logger.error(result["message"])
                # Restore backup
                shutil.copy2(backup_file, BULK_MIGRATE_SCRIPT)
                temp_file.unlink()  # Clean up temp file
                return result
            result["validation_passed"] = True
            logger.info("Syntax validation passed")
        
        # Step 8: Apply changes (replace original with validated temp file)
        shutil.move(str(temp_file), str(BULK_MIGRATE_SCRIPT))
        logger.info(f"Code fix applied successfully: {fix_description}")
        
        result["success"] = True
        result["message"] = f"Code fix applied successfully. Backup: {backup_file}"
        result["changes_applied"] = {
            "format": parsed_changes["format"],
            "lines_modified": location if location else "appended",
            "function_name": parsed_changes.get("function_name"),
            "code_changes_preview": code_changes[:500] if code_changes else None  # First 500 chars for logging
        }
        
        # Log the code changes that were applied
        logger.info(f"📝 Code Fix Applied:")
        logger.info(f"   Format: {parsed_changes['format']}")
        if location:
            logger.info(f"   Lines Modified: {location[0]+1}-{location[1]}")
        if parsed_changes.get("function_name"):
            logger.info(f"   Function: {parsed_changes['function_name']}")
        logger.info(f"   Code Preview: {code_changes[:200]}..." if len(code_changes) > 200 else f"   Code: {code_changes}")
        
    except Exception as e:
        result["message"] = f"Error applying code fix: {str(e)}"
        logger.exception("Error applying code fix")
        
        # Restore backup if it exists
        if backup_file and backup_file.exists():
            try:
                shutil.copy2(backup_file, BULK_MIGRATE_SCRIPT)
                logger.info("Restored backup after error")
            except Exception as restore_error:
                logger.error(f"Failed to restore backup: {restore_error}")
                result["message"] += f" (CRITICAL: Backup restore failed: {restore_error})"
    
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

