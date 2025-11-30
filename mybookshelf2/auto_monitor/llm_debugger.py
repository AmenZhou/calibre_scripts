"""
LLM-powered debugging for stuck workers
Uses OpenAI API to analyze worker logs and suggest fixes
"""
import json
import re
import ast
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from pathlib import Path

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# Handle imports for both script and module usage
try:
    from .config import (
        OPENAI_API_KEY, OPENAI_MODEL, OPENAI_MODEL_FALLBACK,
        OPENAI_MAX_TOKENS, OPENAI_TEMPERATURE, BULK_MIGRATE_SCRIPT,
        MAX_CODE_SNIPPET_LINES, ENABLE_CODE_SNIPPETS
    )
except ImportError:
    from config import (
        OPENAI_API_KEY, OPENAI_MODEL, OPENAI_MODEL_FALLBACK,
        OPENAI_MAX_TOKENS, OPENAI_TEMPERATURE, BULK_MIGRATE_SCRIPT,
        MAX_CODE_SNIPPET_LINES, ENABLE_CODE_SNIPPETS
    )


def analyze_worker_with_llm(worker_id: int, logs: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Analyze a stuck worker using LLM to identify root cause and suggest fixes.
    
    Args:
        worker_id: Worker ID
        logs: Recent log lines from worker
        context: Additional context (book.id ranges, error patterns, etc.)
    
    Returns:
        Dictionary with analysis results, or None if LLM unavailable
    """
    if not OPENAI_AVAILABLE:
        return None
    
    if not OPENAI_API_KEY:
        return None
    
    try:
        # Build prompt
        prompt = build_analysis_prompt(worker_id, logs, context)
        
        # Call OpenAI API (using newer API if available)
        try:
            # Try new OpenAI API (v1.0+)
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            response = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "You are a Python debugging expert specializing in database migration scripts. Analyze worker logs and provide specific, actionable fixes."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=OPENAI_MAX_TOKENS,
                temperature=OPENAI_TEMPERATURE
            )
            analysis_text = response.choices[0].message.content
        except (ImportError, AttributeError):
            # Fallback to old API
            openai.api_key = OPENAI_API_KEY
            response = openai.ChatCompletion.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "You are a Python debugging expert specializing in database migration scripts. Analyze worker logs and provide specific, actionable fixes."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=OPENAI_MAX_TOKENS,
                temperature=OPENAI_TEMPERATURE
            )
            analysis_text = response.choices[0].message.content
        
        # Parse response
        return parse_llm_response(analysis_text, worker_id, context)
        
    except Exception as e:
        # Fallback to simpler model or return None
        try:
            try:
                from openai import OpenAI
                client = OpenAI(api_key=OPENAI_API_KEY)
                response = client.chat.completions.create(
                    model=OPENAI_MODEL_FALLBACK,
                    messages=[
                        {"role": "system", "content": "You are a Python debugging expert."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=OPENAI_MAX_TOKENS,
                    temperature=OPENAI_TEMPERATURE
                )
                analysis_text = response.choices[0].message.content
            except:
                openai.api_key = OPENAI_API_KEY
                response = openai.ChatCompletion.create(
                    model=OPENAI_MODEL_FALLBACK,
                    messages=[
                        {"role": "system", "content": "You are a Python debugging expert."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=OPENAI_MAX_TOKENS,
                    temperature=OPENAI_TEMPERATURE
                )
                analysis_text = response.choices[0].message.content
            return parse_llm_response(analysis_text, worker_id, context)
        except:
            return None


def build_analysis_prompt(worker_id: int, logs: str, context: Dict[str, Any]) -> str:
    """Build the prompt for LLM analysis"""
    minutes_stuck = context.get("minutes_stuck", 0)
    book_id_range = context.get("book_id_range", "unknown")
    error_patterns = context.get("error_patterns", [])
    last_upload = context.get("last_upload_time", "unknown")
    disk_io_util = context.get("disk_io_utilization")
    disk_io_saturated = context.get("disk_io_saturated", False)
    recurring_root_cause = context.get("recurring_root_cause", False)
    root_cause_occurrence_count = context.get("root_cause_occurrence_count", 0)
    
    prompt = f"""Worker {worker_id} is stuck: no uploads for {minutes_stuck} minutes
Last upload: {last_upload}
Current book.id range: {book_id_range}"""
    
    # Add disk I/O information if available
    if disk_io_util is not None:
        prompt += f"""
Disk I/O Utilization: {disk_io_util:.1f}%
Disk I/O Status: {"SATURATED (>=90%)" if disk_io_saturated else "HIGH (70-90%)" if disk_io_util >= 70 else "NORMAL (<70%)"}"""
        if disk_io_saturated:
            prompt += "\n⚠️  CRITICAL: Disk I/O is saturated - this may be causing worker slowdowns/stalls"
    
    # Add recurring root cause information
    if recurring_root_cause and root_cause_occurrence_count > 0:
        prompt += f"""
⚠️  RECURRING ISSUE: This root cause has appeared {root_cause_occurrence_count} time(s) before.
Strongly consider using "code_fix" to fix it permanently instead of "restart"."""
    
    prompt += f"""

Recent logs (last 500 lines):
{logs}

Error patterns detected:
{chr(10).join(f"- {pattern}" for pattern in error_patterns[:10])}"""
    
    # Get and include relevant code snippets
    root_cause_keywords = context.get("root_cause_keywords", "")
    code_snippets = get_relevant_code_snippets(error_patterns, root_cause_keywords)
    
    if code_snippets:
        prompt += "\n\nRelevant Code Snippets:\n"
        for func_name, (code, start_line, end_line) in code_snippets.items():
            prompt += f"\n--- Function: {func_name} (lines {start_line}-{end_line}) ---\n"
            prompt += code
            prompt += "\n"
    
    prompt += f"""

Analyze the issue and provide a JSON response with:
1. "root_cause": Brief description of the problem (include disk I/O if relevant)
2. "fix_type": One of ["restart", "code_fix", "config_fix", "scale_down"]
   - Use "scale_down" if disk I/O saturation is the root cause
3. "fix_description": Detailed description of the fix
4. "code_changes": If fix_type is "code_fix", provide the exact code changes in one of these formats:
   a) Function replacement: Provide the complete function definition starting with "def function_name(...)"
   b) Context replacement: Provide "old_string: [exact code to replace]" and "new_string: [replacement code]"
   c) Line-based: Provide "@@ -start_line,count +start_line,count" diff format
   Always include enough context (3-5 lines before/after) to uniquely identify the location.
5. "confidence": Confidence level 0-1

Fix Type Decision Guide:
- Use "code_fix" if:
  * Root cause is a clear code bug (infinite loop, missing update, logic error)
  * You can identify the exact function and location to fix (code snippets provided above)
  * Confidence >= 0.7
  * Same root cause has appeared before (recurring issue) - STRONGLY prefer code_fix
- Use "restart" if:
  * Issue is transient or unclear
  * Confidence < 0.7
  * First occurrence of this root cause
- Use "config_fix" if:
  * Issue is parameter-related (parallel_uploads, batch_size)
- Use "scale_down" if:
  * Disk I/O saturation is the root cause

Focus on:
- Disk I/O saturation (if disk_io_utilization >= 90%, this is likely the root cause)
- Infinite loops (same book.id range repeated)
- API errors (500, connection failures)
- Database query issues
- Memory or performance problems

IMPORTANT: If disk I/O utilization is >= 90%, the root cause is likely disk I/O saturation.
In this case, recommend "scale_down" fix_type to reduce the number of workers.

IMPORTANT for code_fix:
- Provide complete, syntactically correct Python code
- Include function name if replacing a function
- Include surrounding context (3-5 lines) to uniquely identify location
- Ensure indentation matches the original file
- Test that the fix addresses the root cause
- If this is a recurring issue, code_fix is strongly preferred over restart

Provide specific, actionable fixes."""
    
    return prompt


def parse_llm_response(response_text: str, worker_id: int, context: Dict[str, Any]) -> Dict[str, Any]:
    """Parse LLM response and extract fix information"""
    result = {
        "worker_id": worker_id,
        "timestamp": datetime.now().isoformat(),
        "root_cause": "Unknown",
        "fix_type": "restart",  # Default to restart
        "fix_description": "",
        "code_changes": None,
        "confidence": 0.5
    }
    
    # Try to extract JSON from response
    json_match = re.search(r'\{[^}]+\}', response_text, re.DOTALL)
    if json_match:
        try:
            parsed = json.loads(json_match.group(0))
            result.update(parsed)
        except json.JSONDecodeError:
            pass
    
    # Extract code blocks if present
    code_blocks = re.findall(r'```(?:python)?\n(.*?)\n```', response_text, re.DOTALL)
    if code_blocks and result.get("fix_type") == "code_fix":
        result["code_changes"] = code_blocks[0]
    
    # Extract root cause if not in JSON
    if result["root_cause"] == "Unknown":
        cause_match = re.search(r'root[_\s]cause[:\s]+(.+?)(?:\n|$)', response_text, re.IGNORECASE)
        if cause_match:
            result["root_cause"] = cause_match.group(1).strip()
    
    return result


def get_relevant_code_snippets(error_patterns: List[str], root_cause_keywords: str = "") -> Dict[str, Tuple[str, int, int]]:
    """
    Extract relevant code snippets from bulk_migrate_calibre.py based on error patterns.
    
    Args:
        error_patterns: List of error patterns detected in logs
        root_cause_keywords: Keywords from root cause description
    
    Returns:
        Dictionary mapping function names to (code_snippet, start_line, end_line)
    """
    if not ENABLE_CODE_SNIPPETS:
        return {}
    
    # Map patterns to relevant functions
    pattern_to_functions = {
        # book.id / infinite loop / last_processed_book_id issues
        "book.id": ["find_ebook_files_from_database"],
        "infinite loop": ["find_ebook_files_from_database"],
        "last_processed_book_id": ["find_ebook_files_from_database"],
        "book_id": ["find_ebook_files_from_database"],
        "database query": ["find_ebook_files_from_database"],
        "sql": ["find_ebook_files_from_database"],
        "offset": ["find_ebook_files_from_database"],
        
        # API errors
        "api error": ["upload_file", "check_file_exists_via_api"],
        "500": ["upload_file", "check_file_exists_via_api"],
        "connection": ["upload_file", "check_file_exists_via_api"],
        "websocket": ["upload_file"],
        
        # NUL character issues
        "nul": ["sanitize_metadata_string", "prepare_file_for_upload"],
        "null character": ["sanitize_metadata_string", "prepare_file_for_upload"],
        "0x00": ["sanitize_metadata_string", "prepare_file_for_upload"],
        
        # File name too long
        "file name too long": ["prepare_file_for_upload"],
        "filename too long": ["prepare_file_for_upload"],
        "path too long": ["prepare_file_for_upload"],
        "symlink": ["prepare_file_for_upload"],
    }
    
    # Find relevant functions based on patterns
    relevant_functions = set()
    all_keywords = " ".join(error_patterns + [root_cause_keywords]).lower()
    
    for pattern, functions in pattern_to_functions.items():
        if pattern in all_keywords:
            relevant_functions.update(functions)
    
    if not relevant_functions:
        return {}
    
    # Read the source file
    source_file = Path(BULK_MIGRATE_SCRIPT) if isinstance(BULK_MIGRATE_SCRIPT, Path) else Path(BULK_MIGRATE_SCRIPT)
    if not source_file.exists():
        return {}
    
    try:
        with open(source_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception:
        return {}
    
    # Extract function code using AST
    snippets = {}
    try:
        tree = ast.parse(''.join(lines))
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                func_name = node.name
                if func_name in relevant_functions:
                    # Get line numbers (AST uses 1-based indexing)
                    start_line = node.lineno - 1  # Convert to 0-based for list indexing
                    # Find end line by looking for next function or end of class
                    end_line = len(lines)
                    
                    # Look for next function or class definition
                    for i in range(start_line + 1, len(lines)):
                        # Check if this line starts a new function or class (at same or less indentation)
                        line = lines[i]
                        stripped = line.lstrip()
                        if stripped.startswith('def ') or stripped.startswith('class '):
                            # Check indentation
                            if len(line) - len(stripped) <= len(lines[start_line]) - len(lines[start_line].lstrip()):
                                end_line = i
                                break
                    
                    # Extract function code with context
                    context_before = 5
                    context_after = 5
                    snippet_start = max(0, start_line - context_before)
                    snippet_end = min(len(lines), end_line + context_after)
                    
                    # Limit snippet size
                    max_lines = MAX_CODE_SNIPPET_LINES if MAX_CODE_SNIPPET_LINES else 500
                    if snippet_end - snippet_start > max_lines:
                        snippet_end = snippet_start + max_lines
                    
                    code_snippet = ''.join(lines[snippet_start:snippet_end])
                    snippets[func_name] = (code_snippet, snippet_start + 1, snippet_end)  # 1-based line numbers
                    
    except Exception:
        # Fallback to regex-based extraction if AST fails
        for func_name in relevant_functions:
            pattern = rf'^\s*def\s+{func_name}\s*\([^)]*\)\s*:'
            for i, line in enumerate(lines):
                if re.match(pattern, line):
                    # Found function start, extract until next function/class or end
                    start_line = i
                    end_line = len(lines)
                    
                    # Find end of function
                    indent_level = len(line) - len(line.lstrip())
                    for j in range(i + 1, len(lines)):
                        next_line = lines[j]
                        if next_line.strip():  # Non-empty line
                            next_indent = len(next_line) - len(next_line.lstrip())
                            if next_indent <= indent_level and (next_line.strip().startswith('def ') or next_line.strip().startswith('class ')):
                                end_line = j
                                break
                    
                    # Extract with context
                    context_before = 5
                    context_after = 5
                    snippet_start = max(0, start_line - context_before)
                    snippet_end = min(len(lines), end_line + context_after)
                    
                    # Limit snippet size
                    max_lines = MAX_CODE_SNIPPET_LINES if MAX_CODE_SNIPPET_LINES else 500
                    if snippet_end - snippet_start > max_lines:
                        snippet_end = snippet_start + max_lines
                    
                    code_snippet = ''.join(lines[snippet_start:snippet_end])
                    snippets[func_name] = (code_snippet, snippet_start + 1, snippet_end)  # 1-based line numbers
                    break
    
    return snippets

