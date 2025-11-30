"""
LLM-powered debugging for stuck workers
Uses OpenAI API to analyze worker logs and suggest fixes
"""
import json
import re
from typing import Dict, Any, Optional
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
        OPENAI_MAX_TOKENS, OPENAI_TEMPERATURE
    )
except ImportError:
    from config import (
        OPENAI_API_KEY, OPENAI_MODEL, OPENAI_MODEL_FALLBACK,
        OPENAI_MAX_TOKENS, OPENAI_TEMPERATURE
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
    
    prompt += f"""

Recent logs (last 500 lines):
{logs}

Error patterns detected:
{chr(10).join(f"- {pattern}" for pattern in error_patterns[:10])}

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

