# Add Timeout to Script

## Description
Adds timeout handling to Python scripts that call `monitor_migration.py` or other potentially long-running scripts. This skill ensures scripts follow the project's timeout requirements.

## Context
Use this skill when:
- Creating new scripts that call `monitor_migration.py`
- Modifying existing scripts to add timeout protection
- Working with subprocess calls that might hang

## Steps

1. **Identify the script call location**
   - Find where `monitor_migration.py` or similar scripts are called
   - Check if it's a direct subprocess call or shell command

2. **For subprocess.run() calls:**
   ```python
   import subprocess
   
   try:
       result = subprocess.run(
           ['python3', 'monitor_migration.py'],
           timeout=30,  # 30 seconds for quick checks, 60 for full dashboard
           capture_output=True,
           text=True
       )
       # Process result
   except subprocess.TimeoutExpired:
       # Handle timeout appropriately
       print("Script timed out after 30 seconds")
       # Log or handle as needed
   ```

3. **For shell commands:**
   ```bash
   timeout 30 python3 monitor_migration.py 2>&1 | head -20
   ```

4. **Choose appropriate timeout:**
   - Quick status checks: 5-10 seconds
   - Standard checks: 30 seconds
   - Full dashboard refresh: 60 seconds

5. **Add error handling:**
   - Log timeout events
   - Provide meaningful error messages
   - Consider retry logic if appropriate

## Example

**Before:**
```python
result = subprocess.run(['python3', 'monitor_migration.py'])
```

**After:**
```python
try:
    result = subprocess.run(
        ['python3', 'monitor_migration.py'],
        timeout=30,
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        print(result.stdout)
    else:
        print(f"Error: {result.stderr}")
except subprocess.TimeoutExpired:
    print("monitor_migration.py timed out after 30 seconds")
    # Handle timeout case
```

## Notes
- Always use timeouts for `monitor_migration.py` calls per project rules
- Adjust timeout values based on expected script duration
- Consider using context managers for better resource cleanup
- Document timeout values and reasoning in code comments
