# Create Python Script

## Description
Creates a new Python script following the project's conventions, including proper logging, error handling, timeout support, and documentation.

## Context
Use this skill when:
- Creating new automation scripts
- Adding new utility scripts
- Creating worker scripts
- Building monitoring or maintenance scripts

## Steps

1. **Create the script file with proper structure:**
   ```python
   #!/usr/bin/env python3
   """
   Brief description of what the script does.
   
   Usage:
       python3 script_name.py [options]
   
   Examples:
       python3 script_name.py --option value
   """
   
   import argparse
   import logging
   import sys
   from pathlib import Path
   
   # Setup logging
   def setup_logging(level=logging.INFO):
       logging.basicConfig(
           level=level,
           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
           datefmt='%Y-%m-%d %H:%M:%S'
       )
       return logging.getLogger(__name__)
   
   def main():
       parser = argparse.ArgumentParser(description='Script description')
       parser.add_argument('--option', help='Option description')
       parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
       args = parser.parse_args()
       
       log_level = logging.DEBUG if args.verbose else logging.INFO
       logger = setup_logging(log_level)
       
       try:
           # Main script logic here
           logger.info("Script started")
           # ... your code ...
           logger.info("Script completed successfully")
           return 0
       except Exception as e:
           logger.error(f"Script failed: {e}", exc_info=True)
           return 1
   
   if __name__ == '__main__':
       sys.exit(main())
   ```

2. **Add timeout handling if calling other scripts:**
   ```python
   import subprocess
   
   try:
       result = subprocess.run(
           ['python3', 'other_script.py'],
           timeout=30,
           capture_output=True,
           text=True
       )
   except subprocess.TimeoutExpired:
       logger.error("Subprocess timed out")
   ```

3. **Include error handling:**
   - Use try/except blocks for error-prone operations
   - Log errors with context
   - Provide meaningful error messages
   - Return appropriate exit codes

4. **Add argument parsing:**
   - Use argparse for command-line arguments
   - Include help text for all options
   - Validate input arguments
   - Provide usage examples

5. **Follow project conventions:**
   - Use logging instead of print statements
   - Include docstrings
   - Follow Python style guidelines (PEP 8)
   - Add type hints where appropriate

## Example

```python
#!/usr/bin/env python3
"""
Worker status checker with timeout protection.

Checks the status of migration workers and reports their health.
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def check_worker_status(timeout=10):
    """Check worker status with timeout."""
    logger = logging.getLogger(__name__)
    try:
        result = subprocess.run(
            ['python3', 'monitor_migration.py', '--status'],
            timeout=timeout,
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            logger.info("Worker status check successful")
            print(result.stdout)
            return True
        else:
            logger.error(f"Status check failed: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"Status check timed out after {timeout} seconds")
        return False

def main():
    parser = argparse.ArgumentParser(description='Check migration worker status')
    parser.add_argument('--timeout', type=int, default=10, help='Timeout in seconds')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    args = parser.parse_args()
    
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger = setup_logging(log_level)
    
    logger.info("Starting worker status check")
    success = check_worker_status(args.timeout)
    return 0 if success else 1

if __name__ == '__main__':
    sys.exit(main())
```

## Notes
- Always include shebang line for executable scripts
- Use logging instead of print for production scripts
- Include proper error handling and exit codes
- Add timeout handling for subprocess calls
- Follow project's timeout requirements
- Make scripts executable: `chmod +x script_name.py`
- Include usage examples in docstrings
