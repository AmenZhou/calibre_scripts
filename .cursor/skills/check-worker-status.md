# Check Worker Status

## Description
Checks the status of migration workers safely with proper timeout handling. This skill ensures worker status checks follow project timeout requirements and provide useful diagnostic information.

## Context
Use this skill when:
- Checking if workers are running
- Diagnosing worker issues
- Monitoring worker health
- Verifying worker restarts

## Steps

1. **Use quick status check with timeout:**
   ```bash
   timeout 10 python3 monitor_migration.py --status 2>&1 | head -20
   ```

2. **For Python scripts checking worker status:**
   ```python
   import subprocess
   
   try:
       result = subprocess.run(
           ['python3', 'monitor_migration.py', '--status'],
           timeout=10,  # Quick check - short timeout
           capture_output=True,
           text=True
       )
       if result.returncode == 0:
           print(result.stdout)
       else:
           print(f"Status check failed: {result.stderr}")
   except subprocess.TimeoutExpired:
       print("Status check timed out - worker may be stuck")
   ```

3. **Check worker log files:**
   ```bash
   tail -50 migration_worker*.log | grep -E "(ERROR|WARN|stuck|timeout)"
   ```

4. **Check running processes:**
   ```bash
   ps aux | grep -E "(bulk_migrate|migration_worker)" | grep -v grep
   ```

5. **Verify worker database activity:**
   - Check for recent database updates
   - Verify worker isn't stuck on same book ID
   - Check for duplicate detection patterns

## Example

**Quick Status Check:**
```bash
# Fast status check (10 second timeout)
timeout 10 python3 monitor_migration.py --status 2>&1 | head -20
```

**Comprehensive Check:**
```bash
# Check running processes
ps aux | grep bulk_migrate | grep -v grep

# Check recent log activity
tail -100 migration_worker_1.log | tail -20

# Check for errors
grep -i error migration_worker*.log | tail -10
```

## Notes
- Always use timeouts for status checks (5-10 seconds for quick checks)
- Don't wait for full dashboard refresh unless necessary
- Check logs for patterns indicating stuck workers
- Use `head` or `tail` to limit output size
- Consider worker cooldown periods when interpreting status
