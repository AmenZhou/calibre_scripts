# Auto-Monitor for MyBookshelf2 Migration Workers

Automatically monitors workers for stuck conditions and applies fixes. Can use LLM (OpenAI) to analyze and debug issues.

## Features

- **Automatic Detection**: Monitors each worker individually for "no uploads for 5+ minutes"
- **Stopped Worker Detection**: Automatically detects and restarts workers that have stopped running (have progress files but are not running)
- **LLM-Powered Debugging**: Uses OpenAI API to analyze logs and identify root causes
- **Auto-Fix**: Automatically applies fixes (restarts, code changes, config updates)
- **Automatic Code Fix Application**: Automatically applies code fixes with strict safety checks:
  - **Mandatory Backup**: Always creates timestamped backup before changes
  - **Syntax Validation**: Validates Python syntax before applying (AST + py_compile)
  - **Automatic Rollback**: Restores backup if validation fails
  - **Multiple Format Support**: Handles function replacements, context-based changes, and diff formats
  - **Maximum 3 Attempts**: Stops after 3 failed fix attempts per worker (configurable)
- **Independent Module**: Standalone folder, easy to enable/disable
- **Safety Features**: Cooldown periods, backups, syntax validation, attempt limits

## Setup

### 1. Install Dependencies

```bash
# Optional: For LLM features
pip install openai
```

### 2. Configure

Edit `config.py` or set environment variables:

```bash
export OPENAI_API_KEY="your-api-key-here"  # Optional, for LLM features
```

### 3. Start Auto-Monitor

**Basic mode (restart only):**
```bash
cd mybookshelf2/auto_monitor
./start.sh
```

**With LLM debugging:**
```bash
./start.sh --llm-enabled
```

**Dry-run mode (test without applying fixes):**
```bash
./start.sh --llm-enabled --dry-run
```

### 4. Stop Auto-Monitor

```bash
./stop.sh
```

## Usage

### Command-Line Options

```bash
python3 monitor.py [options]

Options:
  --llm-enabled      Enable LLM-powered debugging (requires OpenAI API key)
  --dry-run         Test mode: detect but don't apply fixes
  --check-interval  Seconds between checks (default: 60)
  --threshold       Minutes before considering worker stuck (default: 5)
```

### Examples

**Basic monitoring (restart stuck workers):**
```bash
./start.sh
```

**Full auto-fix with LLM:**
```bash
export OPENAI_API_KEY="sk-..."
./start.sh --llm-enabled
```

**Test detection:**
```bash
./start.sh --dry-run
```

## Monitoring

**View logs:**
```bash
tail -f auto_restart.log      # Auto-fix actions
tail -f monitor.log           # Monitor process logs
```

**View fix history:**
```bash
cat auto_fix_history.json | jq
```

**Check status:**
```bash
ps aux | grep "auto_monitor/monitor.py"
```

## How It Works

1. **Disk I/O Monitoring & Worker Scaling**: Every 60 seconds, checks disk I/O utilization and automatically adjusts worker count
2. **Stopped Worker Detection**: Checks for workers that have progress files but are not running, and automatically restarts them
3. **Stuck Worker Detection**: Checks each running worker's last upload time
4. **Analysis**: If stuck for 5+ minutes:
   - Collects diagnostic data (logs, errors, book.id ranges)
   - If LLM enabled: Sends to OpenAI for analysis
   - Identifies root cause and fix type
5. **Fix Application**:
   - **Restart**: Calls `restart_worker.sh` (default)
   - **Code Fix**: Applies code changes with backup (if LLM suggests)
   - **Config Fix**: Updates worker parameters
6. **Safety**: Cooldown period (10 min) prevents fix spam

## Worker Scaling Based on Disk I/O

The auto-monitor automatically adjusts the number of workers based on disk I/O utilization to prevent disk saturation and optimize performance.

### Scale-Down (Reduce Workers)

**When it triggers:**
- Disk I/O utilization >= 90% (saturated)
- AND workers are stuck (no uploads for 5+ minutes)
- AND current worker count > minimum (1)

**How it works:**
1. Checks if any workers are stuck
2. If LLM enabled: Analyzes stuck workers to determine if disk I/O is the root cause
3. **Fallback behavior**: If LLM returns "Unknown" but disk I/O >= 90% and workers are stuck, still scales down (disk I/O is clearly saturated)
4. Kills the highest ID worker (reduces by 1)
5. Updates desired worker count

**Cooldown**: 5 minutes between scale-downs (prevents rapid oscillation)

**Minimum**: Never scales below `MIN_WORKER_COUNT` (1 worker)

**Example:**
```
Disk I/O: 94.4% (saturated)
Workers stuck: Worker 4 (no uploads for 20 minutes)
LLM analysis: Returns "Unknown" root cause
→ Still scales down: Kills Worker 4 (disk I/O clearly saturated)
```

### Scale-Up (Increase Workers)

**When it triggers:**
- Disk I/O utilization < 50% (normal)
- AND current worker count < target count (4)
- AND target count < maximum (8)

**How it works:**
1. Checks current disk I/O utilization
2. If below threshold and below target count, starts a new worker
3. Increases desired worker count by 1
4. Finds next available worker ID and starts it

**Cooldown**: 10 minutes between scale-ups (prevents rapid oscillation)

**Maximum**: Never scales above `MAX_WORKER_COUNT` (8 workers)

**Target**: Maintains `TARGET_WORKER_COUNT` (4 workers) when disk I/O is normal

**Example:**
```
Disk I/O: 45% (normal)
Current workers: 2
Target workers: 4
→ Scales up: Starts Worker 3
→ After 10 min cooldown, if still < 50%, starts Worker 4
```

### Configuration Parameters

Edit `config.py` to customize scaling behavior:

```python
TARGET_WORKER_COUNT = 4          # Desired number of workers (default: 4)
MIN_WORKER_COUNT = 1             # Minimum workers (never scale below)
MAX_WORKER_COUNT = 8             # Maximum workers (never scale above)
DISK_IO_SATURATED_THRESHOLD = 90 # Disk utilization % for scale-down (default: 90%)
DISK_IO_NORMAL_THRESHOLD = 50    # Disk utilization % for scale-up (default: 50%)
DISK_IO_SCALE_DOWN_COOLDOWN = 300 # 5 minutes - cooldown before scaling down again
DISK_IO_SCALE_UP_COOLDOWN = 600   # 10 minutes - cooldown before scaling up again
```

### Scaling Behavior Examples

**Scenario 1: Disk I/O Saturation**
- Initial: 4 workers running, disk I/O 92%
- Workers 2, 3, 4 stuck (waiting on disk I/O)
- Auto-monitor: Scales down to 3 workers (kills Worker 4)
- Result: Disk I/O drops to 75%, workers recover

**Scenario 2: Gradual Scale-Up**
- Initial: 2 workers after scale-down, disk I/O 40%
- Auto-monitor: After 10 min cooldown, scales up to 3 workers
- Disk I/O: 55% (still normal)
- Auto-monitor: After another 10 min, scales up to 4 workers (target)
- Result: Maintains 4 workers at target count

**Scenario 3: LLM "Unknown" Fallback**
- Disk I/O: 94%, Worker 1 stuck
- LLM analysis: Returns "Unknown" root cause (insufficient log context)
- Auto-monitor: Still scales down (fallback logic - disk I/O clearly saturated)
- Result: Worker killed, disk I/O improves

## LLM Fix Types

The auto-monitor supports **3 types of fixes** that the LLM can recommend and apply:

### 1. 🔄 RESTART (Default Fix)

**What it does**: Restarts the worker process

**When used**: 
- **Stopped Workers**: Automatically restarts workers that have stopped running (have progress files but are not running)
- Default fallback when no LLM analysis is available
- When LLM recommends restart for transient issues

**How it works**:
- Calls `restart_worker.sh` script
- Stops the stuck/stopped worker
- Reads `last_processed_book_id` from progress file
- Restarts worker from where it left off
- Uses 1 parallel upload by default to reduce memory usage

**Use cases**:
- **Worker stopped unexpectedly** (OOM kill, crash, etc.)
- Transient errors
- Memory leaks
- Connection issues
- Unknown issues (safe fallback)

**Stopped Worker Detection**:
- Checks for progress files (`migration_progress_worker*.json`)
- Compares with actually running workers
- Automatically restarts stopped workers (respects cooldown and pause status)

### 2. 🔧 CODE_FIX (Automatic Code Changes)

**What it does**: Automatically modifies `bulk_migrate_calibre.py` to fix bugs

**When used**: When LLM identifies a code bug that requires code changes

**How it works**:
1. LLM analyzes logs and identifies root cause
2. LLM provides code changes in one of these formats:
   - **Function replacement**: Complete function definition starting with `def function_name(...)`
   - **Context replacement**: `old_string: [exact code to replace]` and `new_string: [replacement code]`
   - **Diff format**: `@@ -start_line,count +start_line,count` with code changes
3. System automatically:
   - Creates timestamped backup in `auto_monitor/backups/`
   - Parses and applies code changes
   - Validates Python syntax (AST + py_compile)
   - Rolls back if validation fails
   - Restarts worker after successful fix

**Use cases**:
- Infinite loops (e.g., `last_processed_book_id` not advancing)
- Logic errors in database queries
- Missing error handling
- Performance issues (inefficient algorithms)
- Bug fixes (e.g., NUL character handling, path length issues)

**Example**: If worker is stuck in an infinite loop querying the same `book.id` range, LLM can detect this and provide a fix to advance `last_processed_book_id` correctly.

### 3. ⚙️ CONFIG_FIX (Configuration Changes)

**What it does**: Changes worker parameters and restarts with new configuration

**When used**: When LLM recommends parameter tuning to resolve issues

**How it works**:
- LLM suggests config changes (e.g., `parallel_uploads`, `batch_size`)
- System restarts worker with new parameters

**Supported parameters**:
- `parallel_uploads`: Number of concurrent uploads (1-10)
- `batch_size`: Books per batch (default: 1000)

**Use cases**:
- Too many parallel uploads causing memory issues → Reduce `parallel_uploads`
- Too few parallel uploads causing slow performance → Increase `parallel_uploads`
- Batch size optimization for better performance

**Example**: If worker is running out of memory, LLM can detect this and suggest reducing `parallel_uploads` from 5 to 2.

## LLM Analysis Capabilities

The LLM analyzes worker logs and can detect:

- **Infinite loops**: Same book.id range repeated in logs
- **API errors**: 500 errors, connection failures, timeout errors
- **Database query issues**: Slow queries, query errors, database locks
- **Memory or performance problems**: High memory usage, slow processing
- **Error patterns**: Repeated errors, exception patterns
- **Stuck conditions**: Workers not making progress despite running

The LLM provides:
- **Root cause identification**: Brief description of the problem
- **Fix recommendation**: One of `restart`, `code_fix`, or `config_fix`
- **Confidence score**: 0-1 indicating how confident the LLM is in the fix
- **Fix description**: Detailed explanation of what the fix does
- **Code changes**: Complete code changes (for code fixes)
- **Config changes**: Parameter changes (for config fixes)

## Automatic Code Fix Safety

The auto-monitor includes **strict safety checks** for automatic code fixes:

1. **Mandatory Backup**: Every code fix creates a timestamped backup in `auto_monitor/backups/`
2. **Syntax Validation**: 
   - Validates using Python AST parser
   - Validates using `py_compile` module
   - Only applies changes if validation passes
3. **Automatic Rollback**: If validation fails, automatically restores the backup
4. **Maximum 3 Attempts**: After 3 failed fix attempts, the worker is paused/stopped (configurable)
5. **Cooldown Period**: 10-minute cooldown between fixes for the same worker
6. **Format Support**: Handles multiple code change formats:
   - Function replacements (complete function definitions)
   - Context-based replacements (old_string → new_string)
   - Diff format (with line numbers)

### Code Fix Process

1. **Detection**: Worker detected as stuck (no uploads for 5+ minutes)
2. **LLM Analysis**: If enabled, LLM analyzes logs and suggests fix
3. **Backup**: Create timestamped backup of `bulk_migrate_calibre.py`
4. **Parse Changes**: Parse code changes from LLM response
5. **Apply Changes**: Apply changes to temporary file
6. **Validate**: Validate Python syntax (AST + py_compile)
7. **Commit or Rollback**: 
   - If valid: Replace original file
   - If invalid: Restore backup, log error
8. **Verify**: Wait 2 minutes, check if worker recovered
9. **Escalate**: After 3 failed attempts, pause/stop worker

## Configuration

Edit `config.py` to customize:

- `STUCK_THRESHOLD_SECONDS`: Time before considering worker stuck (default: 300 = 5 min) - for workers that have uploaded before
- `DISCOVERY_THRESHOLD_SECONDS`: Time before considering worker stuck in discovery/initialization phase (default: 1200 = 20 min) - allows workers time to discover files before first upload
- `COOLDOWN_SECONDS`: Minimum time between fixes per worker (default: 600 = 10 min)
- `CHECK_INTERVAL_SECONDS`: How often to check workers (default: 60 seconds)
- `MAX_FIX_ATTEMPTS`: Maximum fix attempts per worker before escalation (default: 3)
- `SUCCESS_VERIFICATION_SECONDS`: Time to wait after fix to verify success (default: 120 = 2 min)
- `ESCALATION_ACTION`: Action after max attempts - "alert_and_pause" (default), "stop_worker", or "try_different_fix"
- `ENABLE_CODE_FIXES`: Allow automatic code fixes (default: True)
- `ENABLE_CONFIG_FIXES`: Allow automatic config changes (default: True)

## Safety Features

- **Status-Aware Thresholds**: 
  - **5 minutes** for workers that have uploaded before (normal operation)
  - **20 minutes** for workers in discovery/initialization phase (allows time for database queries and file discovery)
- **Progress Detection**: Recognizes "Processed batch", "Found X new files", and database query activity as progress indicators
- **Cooldown**: Don't fix same worker more than once per 10 minutes
- **Max Attempts**: Stop trying after 3 failed fix attempts (configurable)
- **Escalation**: After max attempts, pause worker or stop it (configurable)
- **Success Verification**: Verifies worker recovered after fix (2 minutes wait)
- **Backups**: All code changes backed up with timestamp
- **Syntax Validation**: Python syntax checked before applying code fixes
- **Rollback**: Can restore backup if fix fails
- **Dry-Run**: Test mode to verify detection without applying fixes

## Troubleshooting

**Auto-monitor not starting:**
- Check `monitor.log` for errors
- Verify Python 3 is available
- Check file permissions on scripts

**LLM not working:**
- Verify `OPENAI_API_KEY` is set
- Install `openai` package: `pip install openai`
- Check API key is valid

**Workers not being detected:**
- Verify workers are running: `ps aux | grep bulk_migrate_calibre`
- Check log files exist: `ls migration_worker*.log`

## LLM Fix Logging

When an LLM fix is applied, comprehensive logging includes:

- **Root cause**: What the LLM identified as the problem
- **Fix type**: `restart`, `code_fix`, or `config_fix`
- **Confidence score**: LLM's confidence in the fix (0-1)
- **Fix description**: Detailed explanation of the fix
- **Code changes**: Full code changes applied (for code fixes)
- **Config changes**: Parameter changes (for config fixes)
- **Changes applied**: Details about what was modified (lines, functions, etc.)

All LLM fix details are saved to:
- **Logs**: `auto_restart.log` (real-time monitoring)
- **History**: `auto_fix_history.json` (complete fix history with all details)

View recent LLM fixes:
```bash
# View real-time logs
tail -f auto_restart.log | grep -A 20 "LLM Fix Summary"

# View fix history
cat auto_fix_history.json | jq '.[] | select(.llm_applied == true)'
```

## Files

- `monitor.py`: Main monitoring script
- `llm_debugger.py`: LLM integration for debugging
- `fix_applier.py`: Apply fixes (restart, code, config)
- `config.py`: Configuration settings
- `start.sh` / `stop.sh`: Easy enable/disable scripts
- `.env`: OpenAI API key (not in git)
- `auto_restart.log`: Log of all auto-fix actions
- `auto_fix_history.json`: History of all fixes applied (includes LLM details)
- `backups/`: Timestamped backups of code changes

