# Auto-Monitor for MyBookshelf2 Migration Workers

Automatically monitors workers for stuck conditions and applies fixes. Can use LLM (OpenAI) to analyze and debug issues.

## Features

- **Automatic Detection**: Monitors each worker individually for "no uploads for 5+ minutes"
- **LLM-Powered Debugging**: Uses OpenAI API to analyze logs and identify root causes
- **Auto-Fix**: Automatically applies fixes (restarts, code changes, config updates)
- **Independent Module**: Standalone folder, easy to enable/disable
- **Safety Features**: Cooldown periods, backups, syntax validation

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

1. **Detection**: Every 60 seconds, checks each worker's last upload time
2. **Analysis**: If stuck for 5+ minutes:
   - Collects diagnostic data (logs, errors, book.id ranges)
   - If LLM enabled: Sends to OpenAI for analysis
   - Identifies root cause and fix type
3. **Fix Application**:
   - **Restart**: Calls `restart_worker.sh` (default)
   - **Code Fix**: Applies code changes with backup (if LLM suggests)
   - **Config Fix**: Updates worker parameters
4. **Safety**: Cooldown period (10 min) prevents fix spam

## Configuration

Edit `config.py` to customize:

- `STUCK_THRESHOLD_SECONDS`: Time before considering worker stuck (default: 300 = 5 min)
- `COOLDOWN_SECONDS`: Minimum time between fixes per worker (default: 600 = 10 min)
- `CHECK_INTERVAL_SECONDS`: How often to check workers (default: 60 seconds)
- `MAX_FIX_ATTEMPTS`: Maximum fix attempts per worker before escalation (default: 3)
- `SUCCESS_VERIFICATION_SECONDS`: Time to wait after fix to verify success (default: 120 = 2 min)
- `ESCALATION_ACTION`: Action after max attempts - "alert_and_pause" (default), "stop_worker", or "try_different_fix"
- `ENABLE_CODE_FIXES`: Allow automatic code fixes (default: True)
- `ENABLE_CONFIG_FIXES`: Allow automatic config changes (default: True)

## Safety Features

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

## Files

- `monitor.py`: Main monitoring script
- `llm_debugger.py`: LLM integration for debugging
- `fix_applier.py`: Apply fixes (restart, code, config)
- `config.py`: Configuration settings
- `start.sh` / `stop.sh`: Easy enable/disable scripts
- `auto_restart.log`: Log of all auto-fix actions
- `auto_fix_history.json`: History of all fixes applied
- `backups/`: Timestamped backups of code changes

