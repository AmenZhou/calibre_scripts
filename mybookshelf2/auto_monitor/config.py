"""
Configuration settings for auto-monitor
"""
import os
from pathlib import Path

# Base directory (parent of auto_monitor folder)
BASE_DIR = Path(__file__).parent.parent

# Load .env file if it exists
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    with open(_env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()

# Worker monitoring settings
STUCK_THRESHOLD_SECONDS = 300  # 5 minutes - time before considering worker stuck (for workers that have uploaded)
DISCOVERY_THRESHOLD_SECONDS = 1200  # 20 minutes - time before considering worker stuck in discovery/initialization phase
COOLDOWN_SECONDS = 600  # 10 minutes - minimum time between fixes for same worker
CHECK_INTERVAL_SECONDS = 60  # Check workers every 60 seconds
MAX_FIX_ATTEMPTS = 3  # Maximum number of fix attempts per worker before escalation
SUCCESS_VERIFICATION_SECONDS = 120  # 2 minutes - time to wait after fix to verify success
ESCALATION_ACTION = "alert_and_pause"  # Options: "alert_and_pause", "stop_worker", "try_different_fix"

# LLM settings
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = "gpt-4-turbo-preview"  # Primary model
OPENAI_MODEL_FALLBACK = "gpt-3.5-turbo"  # Fallback if primary unavailable
OPENAI_MAX_TOKENS = 2000
OPENAI_TEMPERATURE = 0.3  # Lower temperature for more deterministic fixes

# File paths
LOG_FILE = BASE_DIR / "auto_monitor" / "auto_restart.log"
HISTORY_FILE = BASE_DIR / "auto_monitor" / "auto_fix_history.json"
WORKER_LOG_DIR = BASE_DIR  # Where migration_worker*.log files are located (parent of auto_monitor)
RESTART_SCRIPT = BASE_DIR / "restart_worker.sh"
BULK_MIGRATE_SCRIPT = BASE_DIR / "bulk_migrate_calibre.py"
BACKUP_DIR = BASE_DIR / "auto_monitor" / "backups"

# Log collection settings
LOG_LINES_TO_ANALYZE = 500  # Number of log lines to send to LLM
MAX_LOG_SIZE_MB = 10  # Maximum log file size to read (avoid huge files)

# Safety settings
ENABLE_CODE_FIXES = True  # Allow automatic code fixes
ENABLE_CONFIG_FIXES = True  # Allow automatic config changes
REQUIRE_BACKUP = True  # Always backup before code changes
VALIDATE_SYNTAX = True  # Validate Python syntax before applying code fixes

