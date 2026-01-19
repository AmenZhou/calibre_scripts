# Changelog

All notable changes to the Calibre Automation Scripts and MyBookshelf2 migration system.

## [2026-01-19] - Monitor Database Query Fix

### Fixed
- **Database count query timeout**: Fixed monitor dashboard showing 0 total books for large databases
  - Problem: Database queries were timing out after 5 seconds for databases with 1M+ records
  - Solution: Increased timeout from 5s to 30s for direct PostgreSQL queries, 15s to 60s for Flask app queries
  - Improved output parsing to handle whitespace and newlines correctly
  - Location: `mybookshelf2/monitor_migration.py` lines 123-184, 186-220

### Technical Details
- Direct PostgreSQL queries now use 30-second timeout (was 5 seconds)
- Flask app fallback queries now use 60-second timeout (was 15 seconds)
- Enhanced parsing to extract numbers from output even with extra whitespace
- Better error logging for debugging query failures

## [2026-01-19] - Upload Timeout Detection with Progress Monitoring

### Added
- **Progress monitoring for uploads**: Implemented intelligent timeout detection that monitors upload progress instead of using a fixed timeout
  - Uses `subprocess.Popen` instead of `subprocess.run` to monitor process activity
  - Checks for progress every 60 seconds (configurable)
  - Detects stuck processes if no progress for 4 minutes (configurable)
  - Kills stuck uploads early instead of waiting full 10 minutes
  - Location: `mybookshelf2/bulk_migrate_calibre.py` lines 1057-1180

- **Multiple progress indicators**: Monitors multiple signals to detect if upload is making progress
  - **Output monitoring**: Detects new stdout/stderr output from upload process
  - **CPU activity**: Uses psutil to monitor CPU time (if available)
  - **I/O activity**: Monitors file I/O operations (if psutil available)
  - **Process status**: Checks if process is still running
  - Falls back gracefully if psutil is not available

- **Configurable thresholds**: Progress monitoring can be customized per upload
  - `max_timeout`: Maximum total timeout (default: 600s = 10 minutes)
  - `progress_check_interval`: How often to check for progress (default: 60s)
  - `stuck_threshold`: Consider stuck if no progress for this duration (default: 240s = 4 minutes)

### Changed
- **Upload process**: Both `bulk_migrate_calibre.py` and `upload_tar_files.py` now use progress monitoring
  - `MyBookshelf2Migrator.upload_file()` now calls `_run_upload_with_progress_monitoring()`
  - `TarFileUploader.upload_file_from_tar()` now uses migrator's progress monitoring
  - Maintains backward compatibility with existing retry logic

### Benefits
- **Faster failure detection**: Stuck uploads are detected and killed in 4 minutes instead of 10 minutes
- **Better resource utilization**: Workers don't wait unnecessarily for stuck processes
- **Improved reliability**: Distinguishes between slow but progressing uploads vs. truly stuck processes
- **Reduced timeout errors**: Legitimate slow uploads can continue past 4 minutes if showing progress

### Technical Details
- Progress monitoring runs in a loop checking every 0.1 seconds for output
- Every 60 seconds, performs comprehensive progress check
- If no progress detected for 240 seconds, terminates process gracefully
- Falls back to standard timeout if process exceeds maximum timeout (600s)
- Works on both Unix and Windows (with platform-specific optimizations)

## [2026-01-07] - Safe Cleanup Deletion Improvements

### Added
- **Verification before deletion**: Added `verify_file_safe_to_delete()` method that double-checks each file before deletion
  - Verifies hash still exists in MyBookshelf2 database
  - Re-verifies path is not referenced by symlinks
  - Checks file still exists and is readable
  - Returns (safe, reason) tuple for detailed feedback
  - Location: `mybookshelf2/cleanup_orphaned_calibre_files.py` lines 300-318

- **Backup before deletion**: Added `create_backup_list()` method that creates backup list before any deletion
  - Saves file paths, hashes, sizes, and modification times to JSON file
  - Backup file: `calibre_cleanup_backup_worker{id}.json` or `calibre_cleanup_backup.json`
  - Includes metadata for potential file recovery
  - Location: `mybookshelf2/cleanup_orphaned_calibre_files.py` lines 320-355

- **Safety flags and options**: Added new command-line arguments for safer deletion
  - `--skip-verification`: Skip verification before deletion (NOT RECOMMENDED)
  - `--require-symlink-check`: Require successful symlink check before deletion (default: True)
  - `--no-require-symlink-check`: Allow deletion even if symlink check failed (NOT RECOMMENDED)
  - `--confirm-threshold`: Require confirmation prompt for deletions above this count (default: 1000)
  - `--backup-dir`: Directory to save backup file (default: current directory)
  - `--verify-only`: Run verification only, do not delete files
  - Location: `mybookshelf2/cleanup_orphaned_calibre_files.py` lines 750-780

### Fixed
- **Symlink loading timeout**: Fixed timeout issues when loading symlink paths from large libraries
  - Increased timeout from 60s to 300s (5 minutes)
  - Added retry logic with exponential backoff (3 retries: 1s, 2s, 4s delays)
  - Added progress logging during symlink loading
  - Better error handling and reporting
  - Location: `mybookshelf2/cleanup_orphaned_calibre_files.py` lines 221-282

- **Deletion safety**: Improved deletion process with verification and safeguards
  - Files verified before deletion (unless `--skip-verification` is used)
  - Deletion aborted if symlink check failed and `--require-symlink-check` is True
  - Batch deletion with progress tracking
  - Detailed deletion statistics (deleted, skipped, failed, verified)
  - Location: `mybookshelf2/cleanup_orphaned_calibre_files.py` lines 675-730

### Changed
- **Enhanced deletion process**: `delete_files()` method now returns detailed statistics
  - Returns dict with: deleted, failed, skipped, verified counts
  - Accepts list of dicts (with path and hash) instead of just paths
  - Adds verification step before each deletion
  - Processes deletions in batches for progress tracking
  - Location: `mybookshelf2/cleanup_orphaned_calibre_files.py` lines 675-710

- **Improved reports**: Reports now include warnings and deletion statistics
  - Warning section if symlink check failed/timed out
  - Deletion statistics section (if deletion was performed)
  - Backup file location in reports
  - Symlink check status in JSON report
  - Location: `mybookshelf2/cleanup_orphaned_calibre_files.py` lines 639-661

### Technical Details
- **Symlink check tracking**: Added `symlink_check_succeeded` flag to track if symlink check completed successfully
- **Backup format**: Backup files include timestamp, worker_id, file paths, hashes, sizes, and modification times
- **Verification logic**: Verification checks hash existence, symlink reference, and file existence before deletion
- **Confirmation prompt**: Requires typing exact count (e.g., "DELETE 1000") for large deletions

### Safety Features
1. **Verification**: Double-check each file before deletion
2. **Backup**: Create backup list before any deletion
3. **Timeout handling**: Skip deletion if symlink check failed (unless disabled)
4. **Batch processing**: Delete in batches with progress tracking
5. **Confirmation**: Require confirmation for large deletions (>1000 files by default)
6. **Audit trail**: Log all deletions with timestamps and statistics

### Files Modified
- `mybookshelf2/cleanup_orphaned_calibre_files.py`: 
  - Fixed `load_symlink_paths()` timeout and added retry logic (lines 221-282)
  - Added `verify_file_safe_to_delete()` method (lines 300-318)
  - Added `create_backup_list()` method (lines 320-355)
  - Enhanced `delete_files()` method with verification (lines 675-710)
  - Updated `run()` method with backup and safety checks (lines 730-780)
  - Added new command-line arguments (lines 750-780)
  - Enhanced `generate_reports()` with warnings and deletion stats (lines 639-661)

## [2025-12-22] - Calibre Library Cleanup Script

### Added
- **Calibre Library Cleanup Script**: New script to identify and optionally remove orphaned files in Calibre library
  - **Location**: `mybookshelf2/cleanup_orphaned_calibre_files.py`
  - **Purpose**: Identifies files that are not tracked by Calibre or not referenced by MyBookshelf2
  - **Features**:
    - Scans all files in Calibre library directory
    - Checks files against Calibre's `metadata.db` to see if they're tracked
    - For tracked files, calculates SHA1 hash and checks MyBookshelf2 database
    - Checks if file paths are referenced via symlinks in MyBookshelf2
    - Generates detailed JSON and text reports categorizing files
    - Supports dry-run mode (default) and delete mode
    - Progress tracking with resumability
    - Batch processing to handle large libraries efficiently
    - Worker-compatible with worker-specific progress files

- **File Categorization**: Script categorizes files into three groups:
  1. **Files not in Calibre DB**: Files in library directory but not tracked by Calibre
  2. **Files with no hash match**: Files tracked by Calibre but not in MyBookshelf2 (orphaned)
  3. **Files with hash match but no path reference**: Duplicate files where hash exists in MyBookshelf2 but this specific path isn't referenced

- **Reporting**: Comprehensive reporting system
  - JSON report (`calibre_cleanup_report.json`): Machine-readable report with all statistics and file lists
  - Text report (`calibre_cleanup_report.txt`): Human-readable report with statistics and sample file lists
  - Progress file (`calibre_cleanup_progress.json`): Tracks processed files for resumability

### Technical Details
- **Database Queries**:
  - Calibre: Queries `metadata.db` using SQL to get all tracked book files
  - MyBookshelf2: Queries `Source.hash` table to get all existing file hashes
  - Symlinks: Uses `find` command inside container to get all symlink target paths
- **Hash Algorithm**: Uses SHA1 hash matching MyBookshelf2's algorithm
- **Path Matching**: Normalizes paths for matching (handles host paths, container paths, spaces in directory names)
- **Safety Features**:
  - Default to dry-run mode (no deletion)
  - Requires explicit `--delete` flag to remove files
  - Progress tracking prevents reprocessing files
  - Batch processing with progress saves after each batch

### Usage
```bash
# Dry-run (report only, no deletion)
python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library --container mybookshelf2_app

# Actually delete orphaned files
python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library --container mybookshelf2_app --delete

# With worker ID and custom batch size
python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library --worker-id 1 --batch-size 500
```

### Files Created
- `mybookshelf2/cleanup_orphaned_calibre_files.py`: Main cleanup script (700+ lines)

## [2025-12-22] - Worker Duplicate Handling and Skip-Ahead Logic

### Fixed
- **Return code 11 handling**: Fixed workers failing on files that already exist
  - Problem: Workers were getting return code 11 ("Data error - no use in retrying") when files already existed, causing them to fail instead of skipping
  - Solution: Added proper handling for return code 11 to treat it as success (file already exists)
  - Impact: Workers 1 and 2 now correctly skip duplicate files instead of failing
  - Location: `mybookshelf2/bulk_migrate_calibre.py` lines 1242-1259

- **Skip-ahead logic for duplicate files**: Workers now automatically skip ahead when encountering too many duplicates
  - Problem: Workers were processing files that already existed, wasting time on fully-migrated ranges
  - Solution: Added logic to track actual uploads vs duplicates, skip ahead after 5 consecutive batches with all duplicates
  - Impact: Workers now automatically find new files to upload instead of getting stuck on duplicates
  - Location: `mybookshelf2/bulk_migrate_calibre.py` lines 2335-2366

### Changed
- Modified `upload_file()` to return tuple `(success, was_duplicate)` instead of just boolean
  - Returns `(True, False)` for actual new uploads
  - Returns `(True, True)` for files that already exist (duplicates)
  - Enables proper tracking of actual uploads vs duplicates

- Updated skip-ahead logic to trigger when `actual_upload_count == 0` for 5 consecutive batches
  - Previously only triggered when `success_count == 0`, but duplicates were being counted as successes
  - Now tracks `actual_upload_count` separately from `success_count`
  - Workers skip ahead by 10,000 book IDs when in fully-migrated ranges

### Technical Details
- Added `actual_upload_count` tracking in batch processing loop
- Modified error handling to check return code 11 before generic error handling
- Skip-ahead logic now considers both filtered duplicates and upload-time duplicates
- Workers automatically resume from new position after skip-ahead

### Files Modified
- `mybookshelf2/bulk_migrate_calibre.py`: 
  - Added return code 11 handling (lines 1242-1259)
  - Modified `upload_file()` return value to include duplicate status (lines 1259, 1361)
  - Added `actual_upload_count` tracking (lines 2118, 2303-2322)
  - Enhanced skip-ahead logic (lines 2335-2366)

### Migration Status
- ✅ Return code 11 handling: Working correctly
- ✅ Duplicate tracking: Accurately distinguishes new uploads from duplicates
- ✅ Skip-ahead logic: Automatically skips ahead after 5 consecutive duplicate batches
- ✅ All workers: Restarted and running with updated code

## [2025-11-30] - Enhanced LLM Code Fix Suggestions

### Added
- **Code Snippet Extraction**: LLM now receives relevant code snippets when analyzing issues
  - Automatically extracts functions from `bulk_migrate_calibre.py` based on error patterns
  - Maps patterns to functions: "book.id"/"infinite loop" → `find_ebook_files_from_database()`, "API error" → `upload_file()`, "NUL character" → `sanitize_metadata_string()`, etc.
  - Includes function code with line numbers in analysis prompt
  - Enables LLM to suggest precise code fixes with actual code context
  - Configurable via `ENABLE_CODE_SNIPPETS` and `MAX_CODE_SNIPPET_LINES` (default: 500 lines)

- **Recurring Root Cause Detection**: Automatically detects when same root cause appears multiple times
  - Uses fuzzy keyword matching to identify similar root causes across workers
  - Tracks occurrence count in `auto_fix_history.json`
  - Suggests `code_fix` instead of `restart` for recurring issues (threshold: 2+ occurrences, configurable)
  - Helps fix root causes permanently instead of repeatedly restarting workers
  - Configurable via `RECURRING_ROOT_CAUSE_THRESHOLD` (default: 2)

- **Enhanced LLM Prompt**: Improved prompt with explicit guidance and code context
  - Includes relevant code snippets when available
  - Explicit decision criteria for when to use `code_fix` vs `restart`
  - Recurring issue warnings to encourage permanent fixes
  - Confidence thresholds for code fix recommendations (>= 0.7, configurable)
  - Better guidance on fix type selection based on root cause clarity

- **Log Analysis Guide**: Comprehensive documentation for analyzing auto-monitor logs
  - `LOG_ANALYSIS_GUIDE.md`: Guide for understanding log structure and generating reports
  - Includes log file locations, message patterns, key metrics
  - Example queries and Python scripts for analysis
  - Report generation templates (daily, weekly, root cause analysis)

### Changed
- **LLM Prompt Structure**: Enhanced to include code snippets and recurring root cause information
  - Code snippets added when error patterns match known functions
  - Recurring root cause info included in prompt to encourage code fixes
  - More explicit guidance about when to use each fix type

- **Fix History Tracking**: Now tracks recurring root cause information
  - `recurring_root_cause`: Boolean indicating if root cause appeared before
  - `root_cause_occurrence_count`: Number of times this root cause appeared
  - Helps identify patterns and systemic issues

### Configuration
- `CODE_FIX_MIN_CONFIDENCE = 0.7`: Minimum confidence to suggest code_fix
- `RECURRING_ROOT_CAUSE_THRESHOLD = 2`: Number of occurrences before suggesting code_fix
- `ENABLE_CODE_SNIPPETS = True`: Whether to include code snippets in prompts
- `MAX_CODE_SNIPPET_LINES = 500`: Maximum lines per code snippet

### Technical Details

#### Code Snippet Extraction
- Function `get_relevant_code_snippets()` in `llm_debugger.py`
- Uses AST parsing with regex fallback to extract function code
- Maps error patterns to relevant functions automatically
- Includes 5-10 lines of context before/after functions
- Limits snippet size to avoid token limits

#### Recurring Root Cause Detection
- Function `check_recurring_root_cause()` in `monitor.py`
- Normalizes root cause text (lowercase, removes punctuation, extracts keywords)
- Matches current root cause against historical ones using keyword intersection
- Considers root causes similar if they share 3+ keywords
- Integrated into diagnostics collection before LLM analysis

#### Integration Flow
1. Worker detected as stuck
2. Diagnostics collected (logs, error patterns, book.id range, etc.)
3. Recurring root cause checked against fix history
4. If recurring, info added to diagnostics
5. LLM prompt built with code snippets (if available) and recurring info
6. LLM analyzes with full context
7. LLM more likely to suggest `code_fix` for recurring issues

### Benefits
- **Better Code Fixes**: LLM can now see actual code, enabling more accurate fix suggestions
- **Permanent Fixes**: Recurring issues automatically trigger code fix suggestions
- **Reduced Restart Loops**: System identifies when restart won't help and suggests code fixes instead
- **Better Analysis**: Comprehensive log analysis guide enables better insights and reporting

### Files Modified
- `mybookshelf2/auto_monitor/llm_debugger.py`: Added `get_relevant_code_snippets()` function, enhanced `build_analysis_prompt()`
- `mybookshelf2/auto_monitor/monitor.py`: Added `check_recurring_root_cause()` function, integrated into diagnostics
- `mybookshelf2/auto_monitor/config.py`: Added code fix configuration options

### Files Created
- `mybookshelf2/auto_monitor/LOG_ANALYSIS_GUIDE.md`: Comprehensive log analysis documentation

## [2025-11-30] - Disk I/O Based Worker Scaling

### Added
- **Automatic Worker Scaling**: Auto-monitor now automatically adjusts worker count based on disk I/O utilization
  - **Scale-Down**: Reduces workers when disk I/O >= 90% (saturated) and workers are stuck
  - **Scale-Up**: Increases workers when disk I/O < 50% (normal) and below target count
  - **LLM Integration**: Uses LLM to analyze if disk I/O is the root cause before scaling down
  - **Fallback Logic**: Scales down even if LLM returns "Unknown" when disk I/O is clearly saturated (>= 90%)

- **Disk I/O Monitoring**:
  - Monitors Calibre library disk utilization using `iostat`
  - Checks every 60 seconds during monitor loop
  - Provides accurate disk utilization percentage (%util)

- **Worker Count Management**:
  - Maintains target worker count (default: 4)
  - Prevents exceeding maximum (8) or going below minimum (1)
  - Automatically kills excess workers if count exceeds desired
  - Automatically restarts stopped workers up to target count

- **Configuration Parameters**:
  - `TARGET_WORKER_COUNT = 4` - Desired number of workers
  - `MIN_WORKER_COUNT = 1` - Minimum workers (never scale below)
  - `MAX_WORKER_COUNT = 8` - Maximum workers (never scale above)
  - `DISK_IO_SATURATED_THRESHOLD = 90` - Disk utilization % for scale-down
  - `DISK_IO_NORMAL_THRESHOLD = 50` - Disk utilization % for scale-up
  - `DISK_IO_SCALE_DOWN_COOLDOWN = 300` - 5 minutes cooldown between scale-downs
  - `DISK_IO_SCALE_UP_COOLDOWN = 600` - 10 minutes cooldown between scale-ups

### Technical Details

#### Scale-Down Logic
1. Checks disk I/O utilization every 60 seconds
2. If disk I/O >= 90% (saturated):
   - Checks if any workers are stuck (no uploads for 5+ minutes)
   - If workers are stuck:
     - If LLM enabled: Analyzes stuck workers to confirm disk I/O is root cause
     - **Fallback**: If LLM returns "Unknown" but disk I/O >= 90% and workers stuck, still scales down
     - Kills highest ID worker (reduces by 1)
     - Updates desired worker count
     - Enforces 5-minute cooldown before next scale-down

#### Scale-Up Logic
1. Checks disk I/O utilization every 60 seconds
2. If disk I/O < 50% (normal):
   - Checks if current workers < target count
   - If below target:
     - Starts new worker (increases by 1)
     - Updates desired worker count
     - Enforces 10-minute cooldown before next scale-up
3. Gradually scales up to target count (4 workers)

#### LLM Integration
- When disk I/O is saturated and workers are stuck, LLM analyzes logs to determine if disk I/O is the root cause
- LLM receives disk I/O context (utilization %, saturated status) in diagnostics
- If LLM confirms disk I/O is root cause, scales down immediately
- **Fallback**: If LLM returns "Unknown" but disk I/O >= 90% and workers stuck, scales down anyway (prevents false negatives)

### Benefits
- **Prevents Disk Saturation**: Automatically reduces workers when disk I/O is saturated
- **Optimizes Performance**: Scales up when disk I/O is normal to maximize throughput
- **Self-Healing**: Automatically adjusts to maintain optimal worker count
- **LLM-Powered**: Uses LLM to intelligently determine if disk I/O is the issue
- **Safe Fallback**: Scales down even if LLM cannot determine root cause when disk I/O is clearly saturated

### Files Modified
- `mybookshelf2/auto_monitor/monitor.py`: Added `scale_workers_based_on_disk_io()`, `get_disk_io_utilization()`, `kill_worker()` functions
- `mybookshelf2/auto_monitor/config.py`: Added worker scaling configuration parameters
- `mybookshelf2/auto_monitor/llm_debugger.py`: Enhanced LLM prompt to include disk I/O context

## [2025-11-29] - Auto-Monitor with LLM-Powered Debugging

### Added
- **Auto-Monitor System**: Comprehensive worker monitoring and auto-fix system
  - **Location**: `mybookshelf2/auto_monitor/` (standalone module)
  - **Features**:
    - Automatic detection of stuck workers (no uploads for 5+ minutes)
    - LLM-powered debugging using OpenAI API
    - Automatic fix application with strict safety checks
    - Independent module that can be easily enabled/disabled

- **LLM Fix Types**: Three types of fixes the LLM can apply:
  1. **RESTART**: Restarts worker process (default fallback)
  2. **CODE_FIX**: Automatically modifies `bulk_migrate_calibre.py` to fix bugs
  3. **CONFIG_FIX**: Changes worker parameters (parallel_uploads, batch_size)

- **Automatic Code Fix Application**: 
  - Parses LLM-provided code changes (function replacement, context-based, or diff format)
  - Creates timestamped backups before any changes
  - Validates Python syntax (AST + py_compile) before applying
  - Automatically rolls back if validation fails
  - Maximum 3 fix attempts per worker before escalation

- **Status-Aware Thresholds**:
  - **5 minutes** for workers that have uploaded before (normal operation)
  - **20 minutes** for workers in discovery/initialization phase (allows time for database queries)
  - Prevents restart loop during legitimate discovery phase

- **Enhanced Progress Detection**:
  - Recognizes "Processed batch" messages as progress
  - Detects "Found X new files so far" during batch processing
  - Recognizes database query activity
  - Prevents false positives during discovery phase

- **Comprehensive LLM Fix Logging**:
  - Logs root cause identified by LLM
  - Logs fix type, confidence score, and fix description
  - Logs code changes (preview and full changes)
  - Logs config changes
  - All details saved to `auto_fix_history.json`

- **Environment Variable Support**:
  - `.env` file support for OpenAI API key
  - Automatic loading from `auto_monitor/.env`
  - Secure storage (not committed to git)

### Fixed
- **Restart Loop During Discovery**: Fixed issue where workers were being restarted too aggressively during discovery phase
  - **Problem**: Workers in discovery phase (5-10 minutes) were being restarted after 5 minutes with no uploads
  - **Solution**: 
    - Increased discovery threshold to 20 minutes
    - Improved progress detection to recognize discovery activity
    - Status-aware thresholds (different for discovery vs normal operation)
  - **Impact**: Eliminated restart loop, workers now get proper time to discover files

### Technical Details

#### Auto-Monitor Architecture
- **Main Script**: `monitor.py` - Main monitoring loop
- **LLM Integration**: `llm_debugger.py` - OpenAI API integration for analysis
- **Fix Application**: `fix_applier.py` - Applies restarts, code fixes, and config fixes
- **Configuration**: `config.py` - All settings (thresholds, limits, safety features)

#### LLM Code Fix Process
1. LLM analyzes worker logs (last 500 lines)
2. LLM identifies root cause and suggests fix type
3. For code fixes: LLM provides code changes in structured format
4. System creates backup of `bulk_migrate_calibre.py`
5. System parses and applies code changes
6. System validates Python syntax (AST + py_compile)
7. If valid: Commits changes and restarts worker
8. If invalid: Rolls back backup and logs error

#### Safety Features
- **Maximum 3 attempts**: After 3 failed fix attempts, worker is paused/stopped
- **10-minute cooldown**: Prevents fix spam for same worker
- **Mandatory backups**: All code changes backed up with timestamp
- **Syntax validation**: Python syntax checked before applying
- **Automatic rollback**: Restores backup if validation fails
- **Success verification**: Waits 2 minutes and verifies worker recovered

#### LLM Analysis Capabilities
The LLM can detect:
- Infinite loops (same book.id range repeated)
- API errors (500, connection failures)
- Database query issues
- Memory or performance problems
- Error patterns in logs
- Stuck conditions

### Files Added
- `mybookshelf2/auto_monitor/monitor.py`: Main monitoring script
- `mybookshelf2/auto_monitor/llm_debugger.py`: LLM integration
- `mybookshelf2/auto_monitor/fix_applier.py`: Fix application logic
- `mybookshelf2/auto_monitor/config.py`: Configuration settings
- `mybookshelf2/auto_monitor/start.sh`: Start script
- `mybookshelf2/auto_monitor/stop.sh`: Stop script
- `mybookshelf2/auto_monitor/README.md`: Comprehensive documentation
- `mybookshelf2/auto_monitor/.env`: OpenAI API key (gitignored)

### Files Modified
- `mybookshelf2/auto_monitor/monitor.py`: Enhanced with status-aware thresholds and better progress detection
- `mybookshelf2/auto_monitor/config.py`: Added `DISCOVERY_THRESHOLD_SECONDS` (20 minutes)
- `mybookshelf2/auto_monitor/fix_applier.py`: Implemented automatic code fix application with safety checks
- `mybookshelf2/auto_monitor/start.sh`: Added `.env` file loading

### Usage
```bash
# Start auto-monitor (basic mode - restart only)
cd mybookshelf2/auto_monitor
./start.sh

# Start with LLM debugging (requires OpenAI API key in .env)
./start.sh --llm-enabled

# View logs
tail -f auto_restart.log
tail -f monitor.log

# View fix history
cat auto_fix_history.json | jq
```

### Migration Status
- ✅ Auto-monitor: Running and monitoring workers
- ✅ LLM integration: Configured with API key
- ✅ Fix types: All 3 types (restart, code_fix, config_fix) implemented
- ✅ Safety features: All safety checks in place
- ✅ Logging: Comprehensive logging of all LLM fixes

## [2025-11-25] - Docker Compose Fixes and Migration Error Resolution

### Fixed
- **Docker Compose V2 Installation**: Installed Docker Compose V2 plugin (v2.40.3) to replace broken standalone docker-compose that failed on Python 3.12
  - Created installation script: `install_docker_compose_simple.sh`
  - Fixed binary download URL (changed from `amd64` to `x86_64`)
  - Docker Compose now works with `docker compose` command (space, not hyphen)

- **Docker Compose Configuration**: Fixed validation errors in `docker-compose.yml`
  - Removed empty `volumes:` section (caused "volumes must be a mapping" error)
  - Removed obsolete `version: '3.8'` field (warning in Docker Compose V2)
  - Changed database port from 5432 to 5433 to avoid conflict with host PostgreSQL

- **Missing Calibre Tools**: Installed Calibre tools in backend container
  - Added Calibre 5.44.0 installation to `mybookshelf2/deploy/Dockerfile`
  - Installed imagemagick, libreoffice, and libgl1-mesa-glx dependencies
  - Verified `ebook-meta` and `ebook-convert` are now available in containers
  - Fixes metadata extraction failures that were blocking uploads

- **File Size Limit**: Increased maximum upload size from 100MB to 500MB
  - Updated `MAX_CONTENT_LENGTH` in `mybookshelf2/settings.py` from 100MB to 500MB
  - Allows processing of 755 previously rejected large files
  - Verified in app container: `MAX_CONTENT_LENGTH: 524288000` (500MB)

- **Progress File Saving**: Fixed atomic file write errors in workers 3 & 4
  - Added directory existence check before writing progress files
  - Added fallback direct write mechanism if atomic rename fails
  - Improved error handling for file permission issues
  - Prevents data loss if migration is interrupted

### Changed
- **Docker Data Root**: Switched Docker data-root to default `/var/lib/docker`
  - Updated `/etc/docker/daemon.json` to use default location
  - Previous custom location had 200GB of orphaned data

- **Dockerfile Structure**: Updated to use generic Dockerfile for both backend and app
  - Added Calibre installation to generic Dockerfile
  - Added `loop.sh` script to Dockerfile
  - Fixed requirements file paths (changed from root to `deploy/` directory)

### Added
- **Installation Scripts**: Created helper scripts for Docker Compose installation
  - `install_docker_compose_simple.sh`: Direct binary download method
  - `verify_docker_compose.sh`: Verification script

- **Error Analysis**: Created migration error analysis and fix plan
  - `fix_migration_errors_plan.md`: Comprehensive error analysis and resolution plan

### Technical Details

#### Docker Compose Fix
- **Issue**: `docker-compose` standalone tool (v1.29.2) failed with `ModuleNotFoundError: No module named 'distutils'` on Python 3.12
- **Solution**: Installed Docker Compose V2 plugin via direct binary download from GitHub
- **Location**: `/usr/libexec/docker/cli-plugins/docker-compose`

#### Calibre Tools Installation
- **Dockerfile Changes**: Added to `mybookshelf2/deploy/Dockerfile`:
  ```dockerfile
  RUN apt-get update && \
      apt-get install -y imagemagick libreoffice libgl1-mesa-glx && \
      wget -nv -O- https://download.calibre-ebook.com/linux-installer.sh | sh /dev/stdin version=5.44.0
  ```

#### File Size Limit
- **Before**: `MAX_CONTENT_LENGTH = 100 * 1024 * 1024` (100MB)
- **After**: `MAX_CONTENT_LENGTH = 500 * 1024 * 1024` (500MB)
- **Impact**: 755 files that were previously rejected can now be processed

#### Progress File Saving
- **Issue**: Workers 3 & 4 couldn't save progress files due to missing directory or file permission issues
- **Fix**: Added directory creation check and fallback write mechanism in `bulk_migrate_calibre.py`

### Migration Status
- ✅ Docker containers: Running (db, backend, app)
- ✅ 4 migration workers: Active and processing
- ✅ Calibre tools: Installed and verified
- ✅ File size limit: Increased to 500MB
- ✅ Progress saving: Fixed and working

### Files Modified
- `mybookshelf2/deploy/Dockerfile`: Added Calibre installation
- `mybookshelf2/docker-compose.yml`: Fixed validation errors, changed database port
- `mybookshelf2/settings.py`: Increased MAX_CONTENT_LENGTH to 500MB
- `mybookshelf2/bulk_migrate_calibre.py`: Fixed progress file saving with directory check and fallback

### Verification
- Calibre tools: `ebook-meta (calibre 5.44)` verified in backend container
- File size limit: `524288000` bytes (500MB) confirmed in app container
- Workers: 8 active processes (4 workers + parent processes)
- No new errors: Verified no "file too big" or "ebook-meta not found" errors in recent logs

## [2025-11-26] - Critical Duplicate Prevention and Performance Improvements

### Fixed
- **CRITICAL: Stale Duplicate Checking** - Fixed root cause of duplicate upload attempts
  - **Problem**: `existing_hashes` was loaded once at startup and never refreshed, causing workers to attempt uploading files already uploaded by other workers
  - **Solution**: 
    - Added periodic hash refresh (every 1000 files or 10 minutes) to pick up files uploaded by other workers
    - Added local cache updates after each successful upload to keep cache current
    - Prevents thousands of wasted duplicate upload attempts
  - **Impact**: Eliminates duplicate upload attempts when multiple workers run in parallel
  - **Verified**: Test confirmed hash refresh working correctly (detected 800+ new hashes from other workers)

- **Database Errors from NUL Characters**: Fixed PostgreSQL errors caused by NUL (0x00) characters in filenames
  - Added `sanitize_filename()` and `sanitize_metadata_string()` methods to remove NUL characters
  - Applied sanitization to all metadata (title, authors, series, language) before upload
  - Applied sanitization to file paths stored in progress files
  - **Impact**: Eliminates "A string literal cannot contain NUL (0x00) characters" PostgreSQL errors

- **API Connection Failures**: Added retry logic with exponential backoff for connection errors
  - Retries up to 3 times with delays of 2s, 4s, 8s on connection errors
  - Handles timeout errors and network issues automatically
  - Added API connectivity health check at migration start
  - **Impact**: Fixes Worker 1 connection failures, reduces transient error rates

### Added
- **Performance Monitoring**: Added detailed upload performance tracking
  - Tracks upload time per file
  - Logs average upload rate (files/min) every 100 files
  - Detects and logs slow uploads (>2 minutes) for investigation
  - **Impact**: Helps identify bottlenecks and track performance improvements

- **Hash Refresh Methods**: 
  - `refresh_existing_hashes()`: Reloads hashes from database periodically
  - `update_existing_hashes()`: Updates local cache after each successful upload
  - Automatic refresh triggers: every 1000 files processed or every 10 minutes

- **Connection Health Checks**:
  - `check_api_connectivity()`: Verifies API endpoint is reachable before migration
  - Logs connectivity status for debugging

### Technical Details

#### Stale Duplicate Checking Fix
- **Root Cause**: Each worker loaded `existing_hashes` once in `__init__()` and never updated it, even as other workers added new files
- **Solution**: 
  - Periodic refresh: `refresh_existing_hashes()` called every 1000 files or 10 minutes
  - Local updates: `update_existing_hashes()` called after each successful upload
  - Tracks `files_processed_since_refresh` and `last_hash_refresh` time
- **Code Location**: `mybookshelf2/bulk_migrate_calibre.py`
  - Lines 281-292: `refresh_existing_hashes()` method
  - Lines 294-299: `update_existing_hashes()` method
  - Lines 1429: Periodic refresh in `migrate()` loop
  - Lines 797, 834: Local cache updates after successful uploads

#### Filename Sanitization
- **Methods**: 
  - `sanitize_filename()`: Removes NUL characters from file paths
  - `sanitize_metadata_string()`: Removes NUL characters from metadata strings
- **Applied to**: All metadata passed to CLI, all file paths in progress files
- **Code Location**: `mybookshelf2/bulk_migrate_calibre.py` lines 301-316

#### Retry Logic
- **Implementation**: Exponential backoff retry wrapper in `upload_file()` method
- **Retry conditions**: Connection errors, timeouts, network issues
- **Retry delays**: 2s, 4s, 8s (exponential backoff)
- **Max retries**: 3 attempts
- **Code Location**: `mybookshelf2/bulk_migrate_calibre.py` lines 720-750

#### Performance Monitoring
- **Metrics tracked**: Upload time per file, average upload rate, slow upload detection
- **Reporting**: Every 100 successful uploads
- **Code Location**: `mybookshelf2/bulk_migrate_calibre.py` lines 79-80, 797-810

### Files Modified
- `mybookshelf2/bulk_migrate_calibre.py`: 
  - Added hash refresh methods and periodic refresh logic
  - Added filename/metadata sanitization
  - Added retry logic with exponential backoff
  - Added performance monitoring
  - Added API connectivity checks

### Testing
- **Unit Tests**: Created `test_migration_changes.py` - all tests passed ✓
- **Integration Test**: Tested with 130 files - all features verified working
  - API connectivity check: ✓ Passed
  - Hash refresh: ✓ Working (refreshed 6+ times, detected 800+ new hashes)
  - Duplicate detection: ✓ Working (130/130 files correctly detected)
  - Sanitization: ✓ Working (no NUL character errors)
- **Test Results**: See `mybookshelf2/TEST_RESULTS.md` for detailed test results

### Expected Improvements
- **Duplicate Upload Attempts**: Should decrease significantly (from thousands to near zero)
- **Connection Errors**: Automatically retried, reducing failure rates
- **Database Errors**: Eliminated NUL character errors
- **Performance Visibility**: Upload rates and slow uploads now tracked and reported

### Migration Status
- ✅ Hash refresh mechanism: Working and verified
- ✅ Duplicate detection: Working correctly
- ✅ API connectivity: Health checks working
- ✅ Sanitization: No NUL character errors
- ✅ Retry logic: Ready for connection error handling
- ✅ Performance monitoring: Tracking upload speeds
