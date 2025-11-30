# Changelog

All notable changes to the Calibre Automation Scripts and MyBookshelf2 migration system.

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



