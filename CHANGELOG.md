# Changelog

All notable changes to the Calibre Automation Scripts and MyBookshelf2 migration system.

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



