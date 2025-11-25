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

