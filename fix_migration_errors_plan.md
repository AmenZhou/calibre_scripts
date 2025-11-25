# Plan to Fix Migration Worker Errors

## Errors Found

### 1. Missing Calibre Tools (Critical)
**Error**: `FileNotFoundError: [Errno 2] No such file or directory: '/usr/bin/ebook-meta'`
**Impact**: Backend container cannot extract metadata from ebooks, causing upload failures
**Location**: Backend container missing Calibre installation
**Count**: Affecting all workers when processing files that need metadata extraction

### 2. File Size Limit (High Priority)
**Error**: `API error: file too big`
**Impact**: 755 files are being rejected because they exceed 100MB limit
**Current Limit**: 100MB (MAX_CONTENT_LENGTH = 100 * 1024 * 1024)
**Location**: `mybookshelf2/settings.py` line 48

### 3. Progress File Save Errors (Medium Priority)
**Error**: `Error saving progress file: [Errno 2] No such file or directory: 'migration_progress_worker3.tmp'`
**Impact**: Workers 3 & 4 cannot save progress, risking data loss if interrupted
**Location**: Progress file atomic write operation failing
**Count**: Frequent errors in workers 3 & 4

## Current Status
- ✅ Docker containers: Running (db, backend, app)
- ✅ 4 workers: Running and processing
- ❌ Backend missing Calibre tools
- ❌ 755 files rejected due to size limit
- ❌ Progress saving failing for workers 3 & 4

## Implementation Plan

### Step 1: Install Calibre Tools in Backend Container
**File**: `mybookshelf2/deploy/Dockerfile`
**Action**: Add Calibre installation to the generic Dockerfile
**Changes**:
```dockerfile
# After Python packages installation, add:
RUN apt-get update && \
    apt-get install -y imagemagick libreoffice libgl1-mesa-glx && \
    wget -nv -O- https://download.calibre-ebook.com/linux-installer.sh | sh /dev/stdin version=5.44.0
```

**Alternative**: Use Dockerfile-backend, but need to build mbs2-ubuntu base image first

### Step 2: Increase File Size Limit
**File**: `mybookshelf2/settings.py`
**Action**: Increase MAX_CONTENT_LENGTH from 100MB to 500MB or 1GB
**Change**: 
```python
MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500MB instead of 100MB
```

**Consideration**: Check disk space and ensure backend/app can handle larger files

### Step 3: Fix Progress File Saving
**File**: `mybookshelf2/bulk_migrate_calibre.py`
**Action**: Fix atomic file write operation
**Possible Issues**:
- File permissions on .tmp files
- Directory doesn't exist
- Race condition between workers

**Fix**: Ensure directory exists, check permissions, add error handling

### Step 4: Rebuild and Restart
1. Rebuild backend container with Calibre tools
2. Restart containers
3. Restart migration workers
4. Monitor for errors

## Priority Order
1. **Fix Calibre tools** (blocks metadata extraction)
2. **Increase file size limit** (755 files blocked)
3. **Fix progress saving** (data loss risk)

## Expected Outcomes
- All workers can extract metadata successfully
- Files up to 500MB can be uploaded
- Progress files save correctly
- Migration continues without errors

