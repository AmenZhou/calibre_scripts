# MyBookshelf2 Setup

This folder contains all MyBookshelf2 installation files.

## Contents

- Source code repository - All MyBookshelf2 application files
- `build_and_start.sh` - Script to build and start MyBookshelf2
- `change_admin_password.sh` - Script to change admin password
- `build_spa_client.sh` - Script to build the SPA client
- `docker-compose.yml` - Docker Compose configuration
- `INSTALLATION_COMPLETE.md` - Installation documentation

## Quick Start

### Start MyBookshelf2
```bash
cd mybookshelf2
./build_and_start.sh
```

### Access MyBookshelf2
- URL: http://localhost:5000
- Username: admin
- Password: mypassword123

### Useful Commands

**Check Status:**
```bash
sudo docker ps
```

**View Logs:**
```bash
sudo docker logs -f mybookshelf2_app
```

**Stop Services:**
```bash
sudo docker stop mybookshelf2_app mybookshelf2_backend mybookshelf2_db
```

**Restart Services:**
```bash
cd mybookshelf2
./build_and_start.sh
```

**Change Admin Password:**
```bash
cd mybookshelf2
./change_admin_password.sh
```

**Build SPA Client:**
```bash
cd mybookshelf2
./build_spa_client.sh
```

**Run Database Migrations:**
```bash
docker exec mybookshelf2_app python3 manage.py migrate_tables
```

## Performance Optimizations

### Web Interface Performance (2025-12-24)

Recent optimizations significantly improve page loading speed, especially for large libraries:

- **Database Index on `ebook.created`**: Added index `ix_ebook_created` to optimize queries ordering by creation date
  - Reduces query time from seconds to milliseconds for 866,850+ ebooks
  - Automatically applied via database migration v3
  - Impact: Main page loads much faster when displaying recent ebooks

- **Flask Threading Enabled**: Server now handles multiple concurrent requests
  - Changed from single-threaded to multi-threaded mode
  - Allows concurrent page loads and API requests
  - Impact: Better responsiveness under load

**To apply performance improvements:**
```bash
# Run database migration (adds performance index)
docker exec mybookshelf2_app python3 manage.py migrate_tables

# Restart app to enable threading
docker restart mybookshelf2_app
```

## Database Migrations

MyBookshelf2 uses a versioned migration system to track database schema changes.

**Check Current Database Version:**
```bash
docker exec mybookshelf2_db psql -U ebooks -d ebooks -c "SELECT version FROM version LIMIT 1;"
```

**Run Pending Migrations:**
```bash
docker exec mybookshelf2_app python3 manage.py migrate_tables
```

**Migration Files:**
- Located in `sql/migration/` directory
- Named `v{N}.sql` where N is the version number
- Current version: 3 (as of 2025-12-24)

**Recent Migrations:**
- **v3** (2025-12-24): Added `ix_ebook_created` index for performance optimization
- **v2**: Added `CONVERSION_BATCH_ENTITY` enum value
- **v1**: Initial schema with rating tables

## Bulk Migration from Calibre

### Overview

The `bulk_migrate_calibre.py` script provides high-performance migration from Calibre libraries to MyBookshelf2 with multiple optimizations for speed and efficiency.

### Performance Optimizations

#### Phase 1: Parallel Uploads (Implemented)
- **Parallel Processing**: Each worker processes files concurrently using ThreadPoolExecutor
- **Configuration**: Use `--parallel-uploads N` parameter (default: 1, range: 1-10)
- **Note**: Default reduced to 1 to prevent server overload. Can be increased if server can handle more load.
- **Speedup**: 3-5x faster than sequential processing (when using 3+ parallel uploads)
- **Expected Rate**: 2-9 files/min per worker (up from 0.19-1.85 files/min)

#### Phase 2a: Quick Wins (Implemented)
- **API Check Before Upload**: Checks `/api/upload/check` endpoint for files not in cache to skip duplicates faster
- **Batch File Copying**: Copies 5 EPUB files at once using tar pipe instead of individual `docker cp` commands
- **Speedup**: Additional 30-50% faster on top of Phase 1
- **Expected Rate**: 3-15 files/min per worker
- **Total Throughput**: 12-60 files/min with 4 workers

### Usage

**Single Worker:**
```bash
python3 bulk_migrate_calibre.py /path/to/calibre/library \
  --worker-id 1 \
  --offset 0 \
  --limit 10000 \
  --parallel-uploads 1
```

**Multiple Workers (Recommended):**
```bash
python3 parallel_migrate.py /path/to/calibre/library \
  --workers 4 \
  --use-symlinks \
  --batch-size 10000 \
  --parallel-uploads 1
```

### Key Features

- **Automatic Deduplication**: Skips files already in MyBookshelf2 database
- **Progress Tracking**: Saves progress to JSON files for safe resumption
- **Hash Refresh**: Periodically refreshes duplicate cache to pick up files from other workers
- **Error Handling**: Retry logic with exponential backoff for transient failures
- **Thread-Safe**: Safe for parallel execution across multiple workers
- **Symlink Mode**: Option to use symlinks instead of copying files (faster for large libraries)

### Monitoring

**Real-time Dashboard:**
```bash
python3 monitor_migration.py
```

**View Worker Logs:**
```bash
tail -f migration_worker1.log
tail -f migration_worker2.log
```

**Check Performance Metrics:**
```bash
grep "Upload performance" migration_worker*.log
```

### Performance Metrics

The script logs performance metrics every 100 files:
```
Upload performance: X.XX files/min (avg Y.Ys per file over last 100 files)
```

### Configuration Options

- `--parallel-uploads N`: Number of concurrent uploads per worker (default: 1)
- `--use-symlinks`: Use symlinks instead of copying files (faster, requires mounted Calibre library)
- `--worker-id N`: Worker identifier for parallel processing
- `--offset N`: Database offset for this worker
- `--limit N`: Maximum number of files to process per batch

### Technical Details

**Phase 1 Optimizations:**
- ThreadPoolExecutor for concurrent uploads within each worker
- Thread-safe progress tracking with locks
- Reduced logging overhead (every 20th file at INFO level)

**Phase 2a Optimizations:**
- HTTP API checks using `/api/upload/check` endpoint
- Batch file copying using tar pipe: `tar cf - files... | docker exec -i container tar xf - -C /tmp`
- Graceful fallback to individual operations on errors

### Expected Performance

| Configuration | Files/min per Worker | Total (4 workers) | Time per File |
|--------------|---------------------|-------------------|---------------|
| Baseline (Sequential) | 0.19-1.85 | 0.76-7.4 | 3-5 minutes |
| Phase 1 (Parallel) | 2-9 | 8-36 | 20-30 seconds |
| Phase 1 + 2a (Optimized) | 3-15 | 12-60 | 10-20 seconds |

## Cleanup Orphaned Calibre Files

### Overview

The `cleanup_orphaned_calibre_files.py` script identifies and optionally removes orphaned files in your Calibre library. It checks files against both Calibre's database and MyBookshelf2 to determine which files are no longer needed.

### What It Does

1. **Scans all files** in the Calibre library directory
2. **Checks against Calibre DB**: Verifies if files are tracked in `metadata.db`
3. **Checks against MyBookshelf2**: 
   - Calculates SHA1 hash for each tracked file
   - Checks if hash exists in MyBookshelf2 `Source` table
   - Checks if file path is referenced via symlinks
4. **Categorizes files**:
   - Files not in Calibre DB (orphaned from Calibre)
   - Files with no hash match (orphaned from MyBookshelf2)
   - Files with hash match but no path reference (duplicates)

### Usage

**Dry-run (report only, recommended first):**
```bash
python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library \
  --container mybookshelf2_app
```

**Actually delete orphaned files:**
```bash
python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library \
  --container mybookshelf2_app \
  --delete
```

**With worker ID for progress tracking:**
```bash
python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library \
  --container mybookshelf2_app \
  --worker-id 1 \
  --batch-size 1000
```

### Output Files

- `calibre_cleanup_report.json`: Machine-readable report with all statistics and file lists
- `calibre_cleanup_report.txt`: Human-readable report with statistics and sample file lists
- `calibre_cleanup_progress.json`: Progress tracking for resumability
- `calibre_cleanup.log`: Detailed execution log

### Safety Features

- **Default dry-run**: Script defaults to dry-run mode (no deletion)
- **Explicit delete flag**: Requires `--delete` flag to actually remove files
- **Progress tracking**: Can resume from last processed file if interrupted
- **Batch processing**: Processes files in batches to avoid memory issues

### Example Output

```
================================================================================
  Calibre Library Cleanup Report
================================================================================

Generated: 2025-12-22 10:30:00
Calibre Library: /path/to/calibre/library
Container: mybookshelf2_app
Mode: DRY-RUN (no files deleted)

================================================================================
  Statistics
================================================================================

Total files scanned: 1,624,566
Files in Calibre DB: 1,624,566
Files not in Calibre DB: 0
Files with no hash match (orphaned from MyBookshelf2): 1,234
Files with hash match but no path reference (duplicates): 567
Files with hash match and path reference (in use): 1,622,765
Errors: 0
```

### Documentation

- `MIGRATION_GUIDE.md` - Detailed migration guide
- `OPTIMIZATION_IDEAS.md` - Additional optimization ideas
- `WORKER_SCALING_GUIDE.md` - Worker scaling recommendations
- `TESTING_GUIDE.md` - Testing procedures
- `CHANGELOG.md` - Complete changelog of all changes

## Troubleshooting

### Slow Page Loading

If pages are loading slowly, especially with large libraries:

1. **Check if performance index exists:**
   ```bash
   docker exec mybookshelf2_db psql -U ebooks -d ebooks -c "\d ebook" | grep ix_ebook_created
   ```

2. **Run database migration if index is missing:**
   ```bash
   docker exec mybookshelf2_app python3 manage.py migrate_tables
   ```

3. **Verify threading is enabled:**
   - Check `server.py` contains `threaded=True` in `app.run()`
   - Restart app: `docker restart mybookshelf2_app`

4. **Check database size:**
   ```bash
   docker exec mybookshelf2_db psql -U ebooks -d ebooks -c "SELECT COUNT(*) FROM ebook;"
   ```
   - Large libraries (500k+ ebooks) may take a moment to load first page
   - Subsequent pages should load quickly with the index

### Database Migration Issues

If migrations fail:

1. **Backup database first:**
   ```bash
   docker exec mybookshelf2_db pg_dump -U ebooks ebooks > backup.sql
   ```

2. **Check current version:**
   ```bash
   docker exec mybookshelf2_db psql -U ebooks -d ebooks -c "SELECT version FROM version;"
   ```

3. **Review migration files:**
   - Check `sql/migration/v{N}.sql` files
   - Ensure SQL syntax is correct

## Documentation

See `INSTALLATION_COMPLETE.md` for detailed installation documentation.

