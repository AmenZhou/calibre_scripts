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

## Bulk Migration from Calibre

### Overview

The `bulk_migrate_calibre.py` script provides high-performance migration from Calibre libraries to MyBookshelf2 with multiple optimizations for speed and efficiency.

### Performance Optimizations

#### Phase 1: Parallel Uploads (Implemented)
- **Parallel Processing**: Each worker processes 3 files concurrently using ThreadPoolExecutor
- **Configuration**: Use `--parallel-uploads N` parameter (default: 3, range: 1-10)
- **Speedup**: 3-5x faster than sequential processing
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
  --parallel-uploads 3
```

**Multiple Workers (Recommended):**
```bash
python3 parallel_migrate.py /path/to/calibre/library \
  --workers 4 \
  --use-symlinks \
  --batch-size 10000 \
  --parallel-uploads 3
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

- `--parallel-uploads N`: Number of concurrent uploads per worker (default: 3)
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

### Documentation

- `MIGRATION_GUIDE.md` - Detailed migration guide
- `OPTIMIZATION_IDEAS.md` - Additional optimization ideas
- `WORKER_SCALING_GUIDE.md` - Worker scaling recommendations
- `TESTING_GUIDE.md` - Testing procedures
- `CHANGELOG.md` - Complete changelog of all changes

## Documentation

See `INSTALLATION_COMPLETE.md` for detailed installation documentation.

