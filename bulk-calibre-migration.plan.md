<!-- f49fb645-80e0-4e41-963f-ed1ed12c3780 0de48cbd-b548-4b2a-9cd6-5d810f8a6b15 -->
# Optimize Migration Script for 2 Million Books

## Current Performance

- **Rate**: ~28.6 books/minute (~1,714/hour, ~41,143/day)
- **Actual books in Calibre**: 1,726,138 books (1,624,566 files)
- **Estimated time for 1.7M books**: ~42 days (single-threaded)
- **With 8 parallel workers**: ~5.3 days

## Goals

1. Add parallel processing to reduce migration time
2. Implement batch processing with resume capability
3. Optimize database operations for large-scale migration
4. Add progress monitoring and reporting
5. Handle errors gracefully with retry logic
6. **Use symlinks to avoid file duplication** (saves ~1.6 TB disk space)

## File Handling: Copy vs Symlink

### Current Behavior

The migration script supports two modes:

1. **Without `--use-symlinks` (default)**: Files are COPIED
   - Files are copied from Calibre library to MyBookshelf2 storage
   - Result: Files are duplicated (Calibre + MyBookshelf2)
   - Disk space: ~2x library size (1.6 TB → ~3.2 TB)
   - **NOT RECOMMENDED for large libraries**

2. **With `--use-symlinks`**: Files are SYMLINKED
   - Files are initially copied for upload, then replaced with symlinks
   - Result: No duplication, MyBookshelf2 uses original Calibre files
   - Disk space: Minimal (only database + metadata, ~371 GB)
   - **RECOMMENDED for 1.7M books**

### Optimization Opportunity: Direct Database Insertion with Symlinks

**Current implementation** (even with `--use-symlinks`):
- Still copies file to container (`docker cp`)
- Uses CLI upload (which uploads file via HTTP, processes metadata, creates DB entries)
- Then replaces copied file with symlink

**Potential optimization** (for symlink mode):
- **Skip file copy entirely** - no `docker cp` needed
- **Skip HTTP file upload** - no `/api/upload` call needed
- Extract metadata directly from Calibre file (already done)
- **Insert directly into MyBookshelf2 database** (bypass CLI/API)
- Create symlink directly in MyBookshelf2 storage structure
- **Result**: Much faster - only SQL operations + metadata extraction, no file I/O

**Performance impact**:
- Current: ~28.6 books/minute (includes file copy + HTTP upload + processing)
- With direct DB insertion: Potentially **10-100x faster** (SQL-only operations)
- Estimated: Could process 1.7M books in **hours instead of days**

**Implementation considerations**:
- Need to replicate CLI upload logic (metadata extraction, database schema)
- Must handle all edge cases (duplicate detection, format handling, etc.)
- Requires careful testing to ensure data integrity
- Need to understand MyBookshelf2 database schema fully
- Must create proper directory structure for symlinks

**Key insight**: With symlinks, we don't need to move/copy files at all - just:
1. Query Calibre database for file paths (FAST - already implemented)
2. Extract metadata from files (FAST - already done)
3. Insert records into MyBookshelf2 database (FAST - SQL operations)
4. Create symlink (FAST - single filesystem operation)

This eliminates the slowest parts: file copying and HTTP uploads.

## Implementation Plan

### 1. Use Calibre Database Instead of File System Scanning ✅ COMPLETED

**File**: `bulk_migrate_calibre.py`

- **CRITICAL**: Replace `find_ebook_files()` to query Calibre's SQLite database instead of scanning filesystem
- Calibre stores all book metadata and file paths in `metadata.db` SQLite database
- Query database for book files (much faster than `find` on 2M files)
- Use SQL queries to get file paths directly from database

**Benefits**:
- **Fast**: Database query is milliseconds vs minutes/hours for `find`
- **Accurate**: Gets exact file paths from Calibre's metadata
- **Complete**: Includes all books registered in Calibre
- **No filesystem scanning**: Avoids the slow `find` command entirely

### 2. Add Parallel Processing Support

**File**: `bulk_migrate_calibre.py`

- Add `--workers` parameter (default: 4, max: 16)
- Use `concurrent.futures.ThreadPoolExecutor` or `multiprocessing.Pool`
- Implement thread-safe progress tracking
- Use thread-safe file operations for progress.json
- Ensure CLI tool can handle concurrent requests (may need connection pooling)

**Key changes**:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def __init__(self, ..., workers: int = 4):
    self.workers = workers
    self.progress_lock = threading.Lock()

def migrate_parallel(self, files: List[Path]):
    with ThreadPoolExecutor(max_workers=self.workers) as executor:
        futures = {executor.submit(self.upload_file, file): file for file in files}
        for future in as_completed(futures):
            # Handle results with thread-safe progress updates
```

### 3. Implement Batch Processing

**File**: `bulk_migrate_calibre.py`

- Add `--batch-size` parameter (default: 10,000)
- Process books in batches
- Save progress after each batch
- Support `--resume` flag to continue from last checkpoint
- Add batch statistics (time per batch, rate, ETA)

**Key features**:
- Checkpoint after each batch completion
- Store batch metadata (start time, end time, success count, error count)
- Resume from last successful batch
- Skip already processed files (using progress.json)

### 3a. **CRITICAL**: Pre-Check MyBookshelf2 Database for Existing Files

**File**: `bulk_migrate_calibre.py`

**Problem**: Workers waste time attempting to upload files that are already in MyBookshelf2 database (from other workers or previous runs). After restart, workers encounter tons of duplicates.

**Solution**: Query MyBookshelf2 database at worker startup to get all existing file hashes, cache in memory, and check before attempting upload.

**Implementation**:

```python
def load_existing_hashes_from_database(self) -> set:
    """Query MyBookshelf2 database for all existing file hashes"""
    script = f"""
import sys
sys.path.insert(0, '/app')
from app import db, create_app
from app import model

app = create_app()
with app.app_context():
    # Get all existing source hashes
    sources = db.session.query(model.Source.hash, model.Source.size).all()
    # Return as set of tuples (hash, size) for fast lookup
    result = set()
    for hash_val, size in sources:
        result.add((hash_val, size))
    print('|'.join([f"{{h}}|{{s}}" for h, s in result]))
"""
    try:
        result = subprocess.run(
            [self.docker_cmd, 'exec', self.container, 'python3', '-c', script],
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0 and result.stdout.strip():
            existing = set()
            for line in result.stdout.strip().split('|'):
                if '|' in line:
                    hash_val, size = line.split('|', 1)
                    existing.add((hash_val, int(size)))
            return existing
    except Exception as e:
        logger.warning(f"Could not load existing hashes from database: {e}")
    return set()

# In __init__:
self.existing_hashes = self.load_existing_hashes_from_database()
logger.info(f"Loaded {len(self.existing_hashes)} existing file hashes from MyBookshelf2 database")

# In upload_file, before attempting upload:
file_size = file_path.stat().st_size
if (original_file_hash, file_size) in self.existing_hashes:
    logger.info(f"File already exists in MyBookshelf2 database: {file_path.name}")
    progress["completed_files"][original_file_hash] = {
        "file": str(file_path),
        "status": "already_exists_in_db"
    }
    self.save_progress(progress)
    return True
```

**Benefits**:
- Workers skip files already in database immediately (no upload attempt)
- Works across workers (all workers check same database)
- Works after restarts (database is source of truth)
- Reduces wasted time on duplicates significantly
- Single database query at startup (fast, cached in memory)

**Performance**:
- Startup: One-time query (~1-2 seconds for 20k files)
- Runtime: In-memory set lookup (O(1), microseconds)
- Saves: ~2 seconds per duplicate file (no upload attempt needed)

### 4. Optimize Database Operations

**File**: `bulk_migrate_calibre.py` (database queries)

- Batch database queries where possible
- Cache format lookups
- Optimize `_replace_with_symlink()` database queries
- Add database connection pooling if needed
- Consider bulk insert operations for metadata

**Optimizations**:
- Pre-fetch format IDs and cache them
- Use prepared statements for repeated queries
- Minimize database round-trips in symlink creation

### 5. **OPTIMIZATION**: Direct Database Insertion for Symlink Mode

**File**: `bulk_migrate_calibre.py`

- **NEW**: Add `--direct-db-insert` flag (only works with `--use-symlinks`)
- When enabled, bypass CLI/API and insert directly into database
- Extract metadata from Calibre files directly
- Insert ebook, source, format, author records via SQL
- Create symlink directly in MyBookshelf2 storage structure
- **Skip**: File copy, HTTP upload, CLI processing

**Key implementation**:

```python
def insert_book_directly(self, calibre_file: Path, metadata: Dict[str, Any]):
    """Insert book directly into database (symlink mode only)"""
    # 1. Extract metadata (already done)
    # 2. Calculate file hash
    # 3. Get/create format ID
    # 4. Get/create language ID
    # 5. Get/create author IDs
    # 6. Insert ebook record
    # 7. Insert source record with symlink path
    # 8. Create symlink in filesystem
    # 9. Link authors to ebook
```

**Performance**: Expected 10-100x speedup vs current CLI method

### 6. Add Progress Monitoring

**File**: `bulk_migrate_calibre.py`

- Real-time progress display (books processed, rate, ETA)
- Periodic statistics logging (every 1000 books)
- Summary reports after each batch
- Error rate tracking
- Performance metrics (average time per book, bottlenecks)

**Output format**:

```
Batch 1/200: 10,000 books
  Processed: 9,987 | Success: 9,950 | Errors: 37
  Rate: 1,750 books/hour
  ETA: 5.2 days remaining
  Current: Processing book 9,987/2,000,000
```

### 7. Error Handling and Retry Logic

**File**: `bulk_migrate_calibre.py`

- Add `--max-retries` parameter (default: 3)
- Retry failed uploads with exponential backoff
- Separate error log with detailed error information
- Track error types (network, validation, conversion, etc.)
- Continue processing on errors (don't stop entire migration)

**Error categories**:
- Transient errors (network, timeout) → retry
- Validation errors (title too long) → skip and log
- Conversion errors → skip and log
- Fatal errors → stop and report

### 8. Performance Testing

**Steps**:
1. Test with 1,000 books (single-threaded baseline)
2. Test with 1,000 books (4 workers) - measure speedup
3. Test with 10,000 books (8 workers) - measure scalability
4. Test direct DB insertion with 1,000 books - measure speedup
5. Test batch processing and resume functionality
6. Monitor database performance and disk I/O
7. Identify bottlenecks and optimize

### 9. Database Optimization

**Considerations**:
- Add indexes on frequently queried columns (if not already present)
- Monitor database size and query performance
- Consider database maintenance during migration
- Plan for database backup strategy

## Files to Modify

1. **`bulk_migrate_calibre.py`**:
   - **CRITICAL**: Add pre-check of MyBookshelf2 database for existing files (prevents duplicate attempts after restarts)
   - Add parallel processing support
   - Add batch processing logic
   - Add progress monitoring
   - Add retry logic
   - Optimize database queries
   - **Add direct database insertion for symlink mode**

## Expected Improvements

- **Time reduction**: 
  - Current: 42 days (single-threaded) → 5.3 days (8 workers)
  - **With direct DB insertion + symlinks**: Potentially **hours** (10-100x faster)
- **Disk space**: ~1.6 TB saved (using symlinks instead of copying)
- **Reliability**: Resume capability prevents losing progress
- **Monitoring**: Real-time visibility into migration progress
- **Error handling**: Graceful error recovery and reporting

## Migration Strategy

**IMPORTANT**: Always use `--use-symlinks` flag for large migrations to save disk space.

1. **Phase 1**: Test with 10,000 books to validate optimizations (with `--use-symlinks`)
2. **Phase 2**: Implement direct database insertion optimization for symlink mode
3. **Phase 3**: Test optimized version with 10,000 books to measure speedup
4. **Phase 4**: Run in batches of 50,000-100,000 books
5. **Phase 5**: Monitor and adjust based on performance
6. **Phase 6**: Complete full migration with monitoring

**Command example**:
```bash
python3 bulk_migrate_calibre.py "/media/.../calibre library" \
  --use-symlinks \
  --workers 8 \
  --batch-size 10000 \
  --limit 100000
```

**Command with direct DB insertion (future)**:
```bash
python3 bulk_migrate_calibre.py "/media/.../calibre library" \
  --use-symlinks \
  --direct-db-insert \
  --workers 8 \
  --batch-size 10000
```

## Risk Mitigation

- **Database overload**: Monitor query performance, add delays if needed
- **Disk space**: With symlinks, minimal additional space needed (~371 GB vs ~3.2 TB)
- **Network issues**: Retry logic handles transient failures
- **Long-running process**: Batch processing allows for restarts
- **File I/O bottleneck**: Direct database insertion in symlink mode eliminates file copying

### To-dos

- [x] Use Calibre database instead of file system scanning (COMPLETED)
- [ ] **CRITICAL**: Implement pre-check of MyBookshelf2 database for existing files to avoid duplicate upload attempts after restarts
- [ ] Add parallel processing support using ThreadPoolExecutor with --workers parameter
- [ ] Implement batch processing with --batch-size and --resume functionality
- [ ] Optimize database queries in _replace_with_symlink() and add caching for format lookups
- [ ] Add real-time progress display, ETA calculation, and periodic statistics logging
- [ ] Implement retry logic with exponential backoff for transient errors
- [ ] **OPTIMIZATION**: Implement direct database insertion for symlink mode (skip file copy/upload)
- [ ] Test with 1,000 and 10,000 books to measure performance improvements
- [ ] Monitor database performance during large-scale migration and optimize as needed
- [ ] Document symlink mode usage and disk space savings

