# Tar File Upload Flow Documentation

## Overview

This document describes the complete flow of how tar files containing books are processed and uploaded to MyBookshelf2. The system supports multiple workers, automatic duplicate detection, resume capability, reuse of existing extraction folders, and automatic recovery from stopped workers.

## Architecture

### Components

1. **`upload_tar_files.py`** - Main worker script that processes tar files
2. **`parallel_upload_tars.py`** - Launcher that distributes tar files among multiple workers
3. **`restart_tar_workers.sh`** - Convenience script to restart all tar upload workers
4. **Auto-monitor** - Supervises workers and automatically restarts stuck workers

## Complete Flow

### 1. Initialization Phase

```
User starts workers → parallel_upload_tars.py
    ↓
Scans source directory for .tar files
    ↓
Distributes tar files among workers (round-robin)
    ↓
Launches upload_tar_files.py for each worker
```

**Example:**
- 28 tar files, 4 workers
- Worker 1: tar files 1, 5, 9, 13, 17, 21, 25
- Worker 2: tar files 2, 6, 10, 14, 18, 22, 26
- Worker 3: tar files 3, 7, 11, 15, 19, 23, 27
- Worker 4: tar files 4, 8, 12, 16, 20, 24, 28

### 2. Worker Startup

```
Worker starts → upload_tar_files.py
    ↓
Loads progress file (migration_progress_worker{N}.json)
    ↓
Checks for current_tar (resume from interruption)
    ↓
If current_tar exists:
    → Resume processing that tar file first
    → Then continue with remaining tar files
Else:
    → Process all assigned tar files in order
```

**Progress File Structure:**
```json
{
    "completed_tars": ["tar1.tar", "tar2.tar"],
    "current_tar": "tar3.tar",  // Currently processing (or was interrupted)
    "tar_progress": {
        "tar1.tar": {"status": "completed", "files_uploaded": 1000}
    },
    "completed_files": {
        "file_hash_1": {"file": "path/to/file.fb2", "status": "uploaded"},
        "file_hash_2": {"file": "path/to/file2.fb2", "status": "already_exists"}
    }
}
```

### 3. Processing a Single Tar File

For each tar file assigned to the worker:

```
process_tar_file(tar_path)
    ↓
Step 1: Check for existing extraction folder FIRST
    ├─→ find_existing_extraction_folder(tar_name)
    │   ├─→ Searches tar_extraction_temp/ for folders matching tar name
    │   ├─→ Finds folders with actual files (not empty)
    │   ├─→ Sorts by file count (most files first)
    │   └─→ Returns best folder (most files, most recent)
    │
    ├─→ If found: 
    │   ├─→ Use existing folder (skip extraction - saves hours!)
    │   ├─→ Mark as reused (won't delete it)
    │   └─→ Start processing files immediately
    │
    └─→ If NOT found: 
        └─→ Extract tar file to new folder
            └─→ extract_tar_file() creates: tar_extraction_temp/{tar_name}_{timestamp}/
```

**Key Point**: Workers **always check for existing extraction folders first** before extracting. This means:
- If books are already extracted → Use existing folder (no extraction needed)
- If no extraction exists → Extract tar file
- This saves hours of extraction time when folders already exist

**Extraction Folder Structure:**
```
tar_extraction_temp/
├── pilimi-zlib2-16470000-16579999_1766592979/  (timestamp folder)
│   └── pilimi-zlib2-16470000-16579999/         (extracted folder with files)
│       ├── 16470000  (MOBI ebook, no extension)
│       ├── 16470001  (MOBI ebook, no extension)
│       └── ...
```

### 4. File Discovery and Processing (Incremental)

```
Step 2: Load duplicate detection data
    ├─→ Load own progress file → completed_hashes (worker-specific)
    ├─→ Load all workers' progress files → all_workers_completed (cross-worker)
    └─→ Load database hashes → existing_hashes (all uploaded files)
    
Step 3: Incremental file discovery
    ├─→ find_and_process_ebook_files_incremental()
    │   ├─→ Scans directory recursively (rglob)
    │   ├─→ For each file:
    │   │   ├─→ If has extension (.mobi, .epub, etc.) → ebook
    │   │   └─→ If no extension → detect_file_type() (magic bytes)
    │   └─→ Yields files as they're found (generator)
    │
    └─→ Files collected in batches (batch_size = 1000)
```

**File Type Detection:**
- Files with extensions: Direct match (`.mobi`, `.epub`, `.fb2`, `.pdf`, etc.)
- Files without extensions: Magic bytes detection
  - MOBI: `BOOKMOBI` at offset 0x3C
  - EPUB: ZIP signature `PK` at start
  - PDF: `%PDF` at start
  - FB2: XML declaration `<?xml` or `<FictionBook`

### 5. Batch Processing (Optimized)

```
When batch is full (1000 files):
    ↓
Step 4: Batch hash calculation (parallel)
    ├─→ Calculate hashes for all files in parallel (4 workers)
    ├─→ Much faster than sequential (4x speedup)
    └─→ Creates file_hash_map: {file_path → file_hash}
    
Step 5: Batch duplicate checking
    ├─→ For each file in batch:
    │   ├─→ Check progress file (fastest, O(1))
    │   ├─→ Check existing_hashes (in-memory set, O(1))
    │   └─→ If duplicate: Skip, mark in progress
    │
    └─→ Filter to files_to_upload (only new files)
    
Step 6: Parallel upload
    ├─→ Upload files_to_upload in parallel (1 concurrent per worker, default)
    ├─→ Each upload:
    │   ├─→ Prepare metadata (extract from file)
    │   ├─→ Copy to container (/tmp/)
    │   ├─→ Call CLI: python3 cli/mbs2.py upload --file ...
    │   └─→ Update progress on success
    │
    └─→ Save progress after batch
    
**Note**: Default `parallel_uploads` is 1 (reduced from 3) to prevent server overload. 
With 4 workers × 1 parallel upload = 4 concurrent uploads total (was 12 with 3 per worker).
```

### 6. Duplicate Detection (Three-Layer System)

```
Layer 1: Progress File (Worker-Specific)
    ├─→ Fastest check (O(1) dictionary lookup)
    ├─→ Contains files this worker has processed
    └─→ File: migration_progress_worker{N}.json
    
Layer 2: All Workers' Progress Files (Cross-Worker)
    ├─→ Loads completed_files from all workers
    ├─→ Prevents processing files done by other workers
    └─→ Updated in real-time
    
Layer 3: Database Hash Cache (All Uploaded Files)
    ├─→ existing_hashes: set of (file_hash, file_size) tuples
    ├─→ Loaded from MyBookshelf2 database (lazy loading)
    ├─→ Contains ALL files uploaded by ANY worker
    └─→ Refreshed periodically to pick up new uploads
```

**Duplicate Check Flow:**
```
For each file:
    1. Check progress file → Skip if found
    2. Check all workers' progress → Skip if found
    3. Check existing_hashes → Skip if found, mark in progress
    4. Upload if not found in any layer
```

### 7. Upload Process

```
upload_file_from_tar(file_path, file_hash, progress, extracted_folder)
    ↓
Step 1: Prepare file
    ├─→ prepare_file_for_upload_no_conversion()
    │   ├─→ Detect file type if no extension
    │   ├─→ Extract metadata (ebook-meta)
    │   └─→ Fix language codes (rus → ru)
    │
    └─→ Returns: (file_path, is_temp, metadata)
    
Step 2: Copy to container
    ├─→ If running in container: shutil.copy2()
    └─→ If running on host: docker cp file container:/tmp/
    
Step 3: Build upload command
    └─→ python3 cli/mbs2.py upload --file /tmp/filename --title ... --author ...
    
Step 4: Execute upload (with retries)
    ├─→ Retry on connection errors (max 3 attempts)
    ├─→ Handle "already exists" response
    └─→ Update progress on success
    
Step 5: Cleanup
    └─→ Remove copied file from container /tmp/
```

### 8. Progress Tracking

```
After each batch:
    ├─→ Save progress to migration_progress_worker{N}.json
    │   ├─→ completed_files: {file_hash → file_info}
    │   ├─→ current_tar: tar file being processed
    │   └─→ tar_progress: {tar_name → result}
    │
    └─→ Thread-safe atomic write (temp file + rename)
    
After tar file completion:
    ├─→ Mark tar as completed in completed_tars
    ├─→ Clear current_tar
    └─→ Save progress
```

### 9. Cleanup

```
After processing tar file:
    ├─→ If reused existing folder: Keep it (may be used by other workers)
    └─→ If created new folder: Delete extraction directory
        └─→ shutil.rmtree(extract_dir)
```

## Resume Capability

### On Worker Restart

```
Worker restarts
    ↓
Loads progress file
    ↓
Checks current_tar
    ↓
If current_tar exists:
    ├─→ Resume processing that tar file
    ├─→ Skip files in completed_files
    └─→ Continue with remaining tar files
Else:
    └─→ Process all assigned tar files
```

### Within Tar File

```
When processing files in extracted folder:
    ├─→ For each file found:
    │   ├─→ Calculate hash
    │   ├─→ Check if hash in completed_files → Skip
    │   ├─→ Check if hash in existing_hashes → Skip
    │   └─→ Upload if new
    │
    └─→ Progress saved after each batch
```

## Cross-Worker Coordination

### Avoiding Duplicates

1. **Progress Files**: Each worker maintains its own progress file
2. **All Workers Check**: Workers load all progress files to see what others have done
3. **Database Cache**: `existing_hashes` contains all files in MyBookshelf2 database
4. **Real-time Updates**: Workers update `existing_hashes` cache after uploads

### Example Scenario

```
Worker 2 processing file "16470001":
    ├─→ Checks own progress → Not found
    ├─→ Checks Worker 3 progress → Not found
    ├─→ Checks Worker 4 progress → Not found
    ├─→ Checks existing_hashes → Not found
    └─→ Uploads file
    
Worker 3 processing same file later:
    ├─→ Checks own progress → Not found
    ├─→ Checks Worker 2 progress → FOUND! Skip
    └─→ No upload needed
```

### Processing Tar Files from Stopped Workers

**Problem**: When a worker stops, its assigned tar files remain unprocessed.

**Solution**: Other workers automatically detect and process tar files from stopped workers.

```
After worker finishes assigned tar files:
    ↓
Checks for orphaned extraction folders
    ↓
Checks for tar files from stopped workers
    ├─→ Scans all progress files
    ├─→ Identifies workers that aren't running (no process)
    ├─→ Finds their uncompleted tar files
    └─→ Processes those tar files
```

**How it works:**
1. Worker finishes all assigned tar files
2. Worker scans all `migration_progress_worker*.json` files
3. For each progress file:
   - Extracts worker ID from filename
   - Checks if worker process is running (`pgrep`)
   - If worker is stopped and has uncompleted tar files → Add to processing queue
4. Processes tar files from stopped workers using same logic as regular tar files
5. Skips already completed tar files (prevents duplicate work)

**Example:**
```
Initial state:
- Worker 1: assigned tar1.tar, tar5.tar
- Worker 2: assigned tar2.tar, tar6.tar (STOPS)
- Worker 3: assigned tar3.tar, tar7.tar
- Worker 4: assigned tar4.tar, tar8.tar

After Worker 2 stops:
- Worker 1 finishes tar1.tar, tar5.tar
- Worker 1 detects: tar2.tar, tar6.tar from stopped Worker 2
- Worker 1 processes tar2.tar and tar6.tar ✅
- No tar files left unprocessed!
```

**Benefits:**
- **Automatic Recovery**: No manual intervention needed
- **No Tar Files Left Behind**: All tar files get processed eventually
- **Safe**: Duplicate detection prevents re-uploading files
- **Efficient**: Workers automatically pick up work from stopped workers

## Existing Extraction Folder Reuse

### How It Works

**Workers ALWAYS check for existing extraction folders FIRST before extracting.**

```
process_tar_file() called
    ↓
Step 1: Check for existing extraction folder
    ├─→ find_existing_extraction_folder(tar_name)
    │   ├─→ Searches tar_extraction_temp/ for folders matching pattern: {tar_name}_*
    │   ├─→ For each matching folder:
    │   │   ├─→ Finds nested extracted folder (e.g., pilimi-zlib2-16470000-16579999/)
    │   │   └─→ Counts files in folder
    │   ├─→ Filters to folders that actually have files (not empty)
    │   ├─→ Sorts by file count (most files first), then by modification time
    │   └─→ Returns best folder (most files, most recent)
    │
    ├─→ If found: 
    │   ├─→ Use existing folder (skip extraction completely)
    │   ├─→ Log: "Using existing extraction folder for {tar_name}"
    │   ├─→ Mark as reused (won't delete it in cleanup)
    │   └─→ Proceed directly to file processing
    │
    └─→ If NOT found:
        └─→ Extract tar file to new folder
            └─→ extract_tar_file() creates: tar_extraction_temp/{tar_name}_{timestamp}/
```

### Benefits

- **Saves Time**: No need to re-extract large tar files (can take 20+ hours for 300GB tar files)
- **Saves Disk I/O**: Reuses existing extraction, avoids redundant disk writes
- **Cross-Worker**: Multiple workers can process same extracted folder safely
- **Safe**: Workers check duplicates before uploading (won't upload same file twice)
- **Automatic**: Workers automatically find and use best existing folder

### Example

```
Worker 2 processing "pilimi-zlib2-16470000-16579999.tar":
    ↓
Checks tar_extraction_temp/ for folders:
    - pilimi-zlib2-16470000-16579999_1766590125/ (0 files) ❌
    - pilimi-zlib2-16470000-16579999_1766592979/ (83,776 files) ✅
    - pilimi-zlib2-16470000-16579999_1766675196/ (0 files) ❌
    ↓
Selects: pilimi-zlib2-16470000-16579999_1766592979/ (most files)
    ↓
Uses existing folder → Skips extraction → Starts processing immediately
```

## File Processing Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Worker Startup                            │
└──────────────────────┬──────────────────────────────────────┘
                        │
                        ▼
            ┌───────────────────────┐
            │ Load Progress File    │
            │ Check current_tar     │
            └───────────┬────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │ For each assigned tar file:   │
        └───────────┬───────────────────┘
                    │
                    ▼
    ┌───────────────────────────────┐
    │ Check for existing extraction │
    │ folder                        │
    └───────┬───────────────────────┘
            │
        ┌───┴───┐
        │       │
    Found?   Not Found?
        │       │
        ▼       ▼
    Use      Extract
    Existing  New
        │       │
        └───┬───┘
            │
            ▼
    ┌───────────────────────────────┐
    │ Load duplicate detection:      │
    │ - Own progress                 │
    │ - All workers' progress        │
    │ - Database hashes               │
    └───────────┬───────────────────┘
                │
                ▼
    ┌───────────────────────────────┐
    │ Incremental file discovery    │
    │ (as files found, yield them)  │
    └───────────┬───────────────────┘
                │
                ▼
    ┌───────────────────────────────┐
    │ Collect files in batches      │
    │ (batch_size = 1000)           │
    └───────────┬───────────────────┘
                │
                ▼
    ┌───────────────────────────────┐
    │ Batch calculate hashes       │
    │ (parallel, 4 workers)         │
    └───────────┬───────────────────┘
                │
                ▼
    ┌───────────────────────────────┐
    │ Batch check duplicates       │
    │ (filter already processed)   │
    └───────────┬───────────────────┘
                │
                ▼
    ┌───────────────────────────────┐
    │ Parallel upload new files    │
    │ (1 concurrent per worker)    │
    └───────────┬───────────────────┘
                │
                ▼
    ┌───────────────────────────────┐
    │ Save progress                 │
    │ Update completed_files        │
    └───────────┬───────────────────┘
                │
                ▼
    ┌───────────────────────────────┐
    │ Cleanup extraction folder     │
    │ (if we created it)            │
    └───────────────────────────────┘
```

## Key Features

### 1. Incremental Processing
- Files are processed as they're discovered (no waiting for full scan)
- Uploads start immediately after file detection
- Progress visible in real-time

### 2. Batch Optimization
- Hash calculation in parallel (4x faster)
- Batch duplicate checking (efficient)
- Only new files uploaded (skips duplicates)

### 3. Cross-Worker Duplicate Detection
- Checks all workers' progress files
- Checks database hash cache
- Prevents duplicate uploads across workers

### 4. Resume Capability
- Resumes from `current_tar` on restart
- Skips already-processed files
- Continues with remaining tar files

### 5. Existing Folder Reuse
- **Always checks for existing extraction folders FIRST** before extracting
- Automatically finds and reuses extraction folders (saves 20+ hours per large tar file)
- Only extracts when no existing folder is found
- Multiple workers can process same folder safely

### 6. Processing Tar Files from Stopped Workers
- Automatically detects stopped workers (no running process)
- Identifies their uncompleted tar files from progress files
- Other workers pick up and process tar files from stopped workers
- Ensures no tar files are left unprocessed when workers stop
- Safe duplicate detection prevents re-uploading files

### 6. Auto-Monitor Integration
- Auto-monitor detects tar upload workers
- Automatically restarts stuck workers
- Recognizes tar extraction as valid progress (won't kill during extraction)

## Progress Files

### Location
- `migration_progress_worker{N}.json` - One per worker

### Structure
```json
{
    "completed_tars": ["tar1.tar", "tar2.tar"],
    "current_tar": "tar3.tar",
    "tar_progress": {
        "tar1.tar": {
            "status": "completed",
            "files_processed": 83776,
            "files_uploaded": 50000,
            "errors": 0
        }
    },
    "completed_files": {
        "file_hash_sha1": {
            "file": "sanitized/path/to/file.fb2",
            "status": "uploaded",
            "uploaded_at": "2025-12-25 10:00:00"
        }
    }
}
```

## Log Files

### Worker Logs
- `migration_worker{N}.log` - Main worker log
- Contains: extraction, scanning, upload progress, errors

### Parallel Launcher Log
- `parallel_tar_upload.log` - Launcher log
- Contains: worker assignments, startup messages

### Auto-Monitor Log
- `auto_monitor.log` - Auto-monitor activity
- Contains: worker health checks, restart actions

## Usage Examples

### Start 4 Workers
```bash
python3 parallel_upload_tars.py /media/haimengzhou/16TB985-CP18TBCD \
    --workers 4 \
    --batch-size 1000 \
    --parallel-uploads 1
```

### Restart All Workers
```bash
./restart_tar_workers.sh
```

### Monitor Progress
```bash
# View worker logs
tail -f migration_worker2.log

# Monitor all workers
python3 monitor_migration.py

# Auto-monitor (automatic supervision)
python3 auto_monitor/monitor.py
```

## Performance Optimizations

1. **Parallel Hash Calculation**: 4x faster than sequential
2. **Batch Duplicate Checking**: O(1) lookups in sets
3. **Incremental Processing**: No waiting for full scan
4. **Existing Folder Reuse**: Saves hours of extraction
5. **Cross-Worker Detection**: Prevents duplicate work
6. **Stopped Worker Recovery**: Automatically redistributes work from stopped workers

## Error Handling

- **Extraction Failures**: Logged, tar marked as failed, continue with next
- **Upload Failures**: Retry up to 3 times with exponential backoff
- **Duplicate Files**: Automatically skipped, logged as success
- **Metadata Extraction Failures**: Uses filename as fallback
- **Worker Crashes**: Auto-monitor detects and restarts

## Cleanup

- **Extraction Folders**: 
  - Reused folders: Kept (may be used by other workers)
  - New folders: Deleted after processing
- **Progress Files**: Persisted for resume capability
- **Log Files**: Rotated/archived as needed

## Summary

The tar upload system provides a robust, efficient, and fault-tolerant way to process large tar files containing books. It automatically handles duplicates, resumes from interruptions, reuses existing extractions, coordinates across multiple workers to maximize throughput while avoiding duplicate work, and automatically recovers from stopped workers by redistributing their tar files to other workers.

## Configuration

### Parallel Uploads

**Default**: `parallel_uploads = 1` (reduced from 3 to prevent server overload)

**Impact**:
- With 4 workers × 1 parallel upload = **4 concurrent uploads total**
- Previously: 4 workers × 3 parallel uploads = 12 concurrent uploads (could overload server)
- **67% reduction** in concurrent uploads for better stability

**Configuration**:
- Can be adjusted via `--parallel-uploads N` parameter (range: 1-10)
- Auto-monitor can also adjust this dynamically based on system load
- Lower values = less server load, more stable
- Higher values = faster uploads, but may overload server

