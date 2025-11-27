# Testing Guide for Migration Improvements

## Changes Implemented

### 1. Critical Fix: Stale Duplicate Checking
- **What it does**: Refreshes `existing_hashes` cache periodically (every 1000 files or 10 minutes) and updates it after each successful upload
- **Why it matters**: Prevents workers from attempting duplicate uploads when other workers have already uploaded files
- **How to verify**: Watch for log messages like:
  - `"Refreshing existing file hashes from MyBookshelf2 database..."`
  - `"Refreshed existing hashes: X -> Y (added Z new hashes)"`

### 2. Filename Sanitization
- **What it does**: Removes NUL (0x00) characters from filenames and metadata before database operations
- **Why it matters**: Prevents PostgreSQL errors: "A string literal cannot contain NUL (0x00) characters"
- **How to verify**: No NUL character errors in logs, files with NUL characters should process successfully

### 3. API Connection & Retry Logic
- **What it does**: 
  - Checks API connectivity at migration start
  - Retries uploads on connection errors with exponential backoff (2s, 4s, 8s delays)
  - Up to 3 retry attempts
- **Why it matters**: Fixes Worker 1 connection failures, reduces transient error rates
- **How to verify**: Watch for:
  - `"API connectivity check passed"` or warning if failed
  - Retry messages: `"Connection error... retrying in Xs..."`
  - `"(attempt 2/3)"` or `"(attempt 3/3)"` in upload logs

### 4. Performance Monitoring
- **What it does**: 
  - Tracks upload time per file
  - Logs average upload rate (files/min) every 100 files
  - Detects and logs slow uploads (>2 minutes)
- **Why it matters**: Helps identify bottlenecks and track improvements
- **How to verify**: Look for log messages like:
  - `"Upload performance: X.XX files/min (avg Y.Ys per file over last 100 files)"`
  - `"Slow upload detected: filename took X.Xs (Y.Y min)"`
  - `"Successfully uploaded: filename (took X.Xs)"`

## Testing Steps

### Step 1: Unit Tests (Already Passed ✓)
```bash
cd /home/haimengzhou/calibre_automation_scripts/mybookshelf2
python3 test_migration_changes.py
```

### Step 2: Small Test Migration
Run a small test migration with a few files to verify end-to-end:

```bash
cd /home/haimengzhou/calibre_automation_scripts/mybookshelf2

# Test with 10 files (single worker)
python3 bulk_migrate_calibre.py "/media/haimengzhou/78613a5d-17be-413e-8691-908154970815/calibre library" \
  --worker-id 1 \
  --limit 10

# Monitor the logs for:
# - Hash refresh messages
# - Performance metrics
# - Retry attempts (if any connection issues)
# - No NUL character errors
```

### Step 3: Monitor Existing Workers
If you have workers already running, check their logs for:

```bash
# Check worker logs
tail -f migration_worker1.log | grep -E "(Refreshing|Refreshed|performance|retrying|Slow upload)"
tail -f migration_worker2.log | grep -E "(Refreshing|Refreshed|performance|retrying|Slow upload)"
tail -f migration_worker3.log | grep -E "(Refreshing|Refreshed|performance|retrying|Slow upload)"
tail -f migration_worker4.log | grep -E "(Refreshing|Refreshed|performance|retrying|Slow upload)"
```

### Step 4: Verify Hash Refresh is Working
After workers have been running for a while, you should see:
- Hash refresh messages every 1000 files processed
- Or every 10 minutes if processing is slow
- Hash counts increasing as other workers upload files

### Step 5: Check for Improvements
Compare before/after:
- **Duplicate upload attempts**: Should decrease significantly
- **Connection errors**: Should be retried automatically
- **Upload speed**: Should be similar or better (parallelization not yet added)
- **Database errors**: Should be eliminated (NUL character errors)

## Expected Log Output Examples

### Hash Refresh (Every 1000 files or 10 min):
```
2025-01-XX XX:XX:XX - INFO - Refreshing existing file hashes from MyBookshelf2 database...
2025-01-XX XX:XX:XX - INFO - Refreshed existing hashes: 125,532 -> 125,645 (added 113 new hashes)
```

### Performance Metrics (Every 100 files):
```
2025-01-XX XX:XX:XX - INFO - Upload performance: 1.85 files/min (avg 32.4s per file over last 100 files)
```

### Retry Logic (On connection errors):
```
2025-01-XX XX:XX:XX - WARNING - Connection error for filename.epub (attempt 1/3): Connection refused, retrying in 2s...
2025-01-XX XX:XX:XX - INFO - Uploading: filename.epub (attempt 2/3)
```

### Slow Upload Detection:
```
2025-01-XX XX:XX:XX - WARNING - Slow upload detected: large_file.pdf took 145.3s (2.4 min)
```

## Troubleshooting

### If hash refresh doesn't happen:
- Check that workers are processing files (should refresh every 1000 files)
- Check that time-based refresh works (should refresh every 10 minutes)

### If retry logic doesn't trigger:
- Connection errors should automatically retry
- Check logs for retry messages
- Verify API connectivity check at start

### If performance metrics don't appear:
- Should appear every 100 successful uploads
- Check that uploads are completing successfully

## Next Steps After Testing

Once testing confirms everything works:
1. Continue with parallelization implementation (ThreadPoolExecutor)
2. Monitor real-world performance improvements
3. Adjust refresh frequency if needed (currently 1000 files or 10 min)


