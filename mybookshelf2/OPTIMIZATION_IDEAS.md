# Migration Speed Optimization Ideas

## Current Bottlenecks

1. **Disk I/O**: 97-98% utilization (SATURATED)
2. **CPU**: 147% utilization (overloaded)
3. **Sequential Processing**: One file at a time per worker
4. **File Copying**: `docker cp` for each file (slow)
5. **Metadata Extraction**: Done twice (once by script, once by backend)
6. **Database Queries**: Multiple queries per file

## Optimization Ideas (Ranked by Impact)

### 1. ⭐⭐⭐ Parallel Uploads Within Workers (HIGHEST IMPACT)

**Current**: Workers process files sequentially (one at a time)
**Proposed**: Process 3-4 files concurrently per worker using ThreadPoolExecutor

**Expected Speedup**: 3-5x faster uploads
**Implementation**: Already planned in the plan, needs implementation

**Code Changes**:
```python
from concurrent.futures import ThreadPoolExecutor, as_completed

# In migrate() method:
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = {executor.submit(self.upload_file, file_path, file_hash, progress): file_path 
               for file_path in files}
    for future in as_completed(futures):
        # Handle results
```

**Benefits**:
- Better CPU utilization (while one file waits for I/O, others process)
- Better disk I/O utilization (multiple reads/writes)
- 3-5x speedup with minimal code changes

**Risks**:
- Need thread-safe progress tracking
- Need to manage concurrent database connections
- May increase memory usage slightly

---

### 2. ⭐⭐⭐ Use API /api/upload/check Before Hash Calculation (HIGH IMPACT)

**Current**: Calculate hash, then check if exists
**Proposed**: Check API first (faster), only calculate hash if needed

**Expected Speedup**: Skip hash calculation for ~50% of files (already uploaded)
**Implementation**: Add API check before `get_file_hash()`

**Code Changes**:
```python
# Before calculating hash, check API
if self.check_file_exists_via_api(file_path):
    logger.info(f"File already exists (API check): {file_path.name}")
    return True

# Only calculate hash if API check passes
file_hash = self.get_file_hash(file_path)
```

**Benefits**:
- Skip expensive hash calculation for duplicates
- Faster duplicate detection
- Reduces CPU and disk I/O

---

### 3. ⭐⭐ Skip Metadata Extraction When Already Have It (MEDIUM-HIGH IMPACT)

**Current**: Extract metadata from file, then backend extracts again
**Proposed**: Skip backend extraction if we provide complete metadata

**Expected Speedup**: 30-50% faster per file (metadata extraction is slow)
**Implementation**: Backend needs to support skipping extraction

**Current Flow**:
1. Script extracts metadata (slow: 5-30s)
2. Backend extracts metadata again (slow: 5-30s)
3. Total: 10-60s wasted per file

**Proposed Flow**:
1. Script extracts metadata (5-30s)
2. Backend uses provided metadata (instant)
3. Total: 5-30s saved per file

**Benefits**:
- Eliminates duplicate metadata extraction
- Faster uploads
- Less CPU usage

---

### 4. ⭐⭐ Batch File Operations (MEDIUM IMPACT)

**Current**: Copy files one at a time with `docker cp`
**Proposed**: Batch copy multiple files, or use tar pipe

**Expected Speedup**: 20-30% faster file copying
**Implementation**: Use `docker cp` with tar or batch operations

**Code Changes**:
```python
# Instead of copying one file at a time:
# Copy multiple files in one operation
files_to_copy = [f1, f2, f3, f4]
tar_cmd = ['tar', 'cf', '-'] + files_to_copy
docker_cmd = ['docker', 'exec', '-i', container, 'tar', 'xf', '-', '-C', '/tmp']
```

**Benefits**:
- Reduces docker command overhead
- Better disk I/O utilization
- Faster file transfers

---

### 5. ⭐ Optimize Database Queries (MEDIUM IMPACT)

**Current**: Multiple queries per file
**Proposed**: Batch queries, use connection pooling

**Expected Speedup**: 10-20% faster
**Implementation**: 
- Batch hash checks (check 100 hashes at once)
- Reuse database connections
- Cache query results

**Benefits**:
- Less database overhead
- Faster duplicate detection
- Better scalability

---

### 6. ⭐ Pre-filter Files More Efficiently (MEDIUM IMPACT)

**Current**: Query database for all files, then filter
**Proposed**: Use more efficient filtering in database query

**Expected Speedup**: 10-15% faster file discovery
**Implementation**: Optimize `find_ebook_files_from_database()` query

**Benefits**:
- Faster file discovery
- Less memory usage
- Better database performance

---

### 7. ⭐ Use Direct Database Insertion for Symlinks (MEDIUM IMPACT)

**Current**: Upload via API even in symlink mode
**Proposed**: Direct database insertion when using symlinks (bypass API)

**Expected Speedup**: 50-80% faster for symlink mode
**Implementation**: Complex - requires direct database access

**Benefits**:
- Skip HTTP upload entirely
- Skip metadata extraction
- Much faster for symlink mode

**Risks**:
- More complex code
- Bypasses API validation
- Requires careful testing

---

### 8. ⭐ Cache Metadata Extraction Results (LOW-MEDIUM IMPACT)

**Current**: Extract metadata every time
**Proposed**: Cache metadata in progress file or separate cache

**Expected Speedup**: 5-10% faster (only for retries/restarts)
**Implementation**: Store metadata in progress file

**Benefits**:
- Faster on restarts
- Less CPU usage
- Better for retries

---

### 9. ⭐ Optimize File Hash Calculation (LOW IMPACT)

**Current**: Calculate SHA1 hash for every file
**Proposed**: Use faster hash algorithm or skip for duplicates

**Expected Speedup**: 5-10% faster
**Implementation**: 
- Use MD5 instead of SHA1 (faster, but less secure - OK for deduplication)
- Or skip hash calculation if API check says file exists

**Benefits**:
- Faster hash calculation
- Less CPU usage

---

### 10. ⭐ Reduce Logging Overhead (LOW IMPACT)

**Current**: Log every file operation
**Proposed**: Reduce logging frequency, use debug level for verbose logs

**Expected Speedup**: 2-5% faster
**Implementation**: Change log levels, batch log messages

**Benefits**:
- Less I/O overhead
- Faster execution
- Still maintain important logs

---

## Recommended Implementation Order

### Phase 1: Quick Wins (High Impact, Low Risk)
1. **Parallel uploads within workers** (3-4 concurrent per worker)
2. **Use API /api/upload/check before hash calculation**
3. **Reduce logging overhead**

**Expected Speedup**: 3-5x faster overall

### Phase 2: Medium Effort (Medium-High Impact)
4. **Skip metadata extraction when already have it** (requires backend support)
5. **Batch file operations** (tar pipe for docker cp)
6. **Optimize database queries** (batch hash checks)

**Expected Speedup**: Additional 30-50% faster

### Phase 3: Advanced (High Impact, Higher Risk)
7. **Direct database insertion for symlinks** (complex but very fast)
8. **Cache metadata extraction results**

**Expected Speedup**: Additional 50-80% faster (for symlink mode)

## Current Performance Baseline

- **Upload rate**: ~0.19-1.85 files/min per worker
- **With 4 workers**: ~0.76-7.4 files/min total
- **Time per file**: 3-5 minutes (180-300 seconds)

## Expected Performance After Optimizations

### Phase 1 Only:
- **Upload rate**: ~2-9 files/min per worker
- **With 4 workers**: ~8-36 files/min total
- **Time per file**: 20-30 seconds (5-10x faster)

### Phase 1 + Phase 2:
- **Upload rate**: ~3-15 files/min per worker
- **With 4 workers**: ~12-60 files/min total
- **Time per file**: 10-20 seconds (10-20x faster)

### All Phases:
- **Upload rate**: ~5-25 files/min per worker (symlink mode)
- **With 4 workers**: ~20-100 files/min total
- **Time per file**: 2-12 seconds (15-150x faster)

## Implementation Priority

**Start with Phase 1** - These are the easiest and have the highest impact:
1. Parallel uploads (biggest win)
2. API check before hash (quick win)
3. Reduce logging (easy win)

These three alone should give you **3-5x speedup** with minimal risk.

## Notes

- **Disk I/O is saturated** - Parallel uploads will help utilize I/O better
- **CPU is overloaded** - But parallel uploads can help (while one waits for I/O, others use CPU)
- **Memory is OK** - Can handle parallel uploads
- **Don't add more workers** - Optimize existing ones instead




