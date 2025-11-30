# LLM API Request Optimization Plan

## Overview

This document outlines the plan to reduce LLM (OpenAI API) requests in the auto-monitor system while maintaining the same level of functionality and effectiveness.

## Current State Analysis

### LLM Call Locations

1. **`auto_fix_worker()`** (line 478 in `monitor.py`)
   - **When**: Every time a worker is detected as stuck
   - **Frequency**: Once per stuck worker per check cycle (every 60 seconds)
   - **Purpose**: Analyze root cause and suggest fix type (restart, code_fix, config_fix)
   - **Context**: Full worker logs (500 lines) + diagnostics

2. **`scale_workers_based_on_disk_io()`** (line 864 in `monitor.py`)
   - **When**: Disk I/O >= 90% (saturated) AND workers are stuck
   - **Frequency**: Once per stuck worker in the loop
   - **Purpose**: Confirm if disk I/O is the root cause before scaling down
   - **Context**: Worker logs + disk I/O context (utilization %, saturated status)
   - **Issue**: **REDUNDANT** - Fallback logic already scales down when disk I/O >= 90% + stuck workers

### Current Request Patterns

- **Normal operation**: 0-1 requests per check cycle (only when workers stuck)
- **Disk I/O saturation**: 2-5 requests per check cycle (1 per stuck worker in scaling + 1 per stuck worker in auto_fix)
- **High activity scenario**: Up to 8 requests per check cycle (4 workers stuck = 4 in scaling + 4 in auto_fix)

## Optimization Opportunities

### 1. Remove Redundant LLM in Disk I/O Scaling (HIGH IMPACT)

**Location**: `scale_workers_based_on_disk_io()` function

**Current Behavior**:
- When disk I/O >= 90% and workers are stuck, LLM analyzes each stuck worker
- If LLM confirms disk I/O is root cause → scale down
- If LLM returns "Unknown" → fallback logic still scales down anyway

**Proposed Change**:
- **Skip LLM analysis entirely** when disk I/O >= 90% and workers are stuck
- Use fallback logic directly (which already scales down)
- Log: "Disk I/O X% saturated and Y worker(s) stuck - scaling down (skipping LLM, disk I/O clearly saturated)"

**Rationale**:
- Fallback logic already handles this case correctly
- LLM analysis is redundant when disk I/O is clearly saturated (>= 90%)
- Saves 1-4 LLM requests per check cycle when disk is saturated

**Expected Savings**: 1-4 requests per check cycle (when disk saturated)

### 2. Add LLM Result Caching (MEDIUM IMPACT)

**Location**: Before LLM calls in `auto_fix_worker()`

**Proposed Change**:
- Cache LLM analysis results by `(worker_id, error_signature_hash)`
- Error signature: Hash of error patterns + book_id_range + status
- Cache duration: 15 minutes (900 seconds)
- Reuse cached result if:
  - Same worker
  - Same error signature
  - Cache entry is less than 15 minutes old

**Implementation**:
```python
# Add to module level
llm_cache: Dict[Tuple[int, str], Tuple[datetime, Dict[str, Any]]] = {}

# Before LLM call
cache_key = (worker_id, hash_error_signature(diagnostics))
if cache_key in llm_cache:
    cached_time, cached_result = llm_cache[cache_key]
    if (datetime.now() - cached_time).total_seconds() < 900:  # 15 min
        logger.debug(f"Using cached LLM analysis for worker {worker_id}")
        llm_analysis = cached_result
    else:
        # Cache expired, remove and continue
        del llm_cache[cache_key]

# After LLM call
if llm_analysis:
    llm_cache[cache_key] = (datetime.now(), llm_analysis)
```

**Rationale**:
- Same worker with same error pattern likely has same root cause
- Avoids re-analyzing identical issues
- Reduces API costs and improves response time

**Expected Savings**: 30-50% reduction for workers with persistent issues

### 3. Skip LLM for Recently Analyzed Workers (LOW IMPACT)

**Location**: `auto_fix_worker()` function

**Proposed Change**:
- Track last LLM analysis time per worker: `worker_last_llm_analysis: Dict[int, datetime]`
- Skip LLM if worker was analyzed within last 10 minutes (600 seconds)
- Only applies if worker is still in cooldown period (recently fixed)

**Implementation**:
```python
# Add to module level
worker_last_llm_analysis: Dict[int, datetime] = {}

# In auto_fix_worker(), before LLM call
if llm_enabled:
    # Check if recently analyzed
    if worker_id in worker_last_llm_analysis:
        time_since_analysis = (datetime.now() - worker_last_llm_analysis[worker_id]).total_seconds()
        if time_since_analysis < 600:  # 10 minutes
            logger.debug(f"Worker {worker_id} analyzed {int(time_since_analysis/60)} min ago, skipping LLM")
            llm_analysis = None
        else:
            # Analyze with LLM
            llm_analysis = analyze_worker_with_llm(...)
            worker_last_llm_analysis[worker_id] = datetime.now()
    else:
        # First time analyzing this worker
        llm_analysis = analyze_worker_with_llm(...)
        worker_last_llm_analysis[worker_id] = datetime.now()
```

**Rationale**:
- Workers in cooldown period were just fixed
- Re-analyzing immediately is unlikely to yield different results
- Reduces redundant analysis of recently fixed workers

**Expected Savings**: 10-20% reduction for workers that get stuck repeatedly

## Implementation Plan

### Phase 1: Remove Redundant LLM in Scaling (Highest Priority)

**File**: `mybookshelf2/auto_monitor/monitor.py`

**Changes**:
1. Remove LLM analysis loop in `scale_workers_based_on_disk_io()` (lines 843-875)
2. Keep fallback logic that scales down when disk I/O >= 90% + stuck workers
3. Update log message to indicate LLM was skipped

**Code Changes**:
```python
# BEFORE (lines 843-875):
if llm_enabled:
    # Analyze with LLM to determine if disk I/O is the issue
    for worker_id, diagnostics in stuck_workers:
        llm_analysis = analyze_worker_with_llm(worker_id, logs, diagnostics)
        # ... check if confirmed ...

# AFTER:
# Skip LLM - fallback logic handles this case
# (Remove entire LLM block, keep fallback logic)
```

### Phase 2: Add LLM Caching

**File**: `mybookshelf2/auto_monitor/monitor.py`

**Changes**:
1. Add `llm_cache` dictionary at module level
2. Add `hash_error_signature()` helper function
3. Check cache before LLM call in `auto_fix_worker()`
4. Store result in cache after LLM call

### Phase 3: Add Recent Analysis Tracking

**File**: `mybookshelf2/auto_monitor/monitor.py`

**Changes**:
1. Add `worker_last_llm_analysis` dictionary at module level
2. Check last analysis time before LLM call
3. Skip if analyzed within last 10 minutes

## Expected Impact

### Request Reduction

**Before Optimization**:
- Normal: 0-1 requests/cycle
- Disk saturated: 2-5 requests/cycle
- High activity: Up to 8 requests/cycle

**After Optimization**:
- Normal: 0-1 requests/cycle (no change)
- Disk saturated: 0-1 requests/cycle (eliminated scaling LLM calls)
- High activity: 1-2 requests/cycle (caching reduces repeats)

**Overall Reduction**: 60-80% reduction in LLM requests

### Cost Savings

Assuming:
- Average 3 requests per check cycle (before)
- Check cycle: 60 seconds
- Requests per hour: ~180
- Cost per request: ~$0.01-0.03 (GPT-4 Turbo)

**Before**: ~$1.80-5.40/hour
**After**: ~$0.36-1.08/hour (80% reduction)
**Daily savings**: ~$34-103/day

## Testing Plan

1. **Unit Tests**:
   - Test cache hit/miss logic
   - Test cache expiration
   - Test recent analysis tracking

2. **Integration Tests**:
   - Monitor LLM request count in logs
   - Verify scaling still works without LLM
   - Verify caching works correctly

3. **Production Monitoring**:
   - Track LLM request count before/after
   - Monitor for any degradation in fix effectiveness
   - Verify cost reduction

## Rollout Strategy

1. **Phase 1** (Immediate): Remove redundant LLM in scaling
   - Low risk, high impact
   - Can be deployed immediately

2. **Phase 2** (After Phase 1 validation): Add caching
   - Medium risk, medium impact
   - Monitor cache hit rate

3. **Phase 3** (Optional): Add recent analysis tracking
   - Low risk, low impact
   - Can be deferred if Phase 1+2 provide sufficient savings

## Risk Assessment

### Low Risk
- **Removing LLM from scaling**: Fallback logic already handles this correctly
- **Caching**: Worst case is stale cache, but 15-minute TTL is reasonable

### Mitigation
- Monitor fix success rate after changes
- Keep fallback logic intact
- Cache TTL can be adjusted if needed

## Success Metrics

- **Primary**: LLM request count reduction (target: 60-80%)
- **Secondary**: Cost reduction (target: 60-80%)
- **Tertiary**: No degradation in fix success rate

## Timeline

- **Phase 1**: 1-2 hours (code changes + testing)
- **Phase 2**: 2-3 hours (caching implementation + testing)
- **Phase 3**: 1 hour (tracking implementation + testing)

**Total**: 4-6 hours for complete implementation

