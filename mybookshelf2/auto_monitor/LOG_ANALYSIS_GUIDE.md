# Auto-Monitor Log Analysis Guide

This guide helps you understand how to analyze auto-monitor logs to generate reports and insights.

## Log File Locations

### Main Log Files

- **`auto_restart.log`**: Main auto-monitor activity log
  - Location: `mybookshelf2/auto_monitor/auto_restart.log`
  - Contains: All monitoring activity, worker stuck detections, LLM analyses, fix applications

- **`monitor.log`**: Detailed monitoring logs (if separate)
  - Location: `mybookshelf2/auto_monitor/monitor.log`
  - Contains: Detailed worker status checks and metrics

- **`auto_fix_history.json`**: Structured fix history
  - Location: `mybookshelf2/auto_monitor/auto_fix_history.json`
  - Contains: JSON array of all fixes applied with full details

- **Worker Logs**: Individual worker activity logs
  - Location: `mybookshelf2/migration_worker{N}.log` (where N is worker ID)
  - Contains: Worker-specific migration activity, uploads, errors

## Log Message Patterns

### Worker Stuck Detection

Pattern: `Worker {id} detected as stuck`
- Indicates a worker has been stuck for the threshold duration
- Includes: minutes stuck, status, last upload time

Example:
```
2025-11-30 16:19:47,451 - INFO - Worker 2 detected as stuck: 20 minutes uptime, no progress detected (threshold: 20.0 min)
```

### LLM Analysis Patterns

Pattern: `LLM Analysis Complete for Worker {id}`
- Root Cause: `Root Cause: {description}`
- Recommended Fix: `Recommended Fix: {restart|code_fix|config_fix|scale_down}`
- Confidence: `Confidence: {0.0-1.0}`

Example:
```
2025-11-30 16:20:02,862 - INFO - 🤖 LLM Analysis Complete for Worker 2:
2025-11-30 16:20:02,863 - INFO -    Root Cause: Unknown
2025-11-30 16:20:02,863 - INFO -    Recommended Fix: restart
2025-11-30 16:20:02,863 - INFO -    Confidence: 0.50
```

### Fix Application Patterns

Pattern: `Applying LLM Recommended {Fix Type} for Worker {id}`
- Fix types: `Restart`, `Code Fix`, `Config Fix`, `Scale Down`

Example:
```
2025-11-30 16:20:02,863 - INFO - 🔄 Applying LLM Recommended Restart for Worker 2...
```

### Success/Failure Verification

Pattern: `Worker {id} fix verified successful` or `Worker {id} fixed: {message}`

Example:
```
2025-11-30 16:20:37,608 - INFO - ✅ Worker 2 fixed: Worker 2 restarted successfully
```

### Recurring Root Cause Detection

Pattern: `Recurring root cause detected`
- Indicates a root cause has appeared multiple times
- Includes occurrence count

Example:
```
2025-11-30 16:20:37,608 - INFO - ⚠️  Recurring root cause detected for worker 2: appeared 3 time(s) before
2025-11-30 16:20:37,608 - INFO -    Suggesting code_fix (threshold: 2 occurrences)
```

## Key Metrics to Extract

### 1. Number of Stuck Worker Detections

Count how many times workers were detected as stuck per time period.

**Command:**
```bash
grep -c "detected as stuck" auto_restart.log
```

**By time period:**
```bash
grep "detected as stuck" auto_restart.log | awk '{print $1}' | sort | uniq -c
```

### 2. Root Cause Frequency Distribution

Analyze which root causes appear most frequently.

**From logs:**
```bash
grep "Root Cause:" auto_restart.log | sed 's/.*Root Cause: //' | sort | uniq -c | sort -rn
```

**From JSON history:**
```bash
cat auto_fix_history.json | jq -r '.[].llm_root_cause' | sort | uniq -c | sort -rn
```

### 3. Fix Type Distribution

Count how many times each fix type was applied.

**From logs:**
```bash
grep "Recommended Fix:" auto_restart.log | sed 's/.*Recommended Fix: //' | sort | uniq -c
```

**From JSON history:**
```bash
cat auto_fix_history.json | jq -r '.[].fix_type' | sort | uniq -c | sort -rn
```

### 4. Success Rate of Fixes

Calculate the percentage of successful fixes.

**From JSON history:**
```bash
cat auto_fix_history.json | jq '[.[] | select(.success == true)] | length'
cat auto_fix_history.json | jq 'length'
# Success rate = (successful / total) * 100
```

### 5. Average Time to Detect and Fix Issues

Calculate average time between stuck detection and fix application.

**Python script:**
```python
import json
from datetime import datetime
from pathlib import Path

history_file = Path("auto_fix_history.json")
with open(history_file) as f:
    history = json.load(f)

# Calculate time deltas (requires timestamps in diagnostics)
# This is a simplified example
for entry in history:
    timestamp = datetime.fromisoformat(entry["timestamp"])
    # Compare with worker stuck detection time if available
    print(f"Fix at {timestamp}")
```

### 6. Recurring Root Cause Patterns

Identify root causes that appear multiple times.

**From JSON history:**
```bash
cat auto_fix_history.json | jq -r '.[] | select(.recurring_root_cause == true) | "\(.worker_id): \(.llm_root_cause) (occurred \(.root_cause_occurrence_count) times)"'
```

### 7. LLM Confidence Scores Distribution

Analyze confidence levels of LLM analyses.

**From JSON history:**
```bash
cat auto_fix_history.json | jq -r '.[].llm_confidence' | awk '{sum+=$1; count++} END {print "Average:", sum/count}'
```

## Example Analysis Queries

### Extract All Root Causes

```bash
grep "Root Cause:" auto_restart.log | sed 's/.*Root Cause: //'
```

### Count Fix Types

```bash
grep "Recommended Fix:" auto_restart.log | sed 's/.*Recommended Fix: //' | sort | uniq -c
```

### Find Recurring Issues

```bash
cat auto_fix_history.json | jq '.[] | select(.recurring_root_cause == true)'
```

### Calculate Success Rate

```bash
total=$(cat auto_fix_history.json | jq 'length')
successful=$(cat auto_fix_history.json | jq '[.[] | select(.success == true)] | length')
echo "Success rate: $((successful * 100 / total))%"
```

### Time-Based Analysis

Group fixes by hour/day:

```bash
grep "Applying LLM Recommended" auto_restart.log | awk '{print $1, $2}' | cut -d: -f1 | sort | uniq -c
```

## Python Script Examples

### Parse auto_fix_history.json to Generate Statistics

```python
import json
from collections import Counter
from pathlib import Path
from datetime import datetime

history_file = Path("auto_monitor/auto_fix_history.json")

with open(history_file) as f:
    history = json.load(f)

# Root cause frequency
root_causes = [entry.get("llm_root_cause", "Unknown") for entry in history]
root_cause_counts = Counter(root_causes)
print("Root Cause Frequency:")
for cause, count in root_cause_counts.most_common(10):
    print(f"  {cause}: {count}")

# Fix type distribution
fix_types = [entry.get("fix_type", "unknown") for entry in history]
fix_type_counts = Counter(fix_types)
print("\nFix Type Distribution:")
for fix_type, count in fix_type_counts.most_common():
    print(f"  {fix_type}: {count}")

# Success rate
successful = sum(1 for entry in history if entry.get("success", False))
total = len(history)
print(f"\nSuccess Rate: {successful}/{total} ({successful/total*100:.1f}%)")

# Recurring issues
recurring = [entry for entry in history if entry.get("recurring_root_cause", False)]
print(f"\nRecurring Issues: {len(recurring)}")
for entry in recurring:
    print(f"  Worker {entry['worker_id']}: {entry.get('llm_root_cause', 'Unknown')} "
          f"(occurred {entry.get('root_cause_occurrence_count', 0)} times)")
```

### Analyze Log Files for Patterns

```python
import re
from pathlib import Path
from collections import defaultdict

log_file = Path("auto_monitor/auto_restart.log")

patterns = {
    "stuck_detections": r"detected as stuck",
    "llm_analyses": r"LLM Analysis Complete",
    "restarts": r"Applying LLM Recommended Restart",
    "code_fixes": r"Applying LLM Code Fix",
    "config_fixes": r"Applying LLM Config Fix",
    "successful_fixes": r"fix verified successful",
}

counts = defaultdict(int)

with open(log_file) as f:
    for line in f:
        for pattern_name, pattern in patterns.items():
            if re.search(pattern, line):
                counts[pattern_name] += 1

print("Pattern Counts:")
for pattern, count in sorted(counts.items()):
    print(f"  {pattern}: {count}")
```

### Generate Summary Reports

```python
import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter

history_file = Path("auto_monitor/auto_fix_history.json")

with open(history_file) as f:
    history = json.load(f)

# Filter by time period (last 24 hours)
cutoff = datetime.now() - timedelta(hours=24)
recent = [
    entry for entry in history
    if datetime.fromisoformat(entry["timestamp"]) > cutoff
]

print(f"Summary Report (Last 24 Hours)")
print(f"Total fixes: {len(recent)}")
print(f"Successful: {sum(1 for e in recent if e.get('success', False))}")
print(f"Failed: {sum(1 for e in recent if not e.get('success', False))}")

# Most common root causes
root_causes = [e.get("llm_root_cause", "Unknown") for e in recent]
print(f"\nTop Root Causes:")
for cause, count in Counter(root_causes).most_common(5):
    print(f"  {cause}: {count}")

# Fix types
fix_types = [e.get("fix_type", "unknown") for e in recent]
print(f"\nFix Types:")
for fix_type, count in Counter(fix_types).most_common():
    print(f"  {fix_type}: {count}")
```

## Report Generation Templates

### Daily Summary Report Format

```
Auto-Monitor Daily Summary - {date}
====================================

Total Fixes Applied: {count}
  - Successful: {success_count}
  - Failed: {fail_count}
  - Success Rate: {success_rate}%

Top Root Causes:
  1. {cause1}: {count1}
  2. {cause2}: {count2}
  3. {cause3}: {count3}

Fix Type Distribution:
  - Restart: {restart_count}
  - Code Fix: {code_fix_count}
  - Config Fix: {config_fix_count}
  - Scale Down: {scale_down_count}

Recurring Issues: {recurring_count}
  {list of recurring issues}

Workers Affected: {worker_list}
```

### Weekly Trend Analysis Format

```
Auto-Monitor Weekly Trend Analysis - Week of {date}
====================================================

Daily Breakdown:
  {day1}: {fixes1} fixes, {success_rate1}% success
  {day2}: {fixes2} fixes, {success_rate2}% success
  ...

Trends:
  - Most common root cause: {cause}
  - Average fixes per day: {avg}
  - Peak day: {peak_day} with {peak_count} fixes

Recurring Issues Identified:
  {list with occurrence counts}

Recommendations:
  {suggestions based on patterns}
```

### Root Cause Analysis Report Format

```
Root Cause Analysis Report - {root_cause}
=========================================

Occurrence Count: {count}
First Occurrence: {first_date}
Last Occurrence: {last_date}
Workers Affected: {worker_list}

Fix Attempts:
  - Total: {total_attempts}
  - Successful: {successful_attempts}
  - Failed: {failed_attempts}

Fix Types Applied:
  {breakdown by fix type}

Recommendation:
  {suggestion based on recurrence and success rate}
```

### Performance Metrics Report Format

```
Auto-Monitor Performance Metrics - {period}
===========================================

Detection Metrics:
  - Average time to detect: {avg_detection_time}
  - Fastest detection: {min_detection_time}
  - Slowest detection: {max_detection_time}

Fix Metrics:
  - Average time to fix: {avg_fix_time}
  - Average confidence: {avg_confidence}
  - Code fix success rate: {code_fix_success_rate}

LLM Performance:
  - Total analyses: {total_analyses}
  - Average confidence: {avg_confidence}
  - Code fix suggestions: {code_fix_count}
  - Restart suggestions: {restart_count}
```

## Tips for Effective Analysis

1. **Use JSON history for structured data**: The `auto_fix_history.json` file contains structured data that's easier to query programmatically.

2. **Combine log and JSON analysis**: Logs provide context and timestamps, JSON provides structured metrics.

3. **Filter by time period**: Focus on recent activity to identify current issues.

4. **Track recurring patterns**: Recurring root causes indicate systemic issues that may need code fixes.

5. **Monitor confidence scores**: Low confidence scores may indicate unclear issues that need investigation.

6. **Compare fix types**: Analyze which fix types are most effective for different root causes.

7. **Worker-specific analysis**: Some workers may have unique issues - analyze per-worker metrics.

8. **Trend analysis**: Look for patterns over time (e.g., increasing stuck detections, decreasing success rates).

## Automation

You can create cron jobs or scheduled tasks to generate reports automatically:

```bash
#!/bin/bash
# Generate daily report
cd /path/to/mybookshelf2/auto_monitor
python3 generate_daily_report.py > reports/daily_$(date +%Y%m%d).txt
```

This guide should help you effectively analyze auto-monitor logs and generate meaningful reports for understanding system behavior and identifying areas for improvement.





