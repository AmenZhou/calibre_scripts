# Migration Guide for 1.7 Million Books

## Quick Start

### 1. Pre-Migration Checks

```bash
cd mybookshelf2

# Verify containers are running
docker ps | grep mybookshelf2

# Test with small batch (optional)
python3 bulk_migrate_calibre.py \
  "/media/haimengzhou/78613a5d-17be-413e-8691-908154970815/calibre library" \
  --worker-id 0 --offset 0 --limit 100 --use-symlinks
```

### 2. Start Parallel Migration

```bash
# Start migration with 4 workers
python3 parallel_migrate.py \
  "/media/haimengzhou/78613a5d-17be-413e-8691-908154970815/calibre library" \
  --workers 4 \
  --use-symlinks \
  --batch-size 10000
```

### 3. Monitor Progress (in separate terminal)

```bash
python3 monitor_migration.py
```

## Expected Timeline

- **4 workers**: ~10 days
- **6 workers**: ~6.7 days (after validating 4 workers)
- **8 workers**: ~5 days (requires monitoring)

## Files Created

- `migration_worker{N}.log` - Individual worker logs
- `migration_progress_worker{N}.json` - Progress tracking per worker
- `migration_errors_worker{N}.log` - Error logs per worker

## Scaling Workers

1. Start with 4 workers
2. Monitor for 24 hours
3. If stable, stop and restart with 6 workers:
   ```bash
   # Stop current migration (Ctrl+C)
   # Restart with more workers
   python3 parallel_migrate.py ... --workers 6
   ```

## Resuming Migration

If migration is interrupted, simply restart with the same command. Progress files prevent duplicate processing.

## Troubleshooting

- **High CPU/Memory**: Reduce worker count
- **Database connection errors**: Reduce workers or increase PostgreSQL max_connections
- **Disk I/O saturation**: Reduce workers
- **Worker stalls**: Check individual log files

