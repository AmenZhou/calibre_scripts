# Cleanup Orphaned Ebooks Guide

## What are Orphaned Ebooks?

Orphaned ebooks are ebook records in the database that have **no source files**. These are created when:
- Uploads fail after creating ebook metadata but before creating source file records
- Duplicate upload attempts create ebook records but are rejected
- Database inconsistencies from previous migration attempts

## Current Status

- **Total ebooks in database**: 154,110
- **Orphaned ebooks (no files)**: 21,645 (~14%)
- **Working ebooks (with files)**: 132,465

## Should You Remove Them?

**YES, but ONLY at the END of migration:**

### Benefits:
1. ✅ Clean database - removes incomplete/failed upload records
2. ✅ Accurate counts - ebook count matches actual usable books
3. ✅ Better UX - users won't see books they can't access/download
4. ✅ Database efficiency - smaller database, faster queries
5. ✅ Prevents confusion - no "ghost" books in the catalog

### When to Clean Up:
- ✅ **AFTER** migration is complete
- ✅ **AFTER** verifying all workers have finished
- ✅ **AFTER** backing up the database (just in case)
- ❌ **NOT** during active migration (might remove records that are being processed)

## Usage

### Step 1: Dry-Run (See What Would Be Deleted)

```bash
cd /home/haimengzhou/calibre_automation_scripts/mybookshelf2

# See what would be deleted (safe, no changes)
python3 cleanup_orphaned_ebooks.py --dry-run

# Save list to file before deleting (recommended)
python3 cleanup_orphaned_ebooks.py --dry-run --save-list orphaned_ebooks_list.txt
```

### Step 2: Backup Database (Recommended)

```bash
# Backup database before cleanup
docker exec mybookshelf2_db pg_dump -U ebooks ebooks > backup_before_cleanup_$(date +%Y%m%d_%H%M%S).sql
```

### Step 3: Delete Orphaned Records

```bash
# Actually delete orphaned ebooks (requires confirmation)
python3 cleanup_orphaned_ebooks.py --delete

# You'll be prompted to type 'DELETE' to confirm
```

### Step 4: Verify

After deletion, the script will automatically verify that orphaned records are removed.

## What Gets Deleted?

The script removes:
- Orphaned ebook records (ebooks with no source files)
- Related records (ebook_authors, ebook_genres, ebook_ratings, bookshelf_items)

**It does NOT delete:**
- Ebooks that have source files (working books)
- Source files themselves
- Any other data

## Safety Features

1. **Dry-run mode by default** - Shows what would be deleted without making changes
2. **Confirmation required** - Must type 'DELETE' to actually delete
3. **Batch processing** - Deletes in batches of 1000 to avoid memory issues
4. **Verification** - Automatically verifies deletion was successful
5. **List saving** - Can save list of IDs before deleting for audit trail

## Expected Results

After cleanup:
- **Before**: 154,110 total ebooks (21,645 orphaned)
- **After**: ~132,465 ebooks (all with source files)
- **Reduction**: ~14% smaller database
- **Accuracy**: Ebook count matches actual usable books

## Troubleshooting

### Script says "No orphaned ebooks found"
- This is good! Your database is clean.
- Or the query might need adjustment if database structure changed

### Error during deletion
- Check database connection: `docker ps | grep mybookshelf2`
- Check container logs: `docker logs mybookshelf2_db`
- Restore from backup if needed

### Want to restore deleted records?
- Use the backup SQL file created in Step 2
- Restore: `docker exec -i mybookshelf2_db psql -U ebooks ebooks < backup_file.sql`

## Example Output

```
================================================================================
  MyBookshelf2 Orphaned Ebook Cleanup
================================================================================
Mode: DRY-RUN (no changes will be made)
Container: mybookshelf2_app

Scanning database for orphaned ebooks...

Found 21,645 orphaned ebooks (ebooks without source files)

Sample orphaned ebooks (first 10):
  ID 33561: L'Ora Dei Grandi Vermi...
  ID 33441: The Twisted Tale of Faerywood Falls...
  ... and 21,635 more

================================================================================
DRY-RUN: No changes made.
To actually delete these 21,645 orphaned ebooks, run:
  python3 cleanup_orphaned_ebooks.py --delete
================================================================================
```

## Summary

**Recommendation**: Run cleanup **after migration completes** to:
- Clean up failed upload records
- Get accurate ebook counts
- Improve database performance
- Better user experience

The script is safe, reversible (with backup), and provides clear feedback on what will be deleted.


