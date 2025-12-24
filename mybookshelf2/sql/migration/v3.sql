-- Migration v3: Add index on ebook.created column for performance optimization
-- This index significantly improves query performance when ordering ebooks by creation date
-- Created: 2025-12-24
-- Purpose: Optimize main page loading by adding index on ebook.created DESC
--
-- Performance Impact:
-- - With 866,850+ ebooks, queries ordering by created DESC were very slow
-- - This index reduces query time from seconds to milliseconds
-- - Improves main page load time significantly

-- Create index on ebook.created column (DESC for descending order queries)
-- Using IF NOT EXISTS to make migration idempotent (safe to run multiple times)
CREATE INDEX IF NOT EXISTS ix_ebook_created ON ebook USING btree (created DESC);

