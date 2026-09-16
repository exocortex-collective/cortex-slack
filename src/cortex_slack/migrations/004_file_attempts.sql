-- Attempt tracking for file downloads.
--
-- Without it, a failure classed "transient" is retried forever. Observed: 16
-- files answering HTTP 400 accumulated 867 download attempts across restarts,
-- and because the work query ordered by fetched_at they sat permanently at the
-- head of the queue. A bound is what makes "retryable" mean something.
ALTER TABLE slack_files ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE slack_files ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ;

-- Ordered so untried files are always reached before retries, which is what
-- stops a handful of persistent failures starving everything behind them.
DROP INDEX IF EXISTS idx_slack_files_fetchable;
CREATE INDEX IF NOT EXISTS idx_slack_files_fetchable
    ON slack_files (last_attempt_at NULLS FIRST, fetched_at)
    WHERE s3_key IS NULL AND fetch_error IS NULL;
