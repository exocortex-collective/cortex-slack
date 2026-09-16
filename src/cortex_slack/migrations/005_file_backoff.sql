-- When a failed download may be retried.
--
-- An attempt budget without a delay spends every attempt inside the same
-- outage, so a file knocked out by a thirty-second network blip exhausts all
-- five retries against that blip and is then given up on for a cause that has
-- already passed. The budget answers "is this worth retrying"; this column
-- answers "when", and they are different questions.
--
-- NULL means eligible now: either never attempted, or the backoff has elapsed
-- and been cleared.
ALTER TABLE slack_files ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

DROP INDEX IF EXISTS idx_slack_files_fetchable;
CREATE INDEX IF NOT EXISTS idx_slack_files_fetchable
    ON slack_files (next_attempt_at NULLS FIRST, fetched_at)
    WHERE s3_key IS NULL AND fetch_error IS NULL;
