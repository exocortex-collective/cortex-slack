-- Why a file could not be fetched, when the answer is permanent.
--
-- Previously this was recorded by rewriting raw->>'mode' to 'tombstone', which
-- was wrong twice over: raw is the record of what Slack returned and must stay
-- that way, and it conflated "Slack says this is a tombstone" with "we could not
-- download it", which are different facts that a later reader would need to
-- tell apart.
--
-- NULL means still fetchable: either stored (s3_key set) or pending.
ALTER TABLE slack_files ADD COLUMN IF NOT EXISTS fetch_error TEXT;

-- The work-finding predicate: not stored, and not permanently failed. Without
-- the fetch_error half, a 410 Gone is retried on every pass forever -- measured
-- at 40 permanently-dead files out of the first 800 attempts, each of which
-- would have been re-requested indefinitely against a rate-limited API.
CREATE INDEX IF NOT EXISTS idx_slack_files_fetchable
    ON slack_files (fetched_at) WHERE s3_key IS NULL AND fetch_error IS NULL;
