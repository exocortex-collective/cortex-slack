-- The parent's reply count, refreshed on every capture.
--
-- It lived only inside the raw payload, and ingest_message deliberately refuses
-- to rewrite raw unless edited_ts changes -- which a reply never does. So a
-- parent's count was frozen at first capture: a message stored before it had any
-- replies claimed zero forever, and the thread sweep, which gated candidates on
-- reply_count > 0, could never see it. Measured on the live archive: zero
-- parents where stored < claimed, meaning the sweep's self-correcting arm had
-- never been able to fire at all.
--
-- This is metadata ABOUT the message rather than the message, so refreshing it
-- does not violate the archive's promise that `raw` is what Slack returned --
-- raw still is.
ALTER TABLE slack_messages_raw ADD COLUMN IF NOT EXISTS reply_count INTEGER NOT NULL DEFAULT 0;

-- Backfill from the payloads already stored, so the sweep has something to work
-- with immediately rather than waiting for each parent to be re-read.
UPDATE slack_messages_raw
   SET reply_count = coalesce((raw->>'reply_count')::int, 0)
 WHERE reply_count = 0 AND (raw->>'reply_count')::int > 0;

DROP INDEX IF EXISTS idx_slack_threads_to_sweep;
CREATE INDEX IF NOT EXISTS idx_slack_threads_to_sweep
    ON slack_messages_raw (thread_swept_at NULLS FIRST, ts DESC);
