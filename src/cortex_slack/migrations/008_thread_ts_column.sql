-- The parent a message replies to, lifted out of the payload and indexed.
--
-- The thread sweep asked "does this parent hold fewer replies than Slack
-- claimed" with a correlated subquery on raw->>'thread_ts'. No index can serve
-- a JSONB extraction, so each evaluation index-scanned the parent's whole
-- channel and heap-fetched the payload to filter. Measured in production: a
-- poll pass at 1202 seconds against a 300-second interval, the provably-short
-- arm alone not finishing in 629s, and a planner cost of 2.06e10. LIMIT 40
-- could not prune it, because in steady state fewer than 40 rows match and the
-- executor must exhaust the index to prove it.
--
-- Same argument as 007 lifted reply_count: this is metadata about the message,
-- not the message, so `raw` stays exactly what Slack returned.
ALTER TABLE slack_messages_raw ADD COLUMN IF NOT EXISTS thread_ts TEXT;

UPDATE slack_messages_raw
   SET thread_ts = raw->>'thread_ts'
 WHERE thread_ts IS NULL AND raw->>'thread_ts' IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_slack_raw_thread
    ON slack_messages_raw (channel_id, thread_ts) WHERE thread_ts IS NOT NULL;

-- Versioned name so the drop and the create are each individually a no-op on
-- re-run. apply_schema() re-executes every migration on every service boot, so
-- an unversioned DROP + CREATE IF NOT EXISTS pair can never short-circuit: the
-- drop guarantees the create has work to do, and 007 was rebuilding an index
-- over 1.55M rows on each of three containers' startups, holding ACCESS
-- EXCLUSIVE while it did.
DROP INDEX IF EXISTS idx_slack_threads_to_sweep;
CREATE INDEX IF NOT EXISTS idx_slack_threads_to_sweep_v2
    ON slack_messages_raw (thread_swept_at NULLS FIRST, ts DESC);
