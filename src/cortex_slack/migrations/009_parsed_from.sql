-- Which raw version a parsed row came from, so a rewrite is always re-parsed.
--
-- The work predicate was "parsed_at < fetched_at": one clock, but NOW() is
-- pinned at transaction start, and the poller holds one transaction across a
-- whole channel. A raw row rewritten late in that transaction carries a
-- fetched_at from its start. If the parse worker parsed the old version in
-- between, its parsed_at is later than that fetched_at, the rewrite commits,
-- and the predicate says "current" forever -- reproduced: raw text v2, parsed
-- text v1, and nothing would ever revisit it.
--
-- So the parser copies the fetched_at it read into parsed_from, and the
-- predicate asks whether the raw row still carries that value. A rewrite in
-- any other transaction changes fetched_at, and equality does not care which
-- of two timestamps is larger.
ALTER TABLE slack_messages ADD COLUMN IF NOT EXISTS parsed_from TIMESTAMPTZ;

-- Rows the old predicate called current adopt the version they hold now. Rows
-- it already owed a re-parse stay NULL and are picked up by the worker. Only
-- ever a full scan once: the partial index below serves the re-run.
UPDATE slack_messages m
   SET parsed_from = r.fetched_at
  FROM slack_messages_raw r
 WHERE r.channel_id = m.channel_id AND r.ts = m.ts
   AND m.parsed_from IS NULL
   AND m.parsed_at >= r.fetched_at;

CREATE INDEX IF NOT EXISTS idx_slack_messages_unversioned
    ON slack_messages (channel_id, ts) WHERE parsed_from IS NULL;
