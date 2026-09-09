-- Slack archive schema.
--
-- Two tiers, as in cortex-granola: ingest writes raw API payloads and nothing
-- else, and a parse worker derives the structured rows. That split is what lets
-- the ingest seam stay identical for the poller, the backfill and a future push
-- adapter -- none of them know how a message is parsed, only how to store one.
--
-- Slack's `ts` is the message identity within a channel and is a string, not a
-- number: "1725379200.001500". Comparing it numerically loses the sub-second
-- suffix that distinguishes two messages posted in the same second, so it stays
-- TEXT everywhere and is only converted for display.

CREATE TABLE IF NOT EXISTS slack_channels (
    channel_id   TEXT PRIMARY KEY,
    name         TEXT,
    kind         TEXT NOT NULL,        -- public | private | im | mpim
    is_archived  BOOLEAN NOT NULL DEFAULT FALSE,
    is_member    BOOLEAN NOT NULL DEFAULT FALSE,
    topic        TEXT,
    purpose      TEXT,
    raw          JSONB NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS slack_users (
    user_id      TEXT PRIMARY KEY,
    handle       TEXT,
    display_name TEXT,
    real_name    TEXT,
    email        TEXT,
    is_bot       BOOLEAN NOT NULL DEFAULT FALSE,
    deleted      BOOLEAN NOT NULL DEFAULT FALSE,
    raw          JSONB NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- What ingest writes. One row per message as Slack returned it.
--
-- No foreign key to slack_channels: a backfill can reach a message in a channel
-- the channel sweep has not recorded yet, and an ingest that fails on ordering
-- is an ingest that drops data for a reason the caller cannot fix. The parse
-- worker resolves the reference later, when both rows exist.
CREATE TABLE IF NOT EXISTS slack_messages_raw (
    channel_id   TEXT NOT NULL,
    ts           TEXT NOT NULL,
    -- Slack reports edits as a nested {"edited": {"ts": ...}}; lifted here so
    -- the ingest can decide "changed or unchanged" without parsing the payload.
    edited_ts    TEXT,
    source       TEXT NOT NULL,        -- backfill | poll | push
    raw          JSONB NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (channel_id, ts)
);

CREATE INDEX IF NOT EXISTS idx_slack_messages_raw_fetched
    ON slack_messages_raw (fetched_at DESC);

-- What the parse worker writes.
CREATE TABLE IF NOT EXISTS slack_messages (
    channel_id   TEXT NOT NULL,
    ts           TEXT NOT NULL,
    thread_ts    TEXT,                 -- NULL for top-level, else the parent ts
    user_id      TEXT,
    subtype      TEXT,
    text         TEXT,
    blocks       JSONB,
    reactions    JSONB,
    reply_count  INTEGER NOT NULL DEFAULT 0,
    edited_at    TIMESTAMPTZ,
    posted_at    TIMESTAMPTZ NOT NULL,
    parsed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (channel_id, ts),
    FOREIGN KEY (channel_id, ts)
        REFERENCES slack_messages_raw (channel_id, ts) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_slack_messages_thread
    ON slack_messages (channel_id, thread_ts) WHERE thread_ts IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_slack_messages_posted
    ON slack_messages (posted_at DESC);
CREATE INDEX IF NOT EXISTS idx_slack_messages_user
    ON slack_messages (user_id, posted_at DESC);

CREATE TABLE IF NOT EXISTS slack_files (
    file_id      TEXT PRIMARY KEY,
    channel_id   TEXT,
    ts           TEXT,
    name         TEXT,
    title        TEXT,
    mimetype     TEXT,
    size_bytes   BIGINT,
    -- NULL until the media worker has stored it; that is the work-finding
    -- predicate, so it must stay nullable rather than defaulting to ''.
    s3_key       TEXT,
    stored_at    TIMESTAMPTZ,
    raw          JSONB NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_slack_files_pending
    ON slack_files (fetched_at) WHERE s3_key IS NULL;

-- One watermark per channel. Slack pagination is per-conversation, so a single
-- global watermark would either re-read every channel or skip the quiet ones.
--
-- last_ts advances only after a clean pass over that channel, for the reason
-- cortex-granola learned the hard way: a watermark that moves on a partial run
-- silently drops whatever the run missed.
CREATE TABLE IF NOT EXISTS slack_sync_state (
    channel_id     TEXT PRIMARY KEY,
    last_ts        TEXT,
    last_synced_at TIMESTAMPTZ,
    last_error     TEXT,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS slack_backfill_jobs (
    id           BIGSERIAL PRIMARY KEY,
    channel_id   TEXT NOT NULL,
    oldest_ts    TEXT,
    latest_ts    TEXT,
    status       TEXT NOT NULL DEFAULT 'pending',  -- pending|running|done|failed
    cursor       TEXT,
    processed    INTEGER NOT NULL DEFAULT 0,
    stored       INTEGER NOT NULL DEFAULT 0,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_slack_backfill_pending
    ON slack_backfill_jobs (created_at) WHERE status = 'pending';
