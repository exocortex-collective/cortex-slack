-- Tags on parsed messages.
--
-- An array column rather than a junction table: every tag here is derived from
-- the message itself and rewritten wholesale on each parse, so there is no
-- independent tag lifecycle for a junction table to manage. A GIN index makes
-- the containment queries the default read needs fast.
--
-- When judgment tags arrive -- assigned by a person or a rules engine, with
-- their own provenance and history -- they want a separate table, because those
-- must survive a re-parse and these must not.
ALTER TABLE slack_messages ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_slack_messages_tags ON slack_messages USING GIN (tags);

-- The default read: human messages, newest first. Everything downstream should
-- go through this rather than spelling the exclusion out again, so that "what
-- counts as noise" has one definition.
--
-- 89% of this workspace is automated and one alert channel is 82% of the whole
-- archive, so this view is the difference between a queryable corpus and a
-- firehose.
-- Columns are named, not `m.*`.
--
-- apply_schema() used to re-run every migration on each service boot. It now
-- records what it has applied, but a database from before that still replays
-- every file once -- and `m.*` is expanded at creation time. The next column
-- added to slack_messages would re-expand ahead of channel_name, and CREATE OR
-- REPLACE VIEW may only append columns, so Postgres would raise "cannot change
-- name of view column", the boot transaction would abort, and all three
-- services would fail to start on a migration that had nothing to do with
-- this view.
--
-- LEFT JOIN, not JOIN. 001 states the design intent: there is deliberately no
-- foreign key to slack_channels, because an adapter can reach a message in a
-- channel the channel sweep has not recorded yet. An inner join turns that
-- expected state into messages silently vanishing from the default read. A null
-- channel_name is a visible gap; a missing row is not.
CREATE OR REPLACE VIEW slack_human_messages AS
SELECT m.channel_id, m.ts, m.thread_ts, m.user_id, m.subtype, m.text,
       m.blocks, m.reactions, m.reply_count, m.edited_at, m.posted_at,
       m.parsed_at, m.tags,
       c.name AS channel_name, c.kind AS channel_kind
  FROM slack_messages m
  LEFT JOIN slack_channels c ON c.channel_id = m.channel_id
 WHERE NOT (m.tags && ARRAY['automated', 'system']);
