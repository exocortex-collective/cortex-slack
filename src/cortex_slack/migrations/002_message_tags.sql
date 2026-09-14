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
CREATE OR REPLACE VIEW slack_human_messages AS
SELECT m.*, c.name AS channel_name, c.kind AS channel_kind
  FROM slack_messages m
  JOIN slack_channels c ON c.channel_id = m.channel_id
 WHERE NOT (m.tags && ARRAY['automated', 'system']);
