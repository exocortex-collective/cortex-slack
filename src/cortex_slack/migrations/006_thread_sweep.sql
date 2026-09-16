-- When a thread's replies were last fetched.
--
-- Walking threads only for parents seen in the current poll covers new threads
-- and misses every reply to an older one: a reply does not change its parent's
-- ts, so the parent sinks below the watermark and is never re-read. Measured
-- after the forward-only fix -- two threads at 0 of 6 and 0 of 3, their parents
-- 95 minutes outside the overlap window while a newer parent in the same
-- channel was complete.
--
-- NULL means never swept. Only parents have this set; it stays NULL on replies.
ALTER TABLE slack_messages_raw ADD COLUMN IF NOT EXISTS thread_swept_at TIMESTAMPTZ;

-- Threads worth revisiting: those that started recently enough to still be
-- attracting replies, oldest sweep first.
CREATE INDEX IF NOT EXISTS idx_slack_threads_to_sweep
    ON slack_messages_raw (thread_swept_at NULLS FIRST, ts)
    WHERE (raw->>'reply_count')::int > 0;
