"""Keep the archive current after the historical backfill.

One pass reads every readable conversation from its own watermark forward. The
watermark is per channel because Slack pagination is per conversation: a single
global mark would either re-read every channel on every pass or skip the quiet
ones.

Two rules carried over from the backfill, both learned the hard way:

  - A watermark advances only after a clean pass over that channel. Re-reading is
    cheap and idempotent; advancing past a message that was never stored cannot
    be undone and leaves nothing behind to notice.
  - A failed message is rolled back to a savepoint, not with conn.rollback().
    ingest_message does not commit, so a plain rollback discards every message
    ingested since the last commit and the watermark then moves past all of them.

The poll does not enqueue parse jobs. Nothing does: the parse worker finds work
by query, so a raw row with no parsed row is itself the record that parsing is
outstanding, and the worker's poll interval is the only latency that adds.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import psycopg2
from cortex_utils.logging import get_logger

from cortex_slack.ingest import Outcome, ingest_message
from cortex_slack.slack.api import TS_PATTERN, SlackAuthError, SlackClient, SlackError
from cortex_slack.threads import starts_a_thread, walk_thread

logger = get_logger(__name__)

SOURCE = "poll"

# Channels whose history this account can read at all. A public channel it has
# not joined is readable; anything it cannot see never appeared in the sweep.
_CHANNELS = """
SELECT c.channel_id, c.name, s.last_ts
  FROM slack_channels c
  LEFT JOIN slack_sync_state s ON s.channel_id = c.channel_id
 WHERE NOT c.is_archived
 ORDER BY c.channel_id
"""


# How long a thread keeps attracting replies. Threads older than this are swept
# only when they are demonstrably short, not on a schedule -- a two-month-old
# conversation is not going to gain a reply, and sweeping it spends a
# rate-limited request to confirm that.
THREAD_ACTIVE_WINDOW = "7 days"

# How often an active thread is re-checked, and how many to sweep per pass. The
# cap matters: a full pass is already 315s of the 300s interval, so an unbounded
# sweep would push the poller further behind on every cycle.
THREAD_RESWEEP_AFTER = "6 hours"
THREAD_SWEEP_LIMIT = 40

# Parents whose replies may be missing. Two populations, and the first is the
# one that self-corrects: a thread storing fewer replies than Slack claimed is
# provably incomplete regardless of age. The second is recency -- an active
# thread gains replies that no channel read will ever surface, because a reply
# does not change its parent's ts.
_STALE_THREADS = """
SELECT p.channel_id, p.ts
  FROM slack_messages_raw p
  -- LEFT JOIN: an unparsed parent must still be sweepable. An inner join made
  -- the sweep depend on the parse worker having caught up, which is a different
  -- service's latency.
  LEFT JOIN slack_messages m ON m.channel_id = p.channel_id AND m.ts = p.ts
 WHERE (
         -- Provably incomplete, at ANY age. Slack said there were more replies
         -- than we hold, so re-walking is certain to be worth a request. No
         -- recency gate: a thread short by ten replies is short whether it
         -- started today or in 2019.
         (p.reply_count > 0
          AND (SELECT count(*) FROM slack_messages_raw r
                WHERE r.channel_id = p.channel_id
                  AND r.raw->>'thread_ts' = p.ts
                  AND r.ts <> p.ts) < p.reply_count)
       OR
         -- Or recent enough to still attract a first reply, and due a re-check.
         --
         -- NOT gated on reply_count. A message stored before it had any replies
         -- carries zero, and the count only refreshes when the message is
         -- re-read -- which, outside the poll's overlap window, never happens.
         -- Gating on it made the sweep structurally blind to exactly the case
         -- it exists for.
         --
         -- Human messages only, to keep the pool affordable: 23,626 messages in
         -- the last seven days, of which 600 are human. A reply to a bot message
         -- is still reached once that message is re-read inside the overlap and
         -- its count refreshes.
         (to_timestamp(split_part(p.ts, '.', 1)::bigint) > NOW() - %s::interval
          AND (p.thread_swept_at IS NULL OR p.thread_swept_at < NOW() - %s::interval)
          AND (p.reply_count > 0
               OR m.tags IS NULL
               OR NOT (m.tags && ARRAY['automated', 'system'])))
       )
 ORDER BY p.thread_swept_at NULLS FIRST, p.ts DESC
 LIMIT %s
"""


@dataclass
class PollStats:
    channels: int = 0
    swept_threads: int = 0
    # Replies are their own field, not folded into `stored`. walk_thread counts
    # every reply it walks whatever the outcome, and the sweep re-walks each
    # active thread every six hours -- so adding them to `stored` reported the
    # archive growing on every pass with nothing written. The backfill already
    # keeps them separate for exactly this reason.
    replies: int = 0
    stored: int = 0
    updated: int = 0
    unchanged: int = 0
    errors: int = 0
    failed_channels: list[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return self.errors == 0


def ts_minus_seconds(ts: str, seconds: int) -> str:
    """Move a Slack ts backwards, preserving its sub-second half.

    Never float arithmetic: float("1725379200.001500") is not exact at this
    magnitude and the suffix is what distinguishes two messages in the same
    second. Only the integer part moves.

    Validates first. int(whole) raises ValueError on a non-numeric watermark,
    and ValueError is not a SlackError -- so it escaped poll_once's per-channel
    handler, left main() uncaught, and crash-looped all 323 channels because one
    row was bad. The guard downstream was written for this and sat one line too
    late to catch it.
    """
    if not TS_PATTERN.match(ts):
        raise SlackError("conversations.history", f"bad watermark {ts!r}")
    whole, _, micros = ts.partition(".")
    return f"{max(int(whole) - seconds, 0)}.{micros or '000000'}"


def poll_once(
    conn: psycopg2.extensions.connection,
    client: SlackClient,
    *,
    overlap_seconds: int = 300,
) -> PollStats:
    """Read every readable conversation from its watermark. Never raises SlackError.

    A channel that fails is recorded and the pass continues: one unreadable
    conversation out of 326 must not stop the other 325. SlackAuthError does
    propagate -- a dead session is not a per-channel problem and retrying 325
    more times would only spend the rate budget proving it.
    """
    stats = PollStats()
    with conn.cursor() as cur:
        cur.execute(_CHANNELS)
        channels = cur.fetchall()

    for channel_id, name, last_ts in channels:
        stats.channels += 1
        try:
            _poll_channel(conn, client, channel_id, last_ts, overlap_seconds, stats)
        except SlackAuthError:
            raise
        except SlackError as exc:
            stats.errors += 1
            stats.failed_channels.append(name or channel_id)
            _record_error(conn, channel_id, str(exc))
            logger.warning("channel poll failed", channel=name or channel_id, error=str(exc))

    sweep_threads(conn, client, stats)
    return stats


def sweep_threads(
    conn: psycopg2.extensions.connection, client: SlackClient, stats: PollStats
) -> None:
    """Re-read threads a channel scan can never surface.

    Walking threads for parents seen in the current pass covers new threads and
    nothing else: a reply does not change its parent's ts, so an older parent
    sinks below the watermark and its thread is frozen at whatever was stored
    the last time the parent happened to be read.
    """
    with conn.cursor() as cur:
        cur.execute(
            _STALE_THREADS, (THREAD_ACTIVE_WINDOW, THREAD_RESWEEP_AFTER, THREAD_SWEEP_LIMIT)
        )
        threads = cur.fetchall()
    conn.rollback()

    for channel_id, thread_ts in threads:
        try:
            ingested, failed = walk_thread(conn, client, channel_id, thread_ts, source=SOURCE)
        except SlackAuthError:
            raise
        except SlackError as exc:
            # One unreadable thread must not end the sweep.
            stats.errors += 1
            logger.warning(
                "thread sweep failed", channel=channel_id, thread_ts=thread_ts, error=str(exc)
            )
            conn.rollback()
            continue

        stats.replies += ingested
        stats.errors += failed
        stats.swept_threads += 1
        if failed == 0:
            # thread_swept_at is a watermark in everything but name: it decides
            # which window the next pass scans. Stamping it after a walk whose
            # replies did not all land restarts the six-hour timer over an
            # incomplete thread.
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE slack_messages_raw SET thread_swept_at = NOW() "
                    "WHERE channel_id = %s AND ts = %s",
                    (channel_id, thread_ts),
                )
        conn.commit()


def _poll_channel(
    conn: psycopg2.extensions.connection,
    client: SlackClient,
    channel_id: str,
    last_ts: str | None,
    overlap_seconds: int,
    stats: PollStats,
) -> None:
    # The scan's start, captured before any reading. The watermark advances to
    # the newest message actually seen, never to "now" -- a mark set from the
    # clock covers a window the scan may not have finished reading.
    oldest = ts_minus_seconds(last_ts, overlap_seconds) if last_ts else None

    newest_seen = last_ts
    channel_errors = 0

    for message in client.history(channel_id, oldest=oldest):
        ts = message.get("ts")
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT msg")
        try:
            outcome = ingest_message(
                conn, channel_id, message, source=SOURCE, enqueue_parse=False
            ).outcome
        except Exception as exc:  # noqa: BLE001 - one message must not end the channel
            channel_errors += 1
            with conn.cursor() as cur:
                cur.execute("ROLLBACK TO SAVEPOINT msg")
            logger.warning("message ingest failed", channel=channel_id, ts=ts, error=str(exc))
            continue
        with conn.cursor() as cur:
            cur.execute("RELEASE SAVEPOINT msg")

        if outcome is Outcome.STORED:
            stats.stored += 1
        elif outcome is Outcome.UPDATED:
            stats.updated += 1
        else:
            stats.unchanged += 1

        # conversations.history returns thread parents only. The backfill walks
        # threads and the poller did not, so three days of polling stored 5 of
        # 48 replies -- and only because thread_broadcast is mirrored into the
        # channel. A reply does not change its parent's ts, so nothing would
        # ever have re-read them.
        if starts_a_thread(message):
            ingested, failed = walk_thread(
                conn, client, channel_id, str(message["ts"]), source=SOURCE
            )
            stats.replies += ingested
            channel_errors += failed

        if isinstance(ts, str) and (newest_seen is None or ts > newest_seen):
            # String comparison is correct here: every Slack ts is exactly ten
            # digits, a dot, then six, so lexicographic and numeric order agree
            # until the year 2286.
            newest_seen = ts

    stats.errors += channel_errors
    if channel_errors == 0:
        _advance(conn, channel_id, newest_seen)
    conn.commit()


def _advance(conn: psycopg2.extensions.connection, channel_id: str, last_ts: str | None) -> None:
    """Record a clean pass, moving the watermark if there is anywhere to move it.

    "I polled this cleanly" and "the watermark moved" are different facts, and
    bundling them hid the first. A quiet channel reaches here with last_ts
    unchanged -- the overwhelmingly common case -- and a WHERE guarding the whole
    row meant last_synced_at was never written and a stale last_error was never
    cleared. Measured after three days of clean five-minute passes: 308 of 323
    channels still carried the backfill's timestamp and were indistinguishable
    from never having been polled.

    GREATEST keeps the never-move-backwards property -- a deleted newest message
    would otherwise rewind the mark and re-ingest history forever -- and is
    NULL-safe in Postgres, so a first pass over an empty channel still records
    itself.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO slack_sync_state (channel_id, last_ts, last_synced_at, last_error)
            VALUES (%s, %s, NOW(), NULL)
            ON CONFLICT (channel_id) DO UPDATE
               SET last_ts = GREATEST(slack_sync_state.last_ts, EXCLUDED.last_ts),
                   last_synced_at = EXCLUDED.last_synced_at,
                   last_error = NULL,
                   updated_at = NOW()
            """,
            (channel_id, last_ts),
        )


def _record_error(conn: psycopg2.extensions.connection, channel_id: str, error: str) -> None:
    """Record the failure without touching last_ts."""
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO slack_sync_state (channel_id, last_error, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (channel_id) DO UPDATE
               SET last_error = EXCLUDED.last_error, updated_at = NOW()
            """,
            (channel_id, error[:2000]),
        )
    conn.commit()
