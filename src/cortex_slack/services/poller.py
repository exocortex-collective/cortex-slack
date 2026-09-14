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


@dataclass
class PollStats:
    channels: int = 0
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
    """
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
    return stats


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
    if oldest is not None and not TS_PATTERN.match(oldest):
        # A malformed watermark would be silently ignored by the API, which
        # returns the whole channel rather than an error.
        raise SlackError("conversations.history", f"bad watermark {oldest!r}")

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

        if isinstance(ts, str) and (newest_seen is None or ts > newest_seen):
            # String comparison is correct here: every Slack ts is exactly ten
            # digits, a dot, then six, so lexicographic and numeric order agree
            # until the year 2286.
            newest_seen = ts

    stats.errors += channel_errors
    if channel_errors == 0 and newest_seen is not None:
        _advance(conn, channel_id, newest_seen)
    conn.commit()


def _advance(conn: psycopg2.extensions.connection, channel_id: str, last_ts: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO slack_sync_state (channel_id, last_ts, last_synced_at, last_error)
            VALUES (%s, %s, NOW(), NULL)
            ON CONFLICT (channel_id) DO UPDATE
               SET last_ts = EXCLUDED.last_ts,
                   last_synced_at = EXCLUDED.last_synced_at,
                   last_error = NULL,
                   updated_at = NOW()
             -- Never move the mark backwards. A channel read with an overlap
             -- window can return an older newest-message than the stored one if
             -- the newest was deleted, and rewinding would re-ingest history
             -- every pass forever.
             WHERE slack_sync_state.last_ts IS NULL
                OR EXCLUDED.last_ts > slack_sync_state.last_ts
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
