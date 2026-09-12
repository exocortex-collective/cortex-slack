"""Historical backfill: one job per conversation, resumable at page granularity.

Resumability is the load-bearing property, not a refinement. This runs on a
laptop whose container runtime suspends when the lid closes -- measured on
cortex-granola, where a 15-minute poll interval stretched to 67 minutes of wall
clock overnight because the VM's clock stops with the host. A backfill that had
to start over on every interruption would never finish.

So the cursor is committed with the messages it produced, in the same
transaction. After any stop -- sleep, restart, a dead token -- the next run
resumes from the last page that actually landed, and re-running a page is
harmless because ingest is idempotent.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg2
from cortex_utils.logging import get_logger

from cortex_slack.ingest import Outcome, ingest_message
from cortex_slack.slack.api import SlackAuthError, SlackClient, SlackError

logger = get_logger(__name__)

SOURCE = "backfill"

# The backfill does not queue parse jobs. At this workspace's scale that would
# add ~600k rows to the shared queue the email pipeline claims from, which
# already holds 336k and is unpartitioned. The parse worker finds backfilled
# messages by query instead -- a raw row with no parsed row IS the record that
# work is outstanding, so the queue adds nothing here but contention.
ENQUEUE_PARSE = False


@dataclass(frozen=True)
class ChannelResult:
    channel_id: str
    stored: int = 0
    updated: int = 0
    unchanged: int = 0
    threads: int = 0
    errors: int = 0

    @property
    def seen(self) -> int:
        return self.stored + self.updated + self.unchanged

    @property
    def clean(self) -> bool:
        return self.errors == 0


def seed_jobs(conn: psycopg2.extensions.connection, channels: list[dict[str, Any]]) -> int:
    """Create one pending job per conversation. Idempotent.

    Ordered so the corpus that matters lands first: conversations the account is
    actually in, before public channels it merely can read. A weekend run that
    gets interrupted should have the DMs and joined channels done, not a
    random prefix.
    """
    created = 0
    with conn.cursor() as cur:
        for channel in _ordered(channels):
            cur.execute(
                """
                INSERT INTO slack_backfill_jobs (channel_id)
                SELECT %s
                 WHERE NOT EXISTS (
                       SELECT 1 FROM slack_backfill_jobs
                        WHERE channel_id = %s AND status IN ('pending', 'running')
                 )
                """,
                (channel["id"], channel["id"]),
            )
            created += cur.rowcount
    conn.commit()
    return created


def _ordered(channels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def rank(channel: dict[str, Any]) -> tuple[int, str]:
        joined = bool(channel.get("is_member") or channel.get("is_im") or channel.get("is_mpim"))
        return (0 if joined else 1, str(channel.get("id", "")))

    return sorted(channels, key=rank)


def run_next_job(conn: psycopg2.extensions.connection, client: SlackClient) -> ChannelResult | None:
    """Claim one pending job and work it to completion. None when none are left.

    One conversation per call rather than a loop, so the caller decides how long
    to keep going and a stop between conversations costs nothing.
    """
    job = _claim(conn)
    if job is None:
        return None
    job_id, channel_id, cursor = job

    logger.info("backfill starting", channel=channel_id, job=job_id, resuming=cursor is not None)
    try:
        result = _walk_channel(conn, client, job_id, channel_id, cursor)
    except SlackAuthError as exc:
        # The credential is dead, not the job. Release it so the next run with a
        # fresh token picks it up where this one stopped, rather than marking a
        # perfectly good conversation failed.
        _release(conn, job_id, str(exc))
        raise
    except SlackError as exc:
        _fail(conn, job_id, str(exc))
        logger.error("backfill failed", channel=channel_id, job=job_id, error=str(exc))
        return ChannelResult(channel_id, errors=1)

    _finish(conn, job_id, channel_id, result)
    logger.info(
        "backfill finished",
        channel=channel_id,
        job=job_id,
        seen=result.seen,
        stored=result.stored,
        threads=result.threads,
        errors=result.errors,
    )
    return result


def _walk_channel(
    conn: psycopg2.extensions.connection,
    client: SlackClient,
    job_id: int,
    channel_id: str,
    cursor: str | None,
) -> ChannelResult:
    stored = updated = unchanged = threads = errors = 0

    for messages, next_cursor in client.history_pages(channel_id, cursor=cursor):
        for message in messages:
            try:
                outcome = ingest_message(
                    conn, channel_id, message, source=SOURCE, enqueue_parse=ENQUEUE_PARSE
                ).outcome
            except Exception as exc:  # noqa: BLE001 - one bad message must not end the channel
                # Counted, not swallowed: errors keep the job from being marked
                # done, so the watermark never advances past a conversation we
                # only partly read. That is the granola bug as an invariant.
                errors += 1
                logger.warning(
                    "message ingest failed",
                    channel=channel_id,
                    ts=message.get("ts"),
                    error=str(exc),
                )
                conn.rollback()
                continue

            if outcome is Outcome.STORED:
                stored += 1
            elif outcome is Outcome.UPDATED:
                updated += 1
            else:
                unchanged += 1

            if _has_thread(message):
                threads += 1
                errors += _walk_thread(conn, client, channel_id, str(message["ts"]))

        # The page's messages and the cursor that moves past it commit together.
        # Storing the cursor first would skip this page's messages after a crash;
        # storing it later would re-read them, which is merely wasteful. They go
        # in one transaction so neither happens.
        _record_progress(conn, job_id, stored, next_cursor)
        conn.commit()

    return ChannelResult(channel_id, stored, updated, unchanged, threads, errors)


def _walk_thread(
    conn: psycopg2.extensions.connection, client: SlackClient, channel_id: str, thread_ts: str
) -> int:
    """Ingest a thread's replies. Returns the error count.

    conversations.history does not return replies, so a channel scan alone
    captures only the parents -- and a Slack conversation's meaning usually
    lives in the thread rather than the message that started it. The parent
    comes back here too and re-ingests as UNCHANGED, so it is not filtered.
    """
    errors = 0
    for reply in client.replies(channel_id, thread_ts):
        try:
            ingest_message(conn, channel_id, reply, source=SOURCE, enqueue_parse=ENQUEUE_PARSE)
        except Exception as exc:  # noqa: BLE001
            errors += 1
            logger.warning(
                "reply ingest failed",
                channel=channel_id,
                thread_ts=thread_ts,
                ts=reply.get("ts"),
                error=str(exc),
            )
            conn.rollback()
    return errors


def _has_thread(message: dict[str, Any]) -> bool:
    """True when this message starts a thread that has replies.

    `thread_ts == ts` identifies the parent; a reply carries the parent's
    thread_ts and would send us round the same thread once per reply.
    """
    thread_ts = message.get("thread_ts")
    if thread_ts is None or thread_ts != message.get("ts"):
        return False
    return int(message.get("reply_count") or 0) > 0


def _claim(conn: psycopg2.extensions.connection) -> tuple[int, str, str | None] | None:
    """Take the oldest pending job.

    SKIP LOCKED so a second instance takes a different conversation rather than
    blocking, and so a crashed instance's row is not held forever.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE slack_backfill_jobs
               SET status = 'running', updated_at = NOW()
             WHERE id = (
                   SELECT id FROM slack_backfill_jobs
                    WHERE status = 'pending'
                    ORDER BY created_at, id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
             )
            RETURNING id, channel_id, cursor
            """
        )
        row = cur.fetchone()
    conn.commit()
    if row is None:
        return None
    return int(row[0]), str(row[1]), row[2]


def _record_progress(
    conn: psycopg2.extensions.connection, job_id: int, stored: int, cursor: str | None
) -> None:
    """Save the resume point. Does NOT commit -- the caller commits it with the
    messages the cursor moves past, so the two can never disagree."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE slack_backfill_jobs SET stored = %s, cursor = %s, updated_at = NOW() "
            "WHERE id = %s",
            (stored, cursor, job_id),
        )


def _finish(
    conn: psycopg2.extensions.connection,
    job_id: int,
    channel_id: str,
    result: ChannelResult,
) -> None:
    """Mark the job done and advance the channel's watermark -- only if clean.

    A run with errors leaves the job failed and the watermark untouched, so the
    next attempt re-reads the whole conversation. Re-reading is cheap and
    idempotent; a watermark that advanced past a message we never stored is
    permanent silent loss.
    """
    status = "done" if result.clean else "failed"
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE slack_backfill_jobs SET status = %s, processed = %s, stored = %s, "
            "updated_at = NOW() WHERE id = %s",
            (status, result.seen, result.stored, job_id),
        )
        if result.clean:
            cur.execute(
                """
                INSERT INTO slack_sync_state (channel_id, last_ts, last_synced_at, last_error)
                SELECT %s, MAX(ts), NOW(), NULL
                  FROM slack_messages_raw WHERE channel_id = %s
                ON CONFLICT (channel_id) DO UPDATE
                   SET last_ts = EXCLUDED.last_ts,
                       last_synced_at = EXCLUDED.last_synced_at,
                       last_error = NULL,
                       updated_at = NOW()
                """,
                (channel_id, channel_id),
            )
    conn.commit()


def _fail(conn: psycopg2.extensions.connection, job_id: int, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE slack_backfill_jobs SET status = 'failed', error = %s, updated_at = NOW() "
            "WHERE id = %s",
            (error[:2000], job_id),
        )
    conn.commit()


def _release(conn: psycopg2.extensions.connection, job_id: int, error: str) -> None:
    """Put the job back as pending, keeping whatever progress it made."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE slack_backfill_jobs SET status = 'pending', error = %s, updated_at = NOW() "
            "WHERE id = %s",
            (error[:2000], job_id),
        )
    conn.commit()
