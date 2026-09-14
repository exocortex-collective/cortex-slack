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

# How long a 'running' job may go untouched before another run may take it.
# _record_progress touches updated_at on every page, so a live job is never
# stolen; only one abandoned by a crash or a stop signal is.
STALE_LEASE = "30 minutes"

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
    replies: int = 0
    errors: int = 0

    @property
    def seen(self) -> int:
        """Every message this walk ingested, replies included.

        Replies were originally left out, so the run log undercounted: a
        completed backfill reported 1,513,696 against 1,545,184 rows actually
        stored. An operator reconciling the two has no way to tell a counting
        gap from 31,488 lost messages, which is the whole reason the number is
        printed.
        """
        return self.stored + self.updated + self.unchanged + self.replies

    @property
    def clean(self) -> bool:
        return self.errors == 0


def seed_jobs(
    conn: psycopg2.extensions.connection,
    channels: list[dict[str, Any]],
    *,
    include_done: bool = False,
) -> int:
    """Create one pending job per conversation. Idempotent across restarts.

    Skips channels that already have a job in ANY terminal or active state,
    `done` included. Only checking pending/running looks idempotent and is not:
    this service is built to be stopped and restarted, and on each restart every
    already-finished conversation would be queued again, so a run that is
    interrupted often enough never converges. Observed doing exactly that --
    three completed channels were re-queued on the second start.

    `include_done=True` re-queues finished conversations deliberately, for a
    full re-scan. Re-reading is idempotent, so it costs requests and nothing
    else.

    Ordered so the corpus that matters lands first: conversations the account is
    actually in, before public channels it merely can read. An interrupted run
    should have the DMs and joined channels done, not a random prefix.
    """
    states = ("pending", "running") if include_done else ("pending", "running", "done")
    created = 0
    with conn.cursor() as cur:
        for channel in _ordered(channels):
            cur.execute(
                """
                INSERT INTO slack_backfill_jobs (channel_id)
                SELECT %s
                 WHERE NOT EXISTS (
                       SELECT 1 FROM slack_backfill_jobs
                        WHERE channel_id = %s AND status = ANY(%s)
                 )
                """,
                (channel["id"], channel["id"], list(states)),
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
    except BaseException as exc:
        # Includes KeyboardInterrupt and SystemExit, which is how this service
        # normally stops. Without this the job stays 'running' and, since
        # seed_jobs skips 'running', the channel is never worked again while the
        # next run reports "backfill complete". The stale-lease reclaim in
        # _claim is the backstop; this makes a deliberate stop cost nothing
        # instead of thirty minutes.
        _release(conn, job_id, f"interrupted: {exc!r}")
        raise

    _finish(conn, job_id, channel_id, result)
    logger.info(
        "backfill finished",
        channel=channel_id,
        job=job_id,
        seen=result.seen,
        stored=result.stored,
        threads=result.threads,
        replies=result.replies,
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
    stored = updated = unchanged = threads = replies = errors = 0

    for messages, next_cursor in client.history_pages(channel_id, cursor=cursor):
        errors_before_page = errors
        for message in messages:
            with conn.cursor() as cur:
                cur.execute("SAVEPOINT msg")
            try:
                outcome = ingest_message(
                    conn, channel_id, message, source=SOURCE, enqueue_parse=ENQUEUE_PARSE
                ).outcome
            except Exception as exc:  # noqa: BLE001 - one bad message must not end the channel
                # ROLLBACK TO SAVEPOINT, not conn.rollback(). ingest_message does
                # not commit, so the open transaction holds every message of this
                # page plus the replies walked from them -- a plain rollback threw
                # all of them away while the counters kept claiming they were
                # stored, and the cursor below then committed past them.
                errors += 1
                with conn.cursor() as cur:
                    cur.execute("ROLLBACK TO SAVEPOINT msg")
                logger.warning(
                    "message ingest failed",
                    channel=channel_id,
                    ts=message.get("ts"),
                    error=str(exc),
                )
                continue
            with conn.cursor() as cur:
                cur.execute("RELEASE SAVEPOINT msg")

            if outcome is Outcome.STORED:
                stored += 1
            elif outcome is Outcome.UPDATED:
                updated += 1
            else:
                unchanged += 1

            if _has_thread(message):
                threads += 1
                ingested, failed = _walk_thread(conn, client, channel_id, str(message["ts"]))
                replies += ingested
                errors += failed

        # The page's messages and the cursor that moves past it commit together.
        # Storing the cursor first would skip this page's messages after a crash;
        # storing it later would re-read them, which is merely wasteful.
        #
        # A page that errored does NOT advance the cursor. Re-reading it costs
        # requests; moving past a message that is not in the database cannot be
        # undone, and a later _release keeps the cursor while discarding the
        # in-memory error count -- so the next run would see a clean pass and
        # advance the watermark over the gap.
        clean_page = errors == errors_before_page
        _record_progress(conn, job_id, stored, next_cursor if clean_page else cursor)
        conn.commit()
        if clean_page:
            cursor = next_cursor

    return ChannelResult(channel_id, stored, updated, unchanged, threads, replies, errors)


def _walk_thread(
    conn: psycopg2.extensions.connection, client: SlackClient, channel_id: str, thread_ts: str
) -> tuple[int, int]:
    """Ingest a thread's replies. Returns (ingested, errors).

    conversations.history does not return replies, so a channel scan alone
    captures only the parents -- and a Slack conversation's meaning usually
    lives in the thread rather than the message that started it. The parent
    comes back here too and re-ingests as UNCHANGED, so it is not filtered.
    """
    ingested = errors = 0
    for reply in client.replies(channel_id, thread_ts):
        try:
            ingest_message(conn, channel_id, reply, source=SOURCE, enqueue_parse=ENQUEUE_PARSE)
            # The parent comes back in this response and was already counted by
            # the history walk that got us here. Counting it again turns the
            # undercount this fix was for into an overcount, which is no more
            # reconcilable against the row count than the original was.
            # A thread_broadcast is returned by conversations.history AND by
            # conversations.replies, and its ts differs from thread_ts, so
            # counting it here counts it twice. Measured: exactly 4,319 of them,
            # which is precisely how far the total over-reported.
            if reply.get("ts") != thread_ts and reply.get("subtype") != "thread_broadcast":
                ingested += 1
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
    return ingested, errors


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
                    -- Reclaim a stale lease as well as taking new work. Only
                    -- SlackAuthError and SlackError hand a job back; every other
                    -- exit -- SIGTERM, the laptop suspending, a psycopg2 error on
                    -- commit -- left it 'running' forever, and since seed_jobs
                    -- skips 'running' the channel was never worked again. The run
                    -- then logged "backfill complete" over a silently short
                    -- archive. updated_at was already maintained for exactly this.
                    WHERE status = 'pending'
                       OR (status = 'running' AND updated_at < NOW() - %s::interval)
                    ORDER BY created_at, id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
             )
            RETURNING id, channel_id, cursor
            """,
            (STALE_LEASE,),
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
