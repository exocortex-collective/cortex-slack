"""The single ingest boundary every arrival mechanism goes through.

The same shape cortex-granola uses, which it took from postmark's
`store_raw_message()`. Four adapters are planned, in this order:

    backfill    services/sync.py     -- phase 1, bounded historical scan with
                                        stealth tokens
    poller      services/sync.py     -- phase 1, primary steady-state adapter
    socket      services/socket.py   -- phase 2, Slack Socket Mode; an outbound
                                        WebSocket, so this host still never
                                        accepts inbound traffic
    events      services/events.py   -- phase 3, only if a public endpoint ever
                                        exists

All of them call `ingest_message()`. An adapter decides only WHICH messages to
ingest and WHEN.

This module deliberately does no logging. Every function returns an
IngestResult naming the message and what happened to it, and the adapter holds
the context worth logging -- which channel, which poll, how far through the
window.

Slack differs from Granola in one way that shapes everything here: a message is
identified by (channel_id, ts), not by a single id, and `ts` is a string whose
sub-second suffix is significant. It is never parsed into a float for identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import psycopg2
from cortex_utils.queue import enqueue
from psycopg2.extras import Json

PARSE_QUEUE = "slack_parse"


class Outcome(StrEnum):
    STORED = "stored"  # first time we have seen this message
    UPDATED = "updated"  # the message was edited since we stored it
    UNCHANGED = "unchanged"  # already current; no work enqueued


@dataclass(frozen=True)
class IngestResult:
    """What one ingest did, and the job it enqueued if it enqueued one.

    UNCHANGED never carries a job_id: nothing was written, so nothing was
    enqueued.

    The converse is deliberately NOT asserted. enqueue(dedup_key=...) returns
    None when an identical job is already pending, which is success -- "the work
    is already covered" -- so an UPDATED message whose parse job is still queued
    from the previous edit lands here legitimately with job_id None.
    """

    channel_id: str
    ts: str
    outcome: Outcome
    job_id: int | None = None

    def __post_init__(self) -> None:
        if self.outcome is Outcome.UNCHANGED and self.job_id is not None:
            raise ValueError(f"UNCHANGED must not carry a job_id: {self.channel_id}/{self.ts}")


def ingest_message(
    conn: psycopg2.extensions.connection,
    channel_id: str,
    message: dict[str, Any],
    *,
    source: str,
    priority: int = 0,
) -> IngestResult:
    """Store one raw message and enqueue parsing. Idempotent.

    Takes the message rather than fetching it: Slack's history endpoints return
    full message objects in the list response, so unlike Granola there is no
    second fetch to skip. `source` records which adapter it arrived through --
    the one thing downstream might legitimately want to know, and the only thing
    that varies by arrival.

    Does not commit. The caller owns the transaction so the raw row and the
    parse job land together or not at all; a committed row with no job is a
    message that never gets parsed and that nothing will retry, because the next
    poll sees it as unchanged.
    """
    ts = message.get("ts")
    if not ts:
        # Every message Slack returns carries ts; its absence means the response
        # shape changed under us. Failing here beats storing a row with no
        # identity, which no watermark can advance past and no parse can find.
        raise ValueError(f"message in {channel_id} has no ts")

    edited_ts = _edited_ts(message)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO slack_messages_raw (channel_id, ts, edited_ts, source, raw)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (channel_id, ts) DO UPDATE
               SET raw = EXCLUDED.raw,
                   edited_ts = EXCLUDED.edited_ts,
                   source = EXCLUDED.source,
                   fetched_at = NOW()
             WHERE slack_messages_raw.edited_ts IS DISTINCT FROM EXCLUDED.edited_ts
            RETURNING (xmax = 0) AS inserted
            """,
            (channel_id, ts, edited_ts, source, Json(message)),
        )
        row = cur.fetchone()

    if row is None:
        # The WHERE on DO UPDATE suppressed the write: same edit state. Nothing
        # to parse.
        return IngestResult(channel_id, ts, Outcome.UNCHANGED)

    outcome = Outcome.STORED if row[0] else Outcome.UPDATED
    job_id = enqueue(
        conn,
        PARSE_QUEUE,
        {"channel_id": channel_id, "ts": ts, "message_key": message_key(channel_id, ts)},
        priority=priority,
        # enqueue's dedup_key names one payload field, so the identity is
        # carried as a composite. Deduping on ts alone would collapse two
        # messages posted in different channels in the same millisecond.
        dedup_key="message_key",
        commit=False,
    )
    return IngestResult(channel_id, ts, outcome, job_id)


def message_key(channel_id: str, ts: str) -> str:
    """The dedup identity for a parse job.

    Slack ids contain no "/", so this is unambiguous without escaping. Kept as a
    function because the parse worker and any future adapter must produce the
    same string; two spellings of this would silently stop deduplicating.
    """
    return f"{channel_id}/{ts}"


def _edited_ts(message: dict[str, Any]) -> str | None:
    """Slack reports an edit as a nested {"edited": {"ts": ..., "user": ...}}.

    Lifted to a column so the ingest can decide changed-or-unchanged in the
    INSERT itself. A message that has never been edited has no `edited` key at
    all, which is why this is None rather than the message's own ts -- using the
    ts would make every unedited message look edited exactly once.
    """
    edited = message.get("edited")
    if not isinstance(edited, dict):
        return None
    ts = edited.get("ts")
    return ts if isinstance(ts, str) else None
