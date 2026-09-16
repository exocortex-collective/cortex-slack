"""Walking a Slack thread.

Shared by the backfill and the poller, because `conversations.history` does not
return thread replies -- it returns only the message that started each thread.
An adapter that reads history and stops there captures the question and none of
the answers, which is where a Slack conversation's meaning usually lives.

This lived in backfill.py and the poller did not call it. Three days of polling
lost 43 of 48 replies before that was noticed: the only ones that survived were
`thread_broadcast`, which Slack mirrors into the channel history. Shared here so
the next adapter inherits the behaviour rather than the omission.
"""

from __future__ import annotations

from typing import Any, Protocol

import psycopg2
from cortex_utils.logging import get_logger

from cortex_slack.ingest import ingest_message

logger = get_logger(__name__)


class RepliesClient(Protocol):
    def replies(self, channel: str, thread_ts: str) -> Any: ...


def starts_a_thread(message: dict[str, Any]) -> bool:
    """True when this message began a thread that has replies.

    `thread_ts == ts` identifies the parent. A reply carries its parent's
    thread_ts, so treating "has a thread_ts" as "starts a thread" would fetch
    the same thread once per reply -- N+1 requests against a limit measured in
    requests per minute.
    """
    thread_ts = message.get("thread_ts")
    if thread_ts is None or thread_ts != message.get("ts"):
        return False
    return int(message.get("reply_count") or 0) > 0


def walk_thread(
    conn: psycopg2.extensions.connection,
    client: RepliesClient,
    channel_id: str,
    thread_ts: str,
    *,
    source: str,
) -> tuple[int, int]:
    """Ingest a thread's replies. Returns (ingested, errors).

    The parent is returned by this endpoint too and re-ingests as UNCHANGED, so
    callers do not filter it. It is not counted: the history walk that reached
    this thread already counted it, and counting it again makes the reported
    total disagree with the row count.

    A thread_broadcast is likewise not counted -- it is returned by BOTH
    conversations.history and conversations.replies.
    """
    ingested = errors = 0
    for reply in client.replies(channel_id, thread_ts):
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT reply")
        try:
            ingest_message(conn, channel_id, reply, source=source, enqueue_parse=False)
        except Exception as exc:  # noqa: BLE001 - one reply must not end the thread
            # ROLLBACK TO SAVEPOINT, not conn.rollback(): ingest_message does not
            # commit, so a plain rollback would discard every message the caller
            # has ingested since its last commit.
            errors += 1
            with conn.cursor() as cur:
                cur.execute("ROLLBACK TO SAVEPOINT reply")
            logger.warning(
                "reply ingest failed",
                channel=channel_id,
                thread_ts=thread_ts,
                ts=reply.get("ts"),
                error=str(exc),
            )
            continue
        with conn.cursor() as cur:
            cur.execute("RELEASE SAVEPOINT reply")

        if reply.get("ts") != thread_ts and reply.get("subtype") != "thread_broadcast":
            ingested += 1
    return ingested, errors
