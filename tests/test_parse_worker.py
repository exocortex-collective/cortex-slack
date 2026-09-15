"""Tests for the parse worker's work-finding.

The property that matters is that a pass costs one scan of the table, not one
per batch. That is invisible to a correctness test, so the cursor mechanics are
tested instead: advance on every batch, advance past failures, and reset at the
end of a pass.
"""

from __future__ import annotations

from typing import Any

import psycopg2

from cortex_slack.ingest import ingest_message
from cortex_slack.services.parse_worker import START, parse_batch


def _ingest(conn: psycopg2.extensions.connection, channel: str, ts: str, **extra: Any) -> None:
    ingest_message(
        conn,
        channel,
        {"type": "message", "ts": ts, "user": "U1", "text": "hi", **extra},
        source="backfill",
        enqueue_parse=False,
    )
    conn.commit()


def test_a_message_is_parsed_with_its_tags(clean: psycopg2.extensions.connection) -> None:
    _ingest(clean, "C1", "1725379200.000001", bot_id="B1", subtype="bot_message")

    written, _ = parse_batch(clean)

    assert written == 1
    with clean.cursor() as cur:
        cur.execute("SELECT tags, user_id FROM slack_messages")
        row = cur.fetchone()
    assert row is not None and "automated" in row[0]


def test_the_cursor_advances_so_a_pass_is_one_scan(
    clean: psycopg2.extensions.connection,
) -> None:
    _ingest(clean, "C1", "1725379200.000001")
    _ingest(clean, "C1", "1725379200.000002")

    written, next_key = parse_batch(clean, limit=1)

    assert written == 1
    assert next_key == ("C1", "1725379200.000001"), "cursor did not advance"


def test_the_cursor_resets_at_the_end_of_a_pass(
    clean: psycopg2.extensions.connection,
) -> None:
    """Rows arrive behind the cursor constantly.

    The backfill walks each channel newest-first and the poller writes into the
    middle, so a cursor that stopped at the end would never see them.
    """
    _ingest(clean, "C1", "1725379200.000001")
    parse_batch(clean)

    written, next_key = parse_batch(clean, after=("C1", "1725379200.000001"))

    assert written == 0
    assert next_key is None, "end of pass must signal a reset, not a position"


def test_parsing_is_idempotent(clean: psycopg2.extensions.connection) -> None:
    """A parser fix is applied by deleting parsed rows and re-running."""
    _ingest(clean, "C1", "1725379200.000001")
    parse_batch(clean)

    written, _ = parse_batch(clean, after=START)

    assert written == 0, "re-parsed a message that was already current"


def test_an_empty_batch_does_not_leave_a_transaction_open(
    clean: psycopg2.extensions.connection,
) -> None:
    """Idling with a transaction open blocks every future ALTER TABLE.

    psycopg2 opens one on the first statement, so a SELECT that finds nothing
    still leaves the connection holding ACCESS SHARE. A sibling worker's
    apply_schema() then waits on an AccessExclusiveLock that is never granted --
    observed in production, with the media worker wedged for 75 seconds before
    anyone noticed it was a lock and not slow I/O.
    """
    written, next_key = parse_batch(clean)

    assert (written, next_key) == (0, None)
    assert clean.status == psycopg2.extensions.STATUS_READY, (
        "left a transaction open after finding no work"
    )


def test_a_rewrite_from_an_older_transaction_is_still_reparsed(
    clean: psycopg2.extensions.connection, dsn: str
) -> None:
    """NOW() is pinned at transaction start, and the poller's spans a channel.

    A raw row rewritten late in that transaction carries a fetched_at from its
    start. If the parser ran in between, "parsed_at < fetched_at" was false
    forever: raw text v2, parsed text v1, and nothing would revisit it. Version
    the parse instead -- record the fetched_at that was read, and re-parse
    whenever the raw row no longer carries it.
    """
    _ingest(clean, "C1", "1725379200.000001", text="v1")
    # The poller's long transaction begins: NOW() is frozen here.
    with clean.cursor() as cur:
        cur.execute("SELECT 1")

    parser = psycopg2.connect(dsn)
    try:
        written, _ = parse_batch(parser)
        assert written == 1

        # The rewrite lands inside the older transaction, so its fetched_at is
        # earlier than the parse that just happened.
        ingest_message(
            clean,
            "C1",
            {
                "type": "message",
                "ts": "1725379200.000001",
                "user": "U1",
                "text": "v2",
                "edited": {"ts": "1725379300.000000", "user": "U1"},
            },
            source="poll",
            enqueue_parse=False,
        )
        clean.commit()

        written, _ = parse_batch(parser)
        with parser.cursor() as cur:
            cur.execute("SELECT text FROM slack_messages WHERE ts = '1725379200.000001'")
            row = cur.fetchone()
    finally:
        parser.close()

    assert written == 1, "the rewrite was invisible to the work predicate"
    assert row is not None and row[0] == "v2"
