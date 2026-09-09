"""Tests for the ingest seam.

Live Postgres, not a mock. The defects this seam can have -- an ON CONFLICT that
fires when it should not, a dedup_key that does not dedup, a raw row committed
without its parse job -- are all invisible to a mock, which hands back whatever
the code asked for.
"""

from __future__ import annotations

from typing import Any

import psycopg2
import pytest

from cortex_slack.ingest import (
    PARSE_QUEUE,
    IngestResult,
    Outcome,
    ingest_message,
    message_key,
)

CHANNEL = "C0FAKECHAN"


def _message(ts: str, *, text: str = "hello", edited_ts: str | None = None) -> dict[str, Any]:
    message: dict[str, Any] = {"ts": ts, "type": "message", "user": "U123", "text": text}
    if edited_ts is not None:
        message["edited"] = {"ts": edited_ts, "user": "U123"}
    return message


def _raw_rows(conn: psycopg2.extensions.connection) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute("SELECT channel_id, ts, edited_ts, source FROM slack_messages_raw ORDER BY ts")
        return cur.fetchall()


def _queued(conn: psycopg2.extensions.connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM queue WHERE queue_name = %s", (PARSE_QUEUE,))
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


def test_first_arrival_is_stored_and_enqueued(clean: psycopg2.extensions.connection) -> None:
    result = ingest_message(clean, CHANNEL, _message("1725379200.001500"), source="backfill")

    assert result.outcome is Outcome.STORED
    assert result.job_id is not None
    assert _raw_rows(clean) == [(CHANNEL, "1725379200.001500", None, "backfill")]
    assert _queued(clean) == 1


def test_reingesting_an_unchanged_message_does_nothing(
    clean: psycopg2.extensions.connection,
) -> None:
    """The steady state of every poll: the overlap window is mostly unchanged.

    If this enqueued, a poller re-reading its overlap would refill the parse
    queue on every pass forever.
    """
    message = _message("1725379200.001500")
    ingest_message(clean, CHANNEL, message, source="backfill")
    clean.commit()

    result = ingest_message(clean, CHANNEL, message, source="poll")

    assert result.outcome is Outcome.UNCHANGED
    assert result.job_id is None
    assert _queued(clean) == 1  # still just the first job
    # The suppressed write must not have rewritten `source` either.
    assert _raw_rows(clean) == [(CHANNEL, "1725379200.001500", None, "backfill")]


def test_an_edit_updates_and_re_enqueues(clean: psycopg2.extensions.connection) -> None:
    ingest_message(clean, CHANNEL, _message("1725379200.001500"), source="poll")
    clean.commit()
    with clean.cursor() as cur:  # drain, so the re-enqueue is unambiguous
        cur.execute("DELETE FROM queue WHERE queue_name = %s", (PARSE_QUEUE,))
    clean.commit()

    result = ingest_message(
        clean,
        CHANNEL,
        _message("1725379200.001500", text="edited", edited_ts="1725379300.000000"),
        source="poll",
    )

    assert result.outcome is Outcome.UPDATED
    assert _raw_rows(clean) == [(CHANNEL, "1725379200.001500", "1725379300.000000", "poll")]
    assert _queued(clean) == 1


def test_the_same_ts_in_two_channels_are_different_messages(
    clean: psycopg2.extensions.connection,
) -> None:
    """Identity is (channel_id, ts), not ts.

    Slack hands out ts per channel, so a collision across channels is ordinary,
    not exotic. Deduping the parse job on ts alone would drop one of these.
    """
    ts = "1725379200.001500"
    first = ingest_message(clean, "C0AAA", _message(ts), source="backfill")
    second = ingest_message(clean, "C0BBB", _message(ts), source="backfill")

    assert first.outcome is Outcome.STORED
    assert second.outcome is Outcome.STORED
    assert _queued(clean) == 2, "one channel's message displaced the other's parse job"


def test_sub_second_suffix_distinguishes_messages(
    clean: psycopg2.extensions.connection,
) -> None:
    """Two messages in the same second are two messages.

    Parsing ts as a float to compare it loses this: 1725379200.001500 and
    1725379200.001501 are distinct ids but round to the same value at some
    precisions, and Slack's ordering depends on the suffix.
    """
    ingest_message(clean, CHANNEL, _message("1725379200.001500"), source="backfill")
    ingest_message(clean, CHANNEL, _message("1725379200.001501"), source="backfill")

    assert len(_raw_rows(clean)) == 2


def test_a_message_without_ts_is_refused(clean: psycopg2.extensions.connection) -> None:
    """A row with no identity is one no watermark can pass and no parse can find."""
    with pytest.raises(ValueError, match="has no ts"):
        ingest_message(clean, CHANNEL, {"type": "message", "text": "orphan"}, source="poll")

    assert _raw_rows(clean) == []


def test_ingest_does_not_commit(clean: psycopg2.extensions.connection) -> None:
    """The raw row and its parse job must land together or not at all.

    A committed row with no job is a message that never gets parsed and that
    nothing retries, because the next poll sees it as unchanged.
    """
    ingest_message(clean, CHANNEL, _message("1725379200.001500"), source="poll")
    clean.rollback()

    assert _raw_rows(clean) == []
    assert _queued(clean) == 0


def test_unchanged_result_cannot_carry_a_job_id() -> None:
    with pytest.raises(ValueError, match="must not carry a job_id"):
        IngestResult(CHANNEL, "1725379200.001500", Outcome.UNCHANGED, job_id=7)


def test_a_non_unchanged_result_may_have_no_job_id() -> None:
    """enqueue returns None when an identical job is already pending.

    That is success -- "the work is already covered" -- so asserting the
    converse invariant would raise on a correct path: an edit arriving while the
    previous edit's parse job is still queued.
    """
    assert IngestResult(CHANNEL, "1", Outcome.UPDATED, job_id=None).job_id is None


def test_message_key_separates_the_halves() -> None:
    assert message_key("C0AAA", "1725379200.001500") == "C0AAA/1725379200.001500"
    assert message_key("C0AAA", "1") != message_key("C0AAA1", "")
