"""Tests for the historical backfill.

Live Postgres. The properties worth proving here are about persistence across
interruption, which a mocked connection cannot show.
"""

from __future__ import annotations

from typing import Any

import psycopg2
import pytest

from cortex_slack.services import backfill
from cortex_slack.services.backfill import run_next_job, seed_jobs
from cortex_slack.slack.api import SlackAuthError, SlackError

C1 = "C0AAAAAAA"
C2 = "C0BBBBBBB"


def _msg(ts: str, **extra: Any) -> dict[str, Any]:
    return {"type": "message", "ts": ts, "user": "U1", "text": "hi", **extra}


class _PagedClient:
    """A client whose history arrives in pages, and which can be made to fail.

    `fail_on_page` raises once that page has been requested, standing in for the
    laptop suspending or the runtime being restarted mid-conversation.
    """

    def __init__(
        self,
        pages: list[list[dict[str, Any]]],
        *,
        threads: dict[str, list[dict[str, Any]]] | None = None,
        fail_on_page: int | None = None,
        error: Exception | None = None,
    ) -> None:
        self._pages = pages
        self._threads = threads or {}
        self._fail_on_page = fail_on_page
        self._error = error or RuntimeError("interrupted")
        self.requested_cursors: list[str | None] = []

    def history_pages(
        self,
        channel: str,
        *,
        oldest: str | None = None,
        latest: str | None = None,
        cursor: str | None = None,
    ) -> Any:
        index = 0 if cursor is None else int(cursor)
        while index < len(self._pages):
            self.requested_cursors.append(cursor)
            if self._fail_on_page is not None and index == self._fail_on_page:
                raise self._error
            next_index = index + 1
            next_cursor = str(next_index) if next_index < len(self._pages) else None
            yield self._pages[index], next_cursor
            index = next_index
            cursor = next_cursor

    def replies(self, channel: str, thread_ts: str) -> list[dict[str, Any]]:
        return self._threads.get(thread_ts, [])


def _job(conn: psycopg2.extensions.connection, channel_id: str) -> tuple[str, str | None, int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, cursor, stored FROM slack_backfill_jobs WHERE channel_id = %s",
            (channel_id,),
        )
        row = cur.fetchone()
        assert row is not None
        return str(row[0]), row[1], int(row[2])


def _stored(conn: psycopg2.extensions.connection, channel_id: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ts FROM slack_messages_raw WHERE channel_id = %s ORDER BY ts", (channel_id,)
        )
        return [r[0] for r in cur.fetchall()]


def _watermark(conn: psycopg2.extensions.connection, channel_id: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT last_ts FROM slack_sync_state WHERE channel_id = %s", (channel_id,))
        row = cur.fetchone()
        return None if row is None else row[0]


# -- seeding -------------------------------------------------------------------


def test_seeding_is_idempotent(clean: psycopg2.extensions.connection) -> None:
    channels = [{"id": C1, "is_member": True}, {"id": C2}]

    assert seed_jobs(clean, channels) == 2
    assert seed_jobs(clean, channels) == 0, "re-seeding queued the same work twice"


def test_joined_conversations_are_seeded_first(clean: psycopg2.extensions.connection) -> None:
    """An interrupted weekend run should have the corpus that matters, not a
    random prefix of it."""
    seed_jobs(
        clean,
        [
            {"id": "C0PUBLIC", "is_member": False},
            {"id": "D0DM", "is_im": True},
            {"id": "C0JOINED", "is_member": True},
        ],
    )

    with clean.cursor() as cur:
        cur.execute("SELECT channel_id FROM slack_backfill_jobs ORDER BY id")
        order = [r[0] for r in cur.fetchall()]

    assert order.index("C0PUBLIC") == 2, f"unjoined channel was not seeded last: {order}"


# -- walking -------------------------------------------------------------------


def test_a_channel_is_walked_to_completion(clean: psycopg2.extensions.connection) -> None:
    seed_jobs(clean, [{"id": C1}])
    client = _PagedClient([[_msg("1725379200.000001")], [_msg("1725379100.000002")]])

    result = run_next_job(clean, client)  # type: ignore[arg-type]

    assert result is not None and result.stored == 2
    assert _stored(clean, C1) == ["1725379100.000002", "1725379200.000001"]
    assert _job(clean, C1)[0] == "done"


def test_run_next_job_returns_none_when_idle(clean: psycopg2.extensions.connection) -> None:
    assert run_next_job(clean, _PagedClient([])) is None  # type: ignore[arg-type]


def test_threads_are_followed(clean: psycopg2.extensions.connection) -> None:
    """conversations.history returns parents only.

    A channel scan alone captures the message that started a thread and none of
    the conversation that followed it, which is usually where the meaning is.
    """
    parent = _msg("1725379200.000001", thread_ts="1725379200.000001", reply_count=2)
    seed_jobs(clean, [{"id": C1}])
    client = _PagedClient(
        [[parent]],
        threads={
            "1725379200.000001": [
                parent,
                _msg("1725379201.000001", thread_ts="1725379200.000001"),
                _msg("1725379202.000001", thread_ts="1725379200.000001"),
            ]
        },
    )

    result = run_next_job(clean, client)  # type: ignore[arg-type]

    assert result is not None and result.threads == 1
    assert len(_stored(clean, C1)) == 3


def test_a_reply_does_not_re_walk_its_own_thread(clean: psycopg2.extensions.connection) -> None:
    """A reply carries the parent's thread_ts.

    Treating `thread_ts is not None` as "starts a thread" would fetch the same
    thread once per reply -- N+1 requests against a rate limit measured in
    requests per minute.
    """
    reply = _msg("1725379201.000001", thread_ts="1725379200.000001", reply_count=2)
    seed_jobs(clean, [{"id": C1}])
    client = _PagedClient([[reply]])

    result = run_next_job(clean, client)  # type: ignore[arg-type]

    assert result is not None and result.threads == 0


# -- resumability --------------------------------------------------------------


def test_an_interrupted_channel_resumes_where_it_stopped(
    clean: psycopg2.extensions.connection,
) -> None:
    """The property the whole design exists for.

    The runtime suspends when the lid closes. A backfill that restarted each
    conversation from the top would never finish a large one.
    """
    pages = [
        [_msg("1725379200.000001")],
        [_msg("1725379100.000002")],
        [_msg("1725379000.000003")],
    ]
    seed_jobs(clean, [{"id": C1}])

    # Dies while fetching the third page, after two have landed.
    with pytest.raises(RuntimeError):
        run_next_job(clean, _PagedClient(pages, fail_on_page=2))  # type: ignore[arg-type]
    clean.rollback()

    assert len(_stored(clean, C1)) == 2, "committed work was lost"
    status, saved_cursor, _ = _job(clean, C1)
    assert saved_cursor == "2", f"no resume point saved: {saved_cursor!r}"

    # Put it back as pending the way a restart would, then resume.
    with clean.cursor() as cur:
        cur.execute(
            "UPDATE slack_backfill_jobs SET status = 'pending' WHERE channel_id = %s", (C1,)
        )
    clean.commit()

    resumed = _PagedClient(pages)
    result = run_next_job(clean, resumed)  # type: ignore[arg-type]

    assert resumed.requested_cursors[0] == "2", "restarted from the top instead of resuming"
    assert result is not None and result.stored == 1, "re-read pages it had already stored"
    assert len(_stored(clean, C1)) == 3


def test_the_cursor_never_runs_ahead_of_the_messages(
    clean: psycopg2.extensions.connection,
) -> None:
    """The cursor and the page it moves past commit together.

    A cursor stored first would skip that page's messages permanently after a
    crash -- silent loss, and invisible because the job still looks complete.
    """
    seed_jobs(clean, [{"id": C1}])
    client = _PagedClient([[_msg("1725379200.000001")], [_msg("1725379100.000002")]])

    run_next_job(clean, client)  # type: ignore[arg-type]

    # Every message the cursor has moved past is present.
    assert len(_stored(clean, C1)) == 2


# -- failure handling ----------------------------------------------------------


def test_a_failed_channel_does_not_advance_its_watermark(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Re-reading is cheap and idempotent; a watermark past unstored messages is
    permanent silent loss."""
    seed_jobs(clean, [{"id": C1}])

    def _boom(*_a: object, **_k: object) -> None:
        raise ValueError("bad message")

    monkeypatch.setattr(backfill, "ingest_message", _boom)
    client = _PagedClient([[_msg("1725379200.000001")]])

    result = run_next_job(clean, client)  # type: ignore[arg-type]

    assert result is not None and result.errors == 1
    assert not result.clean
    assert _job(clean, C1)[0] == "failed"
    assert _watermark(clean, C1) is None, "advanced past a message that was never stored"


def test_a_clean_channel_advances_its_watermark(clean: psycopg2.extensions.connection) -> None:
    seed_jobs(clean, [{"id": C1}])
    client = _PagedClient([[_msg("1725379200.000001"), _msg("1725379100.000002")]])

    run_next_job(clean, client)  # type: ignore[arg-type]

    assert _watermark(clean, C1) == "1725379200.000001", "watermark is not the newest ts seen"


def test_a_dead_token_releases_the_job_rather_than_failing_it(
    clean: psycopg2.extensions.connection,
) -> None:
    """A stealth token expiring is the expected end of phase 1, not a bad channel.

    Marking the job failed would bury a perfectly good conversation behind a
    credential problem, and the next run with a fresh token would skip it.
    """
    seed_jobs(clean, [{"id": C1}])
    client = _PagedClient(
        [[_msg("1725379200.000001")]],
        fail_on_page=0,
        error=SlackAuthError("conversations.history", "invalid_auth"),
    )

    with pytest.raises(SlackAuthError):
        run_next_job(clean, client)  # type: ignore[arg-type]
    clean.rollback()

    assert _job(clean, C1)[0] == "pending", "a dead credential buried the job"


def test_an_api_error_fails_the_job_without_raising(
    clean: psycopg2.extensions.connection,
) -> None:
    """One unreadable conversation must not end the run.

    323 of them; a channel that errors should be recorded and stepped over.
    """
    seed_jobs(clean, [{"id": C1}])
    client = _PagedClient(
        [[_msg("1725379200.000001")]],
        fail_on_page=0,
        error=SlackError("conversations.history", "channel_not_found"),
    )

    result = run_next_job(clean, client)  # type: ignore[arg-type]

    assert result is not None and result.errors == 1
    assert _job(clean, C1)[0] == "failed"


def test_the_backfill_does_not_fill_the_shared_queue(
    clean: psycopg2.extensions.connection,
) -> None:
    """History is found by query, not by queue.

    At this workspace's scale queueing history would add ~600k rows to the
    shared table gmail-sync, parse-worker and triage-worker all claim from --
    which already holds 336k and is unpartitioned. The raw row is itself the
    record that parsing is outstanding, so the job adds only contention.
    """
    seed_jobs(clean, [{"id": C1}])
    client = _PagedClient([[_msg("1725379200.000001"), _msg("1725379100.000002")]])

    run_next_job(clean, client)  # type: ignore[arg-type]

    with clean.cursor() as cur:
        cur.execute("SELECT count(*) FROM queue WHERE queue_name = 'slack_parse'")
        row = cur.fetchone()
    assert row is not None and row[0] == 0, "backfill queued parse jobs"
    assert len(_stored(clean, C1)) == 2, "and the messages are still archived"


def test_reseeding_does_not_requeue_finished_conversations(
    clean: psycopg2.extensions.connection,
) -> None:
    """Restarts must converge.

    Only skipping pending/running looks idempotent and is not: this service is
    built to be stopped and restarted, so every restart would re-queue the whole
    workspace and a frequently-interrupted run would never finish. Observed
    live -- three completed channels were re-queued on the second start.
    """
    seed_jobs(clean, [{"id": C1}])
    run_next_job(clean, _PagedClient([[_msg("1725379200.000001")]]))  # type: ignore[arg-type]
    assert _job(clean, C1)[0] == "done"

    assert seed_jobs(clean, [{"id": C1}]) == 0, "re-queued a finished conversation"


def test_a_full_rescan_can_be_asked_for_explicitly(
    clean: psycopg2.extensions.connection,
) -> None:
    """Skipping done by default must not make a deliberate re-scan impossible."""
    seed_jobs(clean, [{"id": C1}])
    run_next_job(clean, _PagedClient([[_msg("1725379200.000001")]]))  # type: ignore[arg-type]

    assert seed_jobs(clean, [{"id": C1}], include_done=True) == 1


def test_a_failed_conversation_is_retried_on_the_next_run(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Skipping done must not also skip failed -- that would strand it."""
    seed_jobs(clean, [{"id": C1}])
    monkeypatch.setattr(
        backfill, "ingest_message", lambda *a, **k: (_ for _ in ()).throw(ValueError("x"))
    )
    run_next_job(clean, _PagedClient([[_msg("1725379200.000001")]]))  # type: ignore[arg-type]
    assert _job(clean, C1)[0] == "failed"
    monkeypatch.undo()

    assert seed_jobs(clean, [{"id": C1}]) == 1, "a failed conversation was never retried"


def test_thread_replies_are_counted_in_the_total(
    clean: psycopg2.extensions.connection,
) -> None:
    """The reported count must reconcile with the rows actually stored.

    Replies were ingested but not counted, so a completed backfill reported
    1,513,696 against 1,545,184 rows in the table. An operator reconciling those
    cannot tell a counting gap from 31,488 lost messages -- which is the only
    reason the number is printed at all.
    """
    parent = _msg("1725379200.000001", thread_ts="1725379200.000001", reply_count=2)
    seed_jobs(clean, [{"id": C1}])
    client = _PagedClient(
        [[parent]],
        threads={
            "1725379200.000001": [
                parent,
                _msg("1725379201.000001", thread_ts="1725379200.000001"),
                _msg("1725379202.000001", thread_ts="1725379200.000001"),
            ]
        },
    )

    result = run_next_job(clean, client)  # type: ignore[arg-type]

    assert result is not None
    assert result.seen == len(
        _stored(clean, C1)
    ), f"reported {result.seen} but stored {len(_stored(clean, C1))}"
