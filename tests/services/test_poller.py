"""Tests for the incremental poller.

The properties worth proving are about the watermark, because a watermark that
advances past unstored messages loses them permanently and leaves nothing to
notice.
"""

from __future__ import annotations

from typing import Any

import psycopg2
import pytest

from cortex_slack.services import poller
from cortex_slack.services.poller import poll_once, ts_minus_seconds
from cortex_slack.slack.api import SlackAuthError, SlackError

C1 = "C0AAAAAAA"


def _msg(ts: str, **extra: Any) -> dict[str, Any]:
    return {"type": "message", "ts": ts, "user": "U1", "text": "hi", **extra}


class _Client:
    """Records the oldest it was asked for, which is the watermark under test."""

    def __init__(
        self, messages: list[dict[str, Any]] | None = None, error: Exception | None = None
    ) -> None:
        self._messages = messages or []
        self._error = error
        self.oldest_seen: list[str | None] = []

    def history(
        self, channel: str, *, oldest: str | None = None, latest: str | None = None
    ) -> list[dict[str, Any]]:
        self.oldest_seen.append(oldest)
        if self._error is not None:
            raise self._error
        return [m for m in self._messages if oldest is None or m["ts"] > oldest]


def _channel(conn: psycopg2.extensions.connection, channel_id: str = C1, **cols: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO slack_channels (channel_id, name, kind, is_archived, raw) "
            "VALUES (%s, %s, 'public', %s, '{}'::jsonb) ON CONFLICT DO NOTHING",
            (channel_id, cols.get("name", "general"), cols.get("is_archived", False)),
        )
    conn.commit()


def _watermark(conn: psycopg2.extensions.connection, channel_id: str = C1) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT last_ts FROM slack_sync_state WHERE channel_id = %s", (channel_id,))
        row = cur.fetchone()
        return None if row is None else row[0]


def _stored(conn: psycopg2.extensions.connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM slack_messages_raw")
        row = cur.fetchone()
        return 0 if row is None else int(row[0])


# -- the watermark ---------------------------------------------------------


def test_a_clean_pass_advances_to_the_newest_message_seen(
    clean: psycopg2.extensions.connection,
) -> None:
    _channel(clean)
    client = _Client([_msg("1725379200.000001"), _msg("1725379300.000002")])

    stats = poll_once(clean, client)  # type: ignore[arg-type]

    assert stats.stored == 2 and stats.clean
    assert _watermark(clean) == "1725379300.000002"


def test_a_failed_message_leaves_the_watermark_alone(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Re-reading costs requests; advancing past an unstored message is forever."""
    _channel(clean)
    monkeypatch.setattr(
        poller, "ingest_message", lambda *a, **k: (_ for _ in ()).throw(ValueError("x"))
    )

    stats = poll_once(clean, _Client([_msg("1725379200.000001")]))  # type: ignore[arg-type]

    assert stats.errors == 1 and not stats.clean
    assert _watermark(clean) is None, "advanced past a message that was never stored"


def test_one_failed_message_does_not_discard_its_neighbours(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ingest_message does not commit, so a plain rollback would drop the lot."""
    _channel(clean)
    real = poller.ingest_message
    calls = {"n": 0}

    def _fail_the_second(*args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 2:
            raise ValueError("bad")
        return real(*args, **kwargs)

    monkeypatch.setattr(poller, "ingest_message", _fail_the_second)
    client = _Client(
        [_msg("1725379200.000001"), _msg("1725379200.000002"), _msg("1725379200.000003")]
    )

    poll_once(clean, client)  # type: ignore[arg-type]

    assert _stored(clean) == 2, "one bad message took its neighbours with it"


def test_the_next_pass_reads_from_the_watermark_with_an_overlap(
    clean: psycopg2.extensions.connection,
) -> None:
    """Slack can deliver a message whose ts predates one already seen.

    Reading from the exact watermark would skip it permanently.
    """
    _channel(clean)
    client = _Client([_msg("1725379200.000001")])
    poll_once(clean, client, overlap_seconds=300)  # type: ignore[arg-type]

    poll_once(clean, client, overlap_seconds=300)  # type: ignore[arg-type]

    assert client.oldest_seen[0] is None, "first pass should read the whole channel"
    assert client.oldest_seen[1] == "1725378900.000001", "second pass lost the overlap"


def test_the_watermark_never_moves_backwards(clean: psycopg2.extensions.connection) -> None:
    """A deleted newest message would otherwise rewind and re-ingest forever."""
    _channel(clean)
    poll_once(clean, _Client([_msg("1725379300.000002")]))  # type: ignore[arg-type]

    poll_once(clean, _Client([_msg("1725379200.000001")]))  # type: ignore[arg-type]

    assert _watermark(clean) == "1725379300.000002"


# -- failure handling ------------------------------------------------------


def test_one_unreadable_channel_does_not_stop_the_pass(
    clean: psycopg2.extensions.connection,
) -> None:
    """326 conversations; one that errors is recorded and stepped over."""
    _channel(clean, "C0BAD", name="gone")
    _channel(clean, C1, name="general")

    class _SelectiveClient(_Client):
        def history(self, channel: str, **kwargs: Any) -> list[dict[str, Any]]:
            if channel == "C0BAD":
                raise SlackError("conversations.history", "channel_not_found")
            return [_msg("1725379200.000001")]

    stats = poll_once(clean, _SelectiveClient())  # type: ignore[arg-type]

    assert stats.failed_channels == ["gone"]
    assert stats.stored == 1, "a failing channel stopped the healthy one"
    assert _watermark(clean, C1) == "1725379200.000001"


def test_a_dead_session_stops_the_pass(clean: psycopg2.extensions.connection) -> None:
    """Not a per-channel problem; retrying 325 more times proves nothing."""
    _channel(clean)
    client = _Client(error=SlackAuthError("conversations.history", "invalid_auth"))

    with pytest.raises(SlackAuthError):
        poll_once(clean, client)  # type: ignore[arg-type]


def test_an_archived_channel_is_not_polled(clean: psycopg2.extensions.connection) -> None:
    """Its history is already captured and cannot change."""
    _channel(clean, is_archived=True)

    stats = poll_once(clean, _Client([_msg("1725379200.000001")]))  # type: ignore[arg-type]

    assert stats.channels == 0


# -- timestamp arithmetic --------------------------------------------------


def test_the_overlap_preserves_the_sub_second_half() -> None:
    """float arithmetic on a Slack ts is not exact at this magnitude."""
    assert ts_minus_seconds("1725379200.001500", 300) == "1725378900.001500"


def test_the_overlap_cannot_go_negative() -> None:
    assert ts_minus_seconds("0000000100.000001", 300) == "0.000001"
