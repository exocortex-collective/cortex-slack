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
from cortex_slack.slack.api import SlackAuthError, SlackError, validate_ts

C1 = "C0AAAAAAA"


def _msg(ts: str, **extra: Any) -> dict[str, Any]:
    return {"type": "message", "ts": ts, "user": "U1", "text": "hi", **extra}


class _Client:
    """Records the oldest it was asked for, which is the watermark under test.

    Validates every ts with the client's own predicate. cortex-granola's
    incremental sync never worked in production because its fake accepted any
    string where the API wanted one format; a fake laxer than the API is a test
    that passes over a feature that does not exist.
    """

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
        if oldest is not None:
            validate_ts(oldest, field="oldest")
        if latest is not None:
            validate_ts(latest, field="latest")
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
    """And the clamped value must still be a ts the client will send.

    "0.000001" is what this used to produce, and validate_ts refuses it with a
    ValueError -- which is not a SlackError and would have escaped poll_once.
    """
    oldest = ts_minus_seconds("0000000100.000001", 300)

    assert oldest == "0000000000.000001"
    assert validate_ts(oldest, field="oldest") == oldest


# -- thread sweeping -------------------------------------------------------


def _parent(conn: psycopg2.extensions.connection, ts: str, replies: int, channel: str = C1) -> None:
    from cortex_slack.ingest import ingest_message

    ingest_message(
        conn,
        channel,
        {
            "type": "message",
            "ts": ts,
            "user": "U1",
            "text": "start",
            "thread_ts": ts,
            "reply_count": replies,
        },
        source="poll",
        enqueue_parse=False,
    )
    conn.commit()


class _ThreadClient(_Client):
    """Both answers conversations.replies gives.

    A message that never started a thread is NOT an error: Slack returns ok
    with the parent alone, which walk_thread ignores, so [] stands in for it.
    thread_not_found is what Slack says for a message it no longer has; the
    fake says it only for a ts it was told is gone.
    """

    def __init__(
        self,
        replies: dict[str, list[dict[str, Any]]] | None = None,
        *,
        messages: list[dict[str, Any]] | None = None,
        gone: set[str] | None = None,
    ) -> None:
        super().__init__(messages or [])
        self._replies = replies or {}
        self._gone = gone or set()
        self.walked: list[str] = []

    def replies(self, channel: str, thread_ts: str) -> list[dict[str, Any]]:
        validate_ts(thread_ts, field="thread_ts")
        self.walked.append(thread_ts)
        if thread_ts in self._gone:
            raise SlackError("conversations.replies", "thread_not_found")
        return self._replies.get(thread_ts, [])


def test_a_thread_missing_replies_is_swept_whatever_its_age(
    clean: psycopg2.extensions.connection,
) -> None:
    """A reply does not change its parent's ts.

    So an older parent sinks below the watermark and its thread is frozen at
    whatever happened to be stored last time. Walking threads only for parents
    seen in the current pass left two threads at 0 of 6 and 0 of 3, their
    parents 95 minutes outside the overlap while a newer parent in the same
    channel was complete.
    """
    _channel(clean)
    old = "1600000000.000001"  # years old, far outside any activity window
    _parent(clean, old, replies=2)
    client = _ThreadClient(
        {old: [_msg("1600000001.000001", thread_ts=old), _msg("1600000002.000001", thread_ts=old)]}
    )

    stats = poll_once(clean, client)  # type: ignore[arg-type]

    assert old in client.walked, "a provably short thread was never swept"
    assert stats.swept_threads == 1
    assert _stored(clean) == 3


def test_a_complete_old_thread_is_not_swept(clean: psycopg2.extensions.connection) -> None:
    """Sweeping it spends a rate-limited request to confirm nothing changed.

    A years-old conversation is not going to gain a reply.
    """
    _channel(clean)
    old = "1600000000.000001"
    _parent(clean, old, replies=1)
    # Through ingest_message, not raw SQL. thread_ts is a column now, and the
    # sweep's short-thread check reads it -- a direct INSERT that leaves it null
    # makes a complete thread look incomplete and the test asserts the opposite
    # of what it means to.
    from cortex_slack.ingest import ingest_message

    ingest_message(
        clean,
        C1,
        {"type": "message", "ts": "1600000001.000001", "thread_ts": old, "user": "U2"},
        source="poll",
        enqueue_parse=False,
    )
    clean.commit()

    poll_once(clean, _ThreadClient())  # type: ignore[arg-type]

    with clean.cursor() as cur:
        cur.execute("SELECT thread_swept_at FROM slack_messages_raw WHERE ts = %s", (old,))
        row = cur.fetchone()
    assert row is not None and row[0] is None, "swept a complete, long-dead thread"


def test_the_sweep_is_bounded(clean: psycopg2.extensions.connection) -> None:
    """A full pass already takes 315s of a 300s interval.

    An unbounded sweep would push the poller further behind on every cycle.
    """
    _channel(clean)
    from cortex_slack.services.poller import THREAD_SWEEP_LIMIT

    for i in range(THREAD_SWEEP_LIMIT + 10):
        _parent(clean, f"16000000{i:02d}.000001", replies=1)
    client = _ThreadClient()

    poll_once(clean, client)  # type: ignore[arg-type]

    assert len(client.walked) == THREAD_SWEEP_LIMIT


def test_a_thread_started_after_the_parent_left_the_window_is_still_found(
    clean: psycopg2.extensions.connection,
) -> None:
    """The case the sweep exists for, and could not previously reach.

    A reply changes neither the parent's ts nor its edited_ts, so a parent
    stored before it had any replies carried reply_count = 0 forever. The sweep
    gated candidates on reply_count > 0, so it was structurally blind to exactly
    this: measured on the live archive, zero parents where stored < claimed,
    meaning the self-correcting arm had never once been able to fire.
    """
    _channel(clean)
    from cortex_slack.ingest import ingest_message

    # Relative, not a literal: a hardcoded ts from the day this was written
    # aged out of the seven-day recency window a week later and the test
    # started failing on main with the sweep untouched.
    old = _recent_ts(3600)  # recent enough to be active, outside any overlap
    ingest_message(
        clean,
        C1,
        {"type": "message", "ts": old, "user": "U1", "text": "question"},
        source="poll",
        enqueue_parse=False,
    )
    clean.commit()
    # Parsed, so the sweep's human-message arm can see its tags.
    from cortex_slack.services.parse_worker import parse_batch

    parse_batch(clean)

    reply = _msg(_recent_ts(1800), thread_ts=old)
    client = _ThreadClient({old: [reply]})

    poll_once(clean, client)  # type: ignore[arg-type]

    assert old in client.walked, "a thread started after capture was never swept"
    assert reply["ts"] in _stored_ts(clean), "the reply was not stored"


def _stored_ts(conn: psycopg2.extensions.connection) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT ts FROM slack_messages_raw ORDER BY ts")
        return [r[0] for r in cur.fetchall()]


def test_a_growing_thread_refreshes_its_reply_count(
    clean: psycopg2.extensions.connection,
) -> None:
    """The count is the only signal a thread has grown.

    ingest refused to rewrite the row unless edited_ts changed, and a reply
    changes neither ts nor edited_ts -- so the count froze at first capture.
    """
    _channel(clean)
    from cortex_slack.ingest import ingest_message

    ts = "1789000000.000001"
    base = {"type": "message", "ts": ts, "user": "U1", "text": "q", "thread_ts": ts}
    ingest_message(clean, C1, {**base, "reply_count": 0}, source="poll", enqueue_parse=False)
    clean.commit()

    ingest_message(clean, C1, {**base, "reply_count": 3}, source="poll", enqueue_parse=False)
    clean.commit()

    with clean.cursor() as cur:
        cur.execute("SELECT reply_count FROM slack_messages_raw WHERE ts = %s", (ts,))
        row = cur.fetchone()
    assert row is not None and row[0] == 3, "reply count froze at first capture"


def test_the_sweep_watermark_does_not_advance_on_a_failed_walk(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """thread_swept_at is a watermark in everything but name.

    Stamping it after a walk whose replies failed restarts the re-sweep timer
    over an incomplete thread.
    """
    _channel(clean)
    old = "1789000000.000001"
    _parent(clean, old, replies=1)
    from cortex_slack import threads

    monkeypatch.setattr(
        threads, "ingest_message", lambda *a, **k: (_ for _ in ()).throw(ValueError("x"))
    )
    client = _ThreadClient({old: [_msg("1789000001.000001", thread_ts=old)]})

    stats = poll_once(clean, client)  # type: ignore[arg-type]

    assert stats.errors >= 1
    with clean.cursor() as cur:
        cur.execute("SELECT thread_swept_at FROM slack_messages_raw WHERE ts = %s", (old,))
        row = cur.fetchone()
    assert row is not None and row[0] is None, "marked a failed sweep as done"


def test_one_failed_reply_does_not_take_the_thread_or_the_watermark_with_it(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """walk_thread's error count is the only thing holding the watermark for replies.

    Both adapters fold it into the counter that gates the watermark, and both
    suites tested that gate only for messages arriving through history(). A
    regression that swallowed a reply failure, or rolled back the transaction
    instead of the savepoint, would advance the watermark past unstored replies
    with the suite still green. This is the negative case.
    """
    _channel(clean)
    parent = "1725379200.000001"
    replies = [_msg(f"172537920{i}.000001", thread_ts=parent) for i in (1, 2, 3)]
    from cortex_slack import threads

    real_ingest = threads.ingest_message

    def _fail_the_second(conn: Any, channel_id: str, message: dict[str, Any], **kw: Any) -> Any:
        if message["ts"] == replies[1]["ts"]:
            raise ValueError("simulated ingest failure")
        return real_ingest(conn, channel_id, message, **kw)

    monkeypatch.setattr(threads, "ingest_message", _fail_the_second)
    client = _ThreadClient(
        {parent: replies}, messages=[_msg(parent, thread_ts=parent, reply_count=3)]
    )

    stats = poll_once(clean, client)  # type: ignore[arg-type]

    assert stats.errors >= 1, "a failed reply was swallowed"
    with clean.cursor() as cur:
        cur.execute("SELECT ts FROM slack_messages_raw ORDER BY ts")
        stored = [r[0] for r in cur.fetchall()]
    assert stored == [parent, replies[0]["ts"], replies[2]["ts"]], (
        "the savepoint did not isolate the failure: neighbouring replies were lost"
    )
    assert _watermark(clean) is None, "watermark advanced past an unstored reply"


def _recent_ts(seconds_ago: int, micros: str = "000001") -> str:
    import time

    return f"{int(time.time()) - seconds_ago:010d}.{micros}"


def test_the_sweep_does_not_walk_a_thread_by_its_replies(
    clean: psycopg2.extensions.connection,
) -> None:
    """A reply is a recent human message too, and nothing said "parent".

    Measured live: 101 of 636 human messages in a week were replies, so a sixth
    of the sweep budget re-fetched threads the parent row already covered --
    and stamped thread_swept_at on reply rows, which is meant to stay NULL.
    """
    _channel(clean)
    parent, reply = _recent_ts(3600), _recent_ts(1800)
    _parent(clean, parent, replies=1)
    from cortex_slack.ingest import ingest_message

    ingest_message(clean, C1, _msg(reply, thread_ts=parent), source="poll", enqueue_parse=False)
    clean.commit()
    client = _ThreadClient({parent: [_msg(reply, thread_ts=parent)]})

    poll_once(clean, client)  # type: ignore[arg-type]

    assert client.walked == [parent], f"walked by a reply's ts: {client.walked}"


def test_a_thread_walked_in_the_channel_pass_is_not_swept_again_the_same_pass(
    clean: psycopg2.extensions.connection,
) -> None:
    """The channel pass walks a new thread; the sweep runs minutes later.

    Without the channel pass stamping thread_swept_at, the parent is recent and
    unswept when the sweep looks, and the same thread costs two requests.
    """
    _channel(clean)
    parent = _recent_ts(600)
    reply = _msg(_recent_ts(300), thread_ts=parent)
    client = _ThreadClient(
        {parent: [reply]}, messages=[_msg(parent, thread_ts=parent, reply_count=1)]
    )

    poll_once(clean, client)  # type: ignore[arg-type]

    assert client.walked == [parent], f"walked twice in one pass: {client.walked}"


def _stamped(conn: psycopg2.extensions.connection, ts: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT thread_swept_at FROM slack_messages_raw WHERE ts = %s", (ts,))
        row = cur.fetchone()
    return row is not None and row[0] is not None


def test_a_threadless_message_walks_clean(clean: psycopg2.extensions.connection) -> None:
    """The common case: the recency arm checks a message that has no thread.

    Slack answers ok with the parent alone. 646 of these in a week, every
    one stamped by an ordinary successful walk -- this is not the error case.
    """
    _channel(clean)
    lonely = _recent_ts(3600)
    from cortex_slack.ingest import ingest_message

    ingest_message(clean, C1, _msg(lonely), source="poll", enqueue_parse=False)
    clean.commit()
    client = _ThreadClient()

    stats = poll_once(clean, client)  # type: ignore[arg-type]

    assert client.walked == [lonely]
    assert (stats.errors, stats.replies, stats.missing_threads) == (0, 0, 0)
    assert stats.swept_threads == 1 and _stamped(clean, lonely)


def test_a_message_slack_no_longer_has_is_not_an_error(
    clean: psycopg2.extensions.connection,
) -> None:
    """thread_not_found means the message is gone from the workspace.

    Measured live: 26 rows, every message in one group DM across two hours,
    reported as 26 errors on a clean pass and, unstamped, taking 26 of the 40
    sweep slots on every pass after.
    """
    _channel(clean)
    gone = _recent_ts(3600)
    from cortex_slack.ingest import ingest_message

    ingest_message(clean, C1, _msg(gone), source="poll", enqueue_parse=False)
    clean.commit()
    client = _ThreadClient(gone={gone})

    stats = poll_once(clean, client)  # type: ignore[arg-type]

    assert client.walked == [gone]
    assert stats.errors == 0, "a vanished message was counted as a failure"
    assert stats.missing_threads == 1 and stats.swept_threads == 0, (
        "walked nothing, counted as a walk"
    )
    assert _stamped(clean, gone), "not stamped, so it sorts first on every pass"


def test_a_vanished_short_thread_is_released_from_the_short_arm(
    clean: psycopg2.extensions.connection,
) -> None:
    """The provably-short arm has no age gate and no stamp gate.

    A parent that claims more replies than we hold is selected on every pass
    regardless -- so a thread that vanished from Slack while short would be
    walked every five minutes forever. Clamping the claim to what we hold
    releases it; the recency arm still re-checks inside its window.
    """
    _channel(clean)
    old = "1600000000.000001"  # far outside the recency window: only arm 1 can select it
    _parent(clean, old, replies=3)
    client = _ThreadClient(gone={old})

    poll_once(clean, client)  # type: ignore[arg-type]
    poll_once(clean, client)  # type: ignore[arg-type]

    assert client.walked == [old], f"re-walked a thread Slack no longer has: {client.walked}"
    with clean.cursor() as cur:
        cur.execute("SELECT reply_count FROM slack_messages_raw WHERE ts = %s", (old,))
        row = cur.fetchone()
    assert row is not None and row[0] == 0, "the stale claim was kept"


def test_a_message_that_comes_back_is_re_walked_on_the_next_check(
    clean: psycopg2.extensions.connection,
) -> None:
    """Stamping a vanished message must not stop the re-check inside the window."""
    _channel(clean)
    parent = _recent_ts(3600)
    from cortex_slack.ingest import ingest_message

    ingest_message(clean, C1, _msg(parent), source="poll", enqueue_parse=False)
    clean.commit()
    poll_once(clean, _ThreadClient(gone={parent}))  # type: ignore[arg-type]

    with clean.cursor() as cur:
        cur.execute(
            "UPDATE slack_messages_raw SET thread_swept_at = NOW() - interval '7 hours' "
            "WHERE ts = %s",
            (parent,),
        )
    clean.commit()
    reply = _msg(_recent_ts(60), thread_ts=parent)
    stats = poll_once(clean, _ThreadClient({parent: [reply]}))  # type: ignore[arg-type]

    assert stats.replies == 1 and stats.errors == 0
    assert _stored(clean) == 2, "the thread came back and its reply was never fetched"


def test_an_unreadable_thread_is_still_an_error(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only thread_not_found is benign; anything else Slack says stays a failure."""
    _channel(clean)
    parent = _recent_ts(3600)
    _parent(clean, parent, replies=1)
    client = _ThreadClient()
    monkeypatch.setattr(
        client,
        "replies",
        lambda *_a, **_k: (_ for _ in ()).throw(SlackError("conversations.replies", "fatal_error")),
    )

    stats = poll_once(clean, client)  # type: ignore[arg-type]

    assert stats.errors == 1
    with clean.cursor() as cur:
        cur.execute("SELECT thread_swept_at FROM slack_messages_raw WHERE ts = %s", (parent,))
        row = cur.fetchone()
    assert row is not None and row[0] is None, "a failed walk was stamped as swept"


@pytest.mark.parametrize(("claimed", "held", "expected"), [(2, 2, 2), (3, 1, 1)])
def test_the_vanished_claim_is_clamped_to_what_is_held_not_zeroed(
    clean: psycopg2.extensions.connection, claimed: int, held: int, expected: int
) -> None:
    """LEAST(claim, held), not zero.

    Both production parents in the vanished band were held in full, which a
    held=0 test cannot tell from "set to zero". A full hold keeps the claim; a
    partial hold takes what is held; either way the short arm is released.
    """
    _channel(clean)
    old = "1600000000.000001"
    _parent(clean, old, replies=claimed)
    from cortex_slack.ingest import ingest_message

    for i in range(1, held + 1):
        ingest_message(
            clean,
            C1,
            _msg(f"160000000{i}.000001", thread_ts=old),
            source="poll",
            enqueue_parse=False,
        )
    clean.commit()
    client = _ThreadClient(gone={old})

    poll_once(clean, client)  # type: ignore[arg-type]
    poll_once(clean, client)  # type: ignore[arg-type]

    with clean.cursor() as cur:
        cur.execute("SELECT reply_count FROM slack_messages_raw WHERE ts = %s", (old,))
        row = cur.fetchone()
    assert row is not None and row[0] == expected
    assert client.walked.count(old) <= 1, "still selected by the short arm after the clamp"
