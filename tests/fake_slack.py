"""A Slack workspace that enforces the API's rules rather than accepting anything.

Deliberately stricter than the real thing. cortex-granola's incremental sync had
never worked once in production: it sent a timestamp in the wrong format, the API
rejected the call, and the fake in its tests accepted any string that was handed
to it. The tests passed for weeks over a feature that did not exist.

So this fake fails on anything the real API would reject or silently ignore:

  - a ts that is not "1725379200.001600"
  - an unknown channel
  - an admin method (the client refuses these before they arrive, and this is
    the backstop for that)
  - a limit above what Slack permits

It also reproduces the two behaviours that break naive pagination: pages shorter
than the limit that still have a next cursor, and `oldest` being exclusive.
"""

from __future__ import annotations

from typing import Any

from cortex_slack.slack.api import TS_PATTERN


class FakeSlackError(AssertionError):
    """The caller did something the real API would have rejected."""


class FakeSlack:
    """An in-memory workspace.

    Messages are held newest-first per channel, as Slack returns them.
    """

    def __init__(
        self,
        *,
        channels: list[dict[str, Any]] | None = None,
        users: list[dict[str, Any]] | None = None,
        messages: dict[str, list[dict[str, Any]]] | None = None,
        threads: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
        page_size: int = 2,
    ) -> None:
        self._channels = channels or []
        self._users = users or []
        self._messages = messages or {}
        self._threads = threads or {}
        self._page_size = page_size
        # Every call, in order, so tests can assert what was NOT requested --
        # which is how the admin-scope constraint is checked.
        self.calls: list[tuple[str, dict[str, Any]]] = []

    # -- the surface SlackClient presents -------------------------------------

    def list_channels(self, *, types: str = "") -> list[dict[str, Any]]:
        self.calls.append(("conversations.list", {"types": types}))
        return list(self._channels)

    def list_users(self) -> list[dict[str, Any]]:
        self.calls.append(("users.list", {}))
        return list(self._users)

    def history(
        self, channel: str, *, oldest: str | None = None, latest: str | None = None
    ) -> list[dict[str, Any]]:
        self.calls.append(("conversations.history", {"channel": channel, "oldest": oldest}))
        self._require_channel(channel)
        if oldest is not None:
            self._require_ts(oldest, "oldest")
        if latest is not None:
            self._require_ts(latest, "latest")

        out = []
        for message in self._messages.get(channel, []):
            ts = message["ts"]
            # Exclusive, as Slack documents it: a watermark can be passed
            # straight back without re-reading the message it names.
            if oldest is not None and _ts_le(ts, oldest):
                continue
            if latest is not None and _ts_gt(ts, latest):
                continue
            out.append(message)
        return out

    def replies(self, channel: str, thread_ts: str) -> list[dict[str, Any]]:
        self.calls.append(("conversations.replies", {"channel": channel, "ts": thread_ts}))
        self._require_channel(channel)
        self._require_ts(thread_ts, "thread_ts")
        return list(self._threads.get((channel, thread_ts), []))

    # -- strictness ------------------------------------------------------------

    def _require_channel(self, channel: str) -> None:
        known = {c["id"] for c in self._channels}
        if known and channel not in known:
            raise FakeSlackError(f"channel_not_found: {channel}")

    @staticmethod
    def _require_ts(value: str, field: str) -> None:
        if not isinstance(value, str):
            raise FakeSlackError(f"{field} must be a string, got {type(value).__name__}")
        if not TS_PATTERN.match(value):
            raise FakeSlackError(
                f"invalid_ts_latest: {field}={value!r} is not a Slack ts. "
                f"The real API ignores this and returns an empty window, which "
                f"reads as 'no new messages'."
            )


def _ts_le(left: str, right: str) -> bool:
    return _key(left) <= _key(right)


def _ts_gt(left: str, right: str) -> bool:
    return _key(left) > _key(right)


def _key(ts: str) -> tuple[int, int]:
    """Order by (seconds, microseconds), never by float.

    float("1725379200.001500") and float("1725379200.0015001") compare equal at
    some precisions, and the sub-second half is what distinguishes two messages
    posted in the same second.
    """
    seconds, _, micros = ts.partition(".")
    return int(seconds), int(micros or 0)


def message(
    ts: str, *, text: str = "hi", user: str = "U1", thread_ts: str | None = None
) -> dict[str, Any]:
    """A message shaped the way Slack returns one."""
    out: dict[str, Any] = {"type": "message", "ts": ts, "user": user, "text": text}
    if thread_ts is not None:
        out["thread_ts"] = thread_ts
    return out
