"""Tests for the Slack client.

No network. The pieces worth testing here are the ones that go wrong silently:
timestamp validation, pagination that stops early, and the admin refusal.
"""

from __future__ import annotations

import pytest

from cortex_slack.slack.api import (
    TS_PATTERN,
    AdminMethodRefused,
    SlackAuthError,
    SlackClient,
    SlackError,
    validate_ts,
)


def _client(**kwargs: object) -> SlackClient:
    return SlackClient(
        "xoxc-test",
        "xoxd-test",
        "https://example.slack.com",
        **kwargs,  # type: ignore[arg-type]
    )


# -- timestamp validation ------------------------------------------------------


@pytest.mark.parametrize("value", ["1725379200.001500", "0000000000.000000"])
def test_valid_timestamps_pass(value: str) -> None:
    assert validate_ts(value, field="oldest") == value


@pytest.mark.parametrize(
    "value",
    [
        "2026-09-01T00:00:00Z",  # RFC3339 -- the granola bug, verbatim
        "2026-09-01T00:00:00+00:00",  # datetime.isoformat()
        "1725379200",  # seconds only, no sub-second half
        "1725379200.0015",  # too few digits after the dot
        "1725379200.00150000",  # too many
        "",
        "now",
    ],
)
def test_invalid_timestamps_are_refused(value: str) -> None:
    """The API ignores a malformed window and returns nothing.

    That is indistinguishable from "no new messages", which is how cortex-granola
    ran a broken incremental sync for weeks without noticing.
    """
    with pytest.raises(ValueError, match="is not a Slack ts"):
        validate_ts(value, field="oldest")


def test_history_validates_before_any_request(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bad window must fail locally, not become an empty result."""
    client = _client()
    posted: list[str] = []
    monkeypatch.setattr(client, "_post", lambda m, p: posted.append(m) or {"ok": True})

    with pytest.raises(ValueError, match="oldest"):
        list(client.history("C1", oldest="2026-09-01T00:00:00Z"))

    assert posted == [], "sent a request with a window the API would ignore"


def test_replies_validates_thread_ts(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client()
    monkeypatch.setattr(client, "_post", lambda m, p: {"ok": True})

    with pytest.raises(ValueError, match="thread_ts"):
        list(client.replies("C1", "not-a-ts"))


def test_ts_pattern_rejects_a_trailing_newline() -> None:
    """A token or ts pasted from a terminal often carries one.

    `$` in a Python regex also matches just before a trailing newline, so this
    is the case where a pattern that looks right is not.
    """
    assert not TS_PATTERN.match("1725379200.001500\n")


# -- admin refusal -------------------------------------------------------------


@pytest.mark.parametrize(
    "method",
    [
        "admin.conversations.search",
        "admin.users.list",
        "admin.conversations.getTeams",
    ],
)
def test_admin_methods_are_refused(method: str) -> None:
    """The session token carries admin scope; the client must not spend it.

    The archive records only what the account can already see. Enforced at the
    single request boundary so a method added later inherits the rule.
    """
    with pytest.raises(AdminMethodRefused, match="admin-scoped"):
        _client()._post(method, {})


def test_refusal_happens_before_the_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Refusing after the request would still have used the capability."""
    client = _client()
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda *a, **k: pytest.fail("an admin request reached the network"),
    )

    with pytest.raises(AdminMethodRefused):
        client._post("admin.users.list", {})


def test_ordinary_methods_are_not_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    """The guard must not be so broad it blocks the archive's own reads."""
    client = _client()
    monkeypatch.setattr(client, "_post", lambda m, p: {"ok": True, "channels": []})

    assert list(client.list_channels()) == []


# -- pagination ----------------------------------------------------------------


def test_pagination_follows_the_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = [
        {"ok": True, "messages": [{"ts": "1.1"}], "response_metadata": {"next_cursor": "c1"}},
        {"ok": True, "messages": [{"ts": "2.2"}], "response_metadata": {"next_cursor": ""}},
    ]
    seen_cursors: list[object] = []

    def _post(_method: str, params: dict[str, object]) -> dict[str, object]:
        seen_cursors.append(params.get("cursor"))
        return pages.pop(0)

    client = _client()
    monkeypatch.setattr(client, "_post", _post)

    out = list(client._paginate("conversations.history", "messages", {}))

    assert [m["ts"] for m in out] == ["1.1", "2.2"]
    assert seen_cursors == [None, "c1"]


def test_a_short_page_with_a_cursor_still_continues(monkeypatch: pytest.MonkeyPatch) -> None:
    """Slack returns fewer items than `limit` routinely.

    Stopping on a short page rather than on an empty cursor drops the tail of
    any conversation busier than one page -- and does it silently.
    """
    pages = [
        {"ok": True, "messages": [{"ts": "1.1"}], "response_metadata": {"next_cursor": "c1"}},
        {"ok": True, "messages": [{"ts": "2.2"}, {"ts": "3.3"}]},
    ]
    client = _client()
    monkeypatch.setattr(client, "_post", lambda m, p: pages.pop(0))

    out = list(client._paginate("conversations.history", "messages", {}))

    assert len(out) == 3


def test_a_missing_collection_key_ends_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty conversation omits the key rather than sending []."""
    client = _client()
    monkeypatch.setattr(client, "_post", lambda m, p: {"ok": True})

    assert list(client._paginate("conversations.history", "messages", {})) == []


# -- error mapping -------------------------------------------------------------


def test_auth_failures_are_their_own_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """A dead stealth token is the expected end of phase 1, not a fault.

    Distinguishing it lets the caller stop and say so instead of spending its
    retry budget on a credential that will never work again.
    """
    client = _client()
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda *a, **k: pytest.fail("should not be reached"),
    )
    monkeypatch.setattr(client, "_post", SlackClient._post.__get__(client))

    class _Response:
        def read(self) -> bytes:
            return b'{"ok": false, "error": "invalid_auth"}'

        def __enter__(self) -> _Response:
            return self

        def __exit__(self, *_: object) -> None:
            return None

    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: _Response())

    with pytest.raises(SlackAuthError) as caught:
        client._post("conversations.history", {})

    assert caught.value.code == "invalid_auth"
    assert isinstance(caught.value, SlackError), "callers catching SlackError must still see it"
