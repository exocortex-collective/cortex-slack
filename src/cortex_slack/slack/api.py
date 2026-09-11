"""Slack Web API client for phase-1 stealth-token access.

Stealth tokens are the browser session's own credentials, so requests go to the
workspace subdomain (`https://valideval.slack.com/api/...`) rather than
`slack.com`, and carry the `d` cookie alongside the token. An internal Slack app
in phase 2 replaces both; this module is the only place that knows the
difference.

Every timestamp parameter is validated before it is sent. cortex-granola shipped
an incremental sync that had never once worked because it sent
`datetime.isoformat()` where the API wanted RFC3339, the API rejected it, and
the fake in the tests accepted any string at all. The check below is that bug's
fix written down in advance.
"""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from typing import Any

from cortex_utils.logging import get_logger

logger = get_logger(__name__)

# "1725379200.001500" -- ten digits, a dot, six more. Slack is consistent about
# this shape and the sub-second half is significant, so a value that does not
# match is a bug in the caller rather than an unusual message.
#
# \Z, not $. In Python `$` also matches just before a trailing newline, so `$`
# here accepts "1725379200.001500\n" -- which is what a ts pasted out of a
# terminal or read from a file looks like. It would pass validation and then be
# rejected by Slack, which is precisely the failure this pattern exists to
# catch. Measured: the `$` form matched, the `\Z` form does not.
TS_PATTERN = re.compile(r"^\d{10}\.\d{6}\Z")

# Conservative by default. An internal app gets 50+ req/min; a browser session
# is not an app and has no published allowance, so this sits near what the web
# client itself does rather than near what the documentation permits.
DEFAULT_RATE_PER_SECOND = 1.0
DEFAULT_BURST = 3

# Slack caps this at 1000 for apps that are allowed it. Asking for more than the
# server permits is not an error -- it silently returns fewer -- so pagination
# never assumes a full page means more pages.
PAGE_LIMIT = 200


class AdminMethodRefused(RuntimeError):
    """A caller tried to reach an admin-scoped endpoint.

    The session token this runs on happens to carry `admin` scope, which would
    let it read things the account cannot see through Slack's own UI --
    workspace-wide channel listings, other people's conversations, membership it
    was never granted. The archive's whole premise is that it records only what
    Devon could already read, so the extra reach is not a capability to use
    carefully; it is one to refuse.

    Enforced at the request boundary rather than documented, because "we agreed
    not to" is not a property anyone can verify from the code.
    """


class SlackError(RuntimeError):
    """An `ok: false` response, carrying Slack's own error code."""

    def __init__(self, method: str, code: str) -> None:
        super().__init__(f"{method}: {code}")
        self.method = method
        self.code = code


class SlackAuthError(SlackError):
    """The credentials are dead. Retrying will not help.

    Separate from SlackError because a stealth token expiring is the expected
    end of phase 1, not a fault: the caller should stop and say so plainly
    rather than burn its retry budget.
    """


_AUTH_CODES = frozenset({"invalid_auth", "not_authed", "account_inactive", "token_revoked"})


def validate_ts(value: str, *, field: str) -> str:
    """Refuse a timestamp Slack would not understand.

    Returns the value so it can be used inline. Raising here means a bad
    parameter fails on the first call with the name of the field, instead of
    being ignored by the API and surfacing as a window that silently returns
    nothing -- which reads exactly like "no new messages".
    """
    if not TS_PATTERN.match(value):
        raise ValueError(f"{field}={value!r} is not a Slack ts (expected 1725379200.001500)")
    return value


class _TokenBucket:
    def __init__(self, rate: float, burst: int) -> None:
        self._rate = rate
        self._capacity = float(burst)
        self._tokens = float(burst)
        self._updated = time.monotonic()

    def take(self) -> None:
        while True:
            now = time.monotonic()
            self._tokens = min(self._capacity, self._tokens + (now - self._updated) * self._rate)
            self._updated = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            time.sleep((1.0 - self._tokens) / self._rate)


class SlackClient:
    """Reads a Slack workspace. Never writes -- this is an archive, not a bot."""

    def __init__(
        self,
        xoxc_token: str,
        xoxd_token: str,
        base_url: str,
        *,
        timeout: int = 60,
        max_retries: int = 5,
        rate: float = DEFAULT_RATE_PER_SECOND,
        burst: int = DEFAULT_BURST,
    ) -> None:
        self._token = xoxc_token
        self._cookie = xoxd_token
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._bucket = _TokenBucket(rate, burst)

    def _post(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """One API call, with rate limiting and 429-aware retries.

        Refuses admin endpoints outright. Every request in this client goes
        through here, so the check cannot be sidestepped by adding a method
        later and forgetting the rule.
        """
        if method.startswith("admin."):
            raise AdminMethodRefused(
                f"{method} is admin-scoped; this archive records only what the "
                f"account can already see"
            )
        body = {k: v for k, v in params.items() if v is not None}
        body["token"] = self._token
        encoded = urllib.parse.urlencode(body).encode("utf-8")

        for attempt in range(self._max_retries + 1):
            self._bucket.take()
            request = urllib.request.Request(
                f"{self._base_url}/api/{method}",
                data=encoded,
                method="POST",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
                    # The d cookie stays percent-encoded exactly as the browser
                    # stores it. Decoding it produces a token Slack rejects.
                    "Cookie": f"d={self._cookie}",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=self._timeout) as response:
                    payload: dict[str, Any] = json.loads(response.read())
            except urllib.error.HTTPError as exc:
                delay = _retry_after(exc)
                if delay is None or attempt == self._max_retries:
                    raise
                logger.warning(
                    "slack http error, retrying", method=method, status=exc.code, delay_s=delay
                )
                time.sleep(delay)
                continue

            if payload.get("ok"):
                return payload

            code = str(payload.get("error", "unknown"))
            if code in _AUTH_CODES:
                raise SlackAuthError(method, code)
            if code == "ratelimited" and attempt < self._max_retries:
                # Slack can signal this in the body with a 200, not only as a
                # 429, so the HTTPError branch above does not cover it.
                delay = float(payload.get("retry_after", 30))
                logger.warning("slack ratelimited, retrying", method=method, delay_s=delay)
                time.sleep(delay)
                continue
            raise SlackError(method, code)

        raise SlackError(method, "retries exhausted")

    def _paginate(self, method: str, key: str, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """Walk a cursor-paginated collection.

        Stops on an empty next_cursor rather than on a short page: Slack returns
        fewer items than `limit` routinely, and treating that as the end drops
        the tail of any conversation busier than one page.
        """
        cursor: str | None = None
        while True:
            page = self._post(method, {**params, "limit": PAGE_LIMIT, "cursor": cursor})
            items = page.get(key) or []
            yield from items
            cursor = (page.get("response_metadata") or {}).get("next_cursor") or None
            if not cursor:
                return

    def list_channels(
        self, *, types: str = "public_channel,private_channel,mpim,im"
    ) -> Iterator[dict[str, Any]]:
        """Every conversation this account can see, DMs included.

        The scope is settled: this archive records what Devon's own account can
        already read. `exclude_archived` is deliberately not set -- an archived
        channel still holds history worth keeping.
        """
        yield from self._paginate("conversations.list", "channels", {"types": types})

    def list_users(self) -> Iterator[dict[str, Any]]:
        yield from self._paginate("users.list", "members", {})

    def history_pages(
        self,
        channel: str,
        *,
        oldest: str | None = None,
        latest: str | None = None,
        cursor: str | None = None,
    ) -> Iterator[tuple[list[dict[str, Any]], str | None]]:
        """History a page at a time, yielding (messages, cursor_after_this_page).

        The page-level form exists so a caller can checkpoint. `history()` hides
        the cursor, which is right for a poller reading a small window and wrong
        for a backfill on a machine that suspends without warning: without the
        cursor, an interrupted conversation restarts from the top, and a large
        one may never finish.

        `cursor` resumes a previous walk. The yielded cursor is what to store to
        resume *after* the page just handed over -- store it only once that
        page's messages are safely written, or a crash between the two loses
        them silently.
        """
        params: dict[str, Any] = {"channel": channel}
        if oldest is not None:
            params["oldest"] = validate_ts(oldest, field="oldest")
        if latest is not None:
            params["latest"] = validate_ts(latest, field="latest")

        while True:
            page = self._post(
                "conversations.history", {**params, "limit": PAGE_LIMIT, "cursor": cursor}
            )
            messages: list[dict[str, Any]] = page.get("messages") or []
            cursor = (page.get("response_metadata") or {}).get("next_cursor") or None
            yield messages, cursor
            if not cursor:
                return

    def history(
        self, channel: str, *, oldest: str | None = None, latest: str | None = None
    ) -> Iterator[dict[str, Any]]:
        """Messages in a channel, newest first, bounded by ts.

        `oldest` is exclusive in Slack's semantics, which is what makes it safe
        to pass a watermark directly without re-reading the message it names.
        """
        params: dict[str, Any] = {"channel": channel}
        if oldest is not None:
            params["oldest"] = validate_ts(oldest, field="oldest")
        if latest is not None:
            params["latest"] = validate_ts(latest, field="latest")
        yield from self._paginate("conversations.history", "messages", params)

    def replies(self, channel: str, thread_ts: str) -> Iterator[dict[str, Any]]:
        """Every message in a thread, including the parent.

        Slack does not return thread replies from conversations.history, so a
        channel scan alone captures only the parents. The parent is returned
        here too and is ingested idempotently, so callers do not filter it.
        """
        yield from self._paginate(
            "conversations.replies",
            "messages",
            {"channel": channel, "ts": validate_ts(thread_ts, field="thread_ts")},
        )


def _retry_after(exc: urllib.error.HTTPError) -> float | None:
    """Seconds to wait, or None if this error is not worth retrying."""
    if exc.code == 429:
        raw = exc.headers.get("Retry-After") if exc.headers else None
        try:
            return float(raw) if raw else 30.0
        except ValueError:
            return 30.0
    if 500 <= exc.code < 600:
        return 5.0
    return None
