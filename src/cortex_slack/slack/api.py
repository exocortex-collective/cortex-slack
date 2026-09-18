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

import http.client
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

# Backoff for a failure below HTTP: a dropped TLS socket, a reset, a timeout,
# a body that stopped arriving. Doubling from one second, capped, across the
# same max_retries the HTTP path uses -- so five retries span about half a
# minute of SLEEP, long enough that each samples a different moment of an
# outage.
#
# Sleep is not elapsed time. Each attempt may also spend up to `timeout`
# seconds (60 by default) waiting on the socket, so one _post can occupy
# (max_retries + 1) * timeout on top of whatever it sleeps -- and run_poller
# only reads the stop flag between passes, so a stop during a pass waits that
# out. Cortex-b4o.20 tracks making the wait interruptible.
#
# This bound covers the transport arm only. The sleeps the SERVER asks for --
# a 429's Retry-After, a body-level retry_after -- were unbounded by anything
# but MAX_SLEEP_BUDGET_S below. Measured at the production defaults before it
# existed: a plain 429 with no header slept 150s across the budget, and one
# carrying Retry-After: 99999 slept 1500s, in bare uninterruptible calls.
TRANSPORT_FIRST_DELAY_S = 1.0
# 1 * 2**4: the largest delay the default budget of five retries produces. A
# higher cap would describe a wait that cannot happen, which retry.py already
# had to correct once; it binds only for a caller passing max_retries >= 6.
TRANSPORT_MAX_DELAY_S = 16.0

# Slack caps this at 1000 for apps that are allowed it. Asking for more than the
# server permits is not an error -- it silently returns fewer -- so pagination
# never assumes a full page means more pages.
PAGE_LIMIT = 200


# The longest this client will sleep at a server's request. Beyond it, waiting
# costs more than rescheduling: both callers resume for free -- the backfill
# releases the job with its cursor, the poller moves to the next channel -- and
# a bare time.sleep is not interruptible, since PEP 475 resumes it past a
# signal. So a longer request raises instead, and the work comes back on the
# next pass rather than inside an unanswerable process.
#
# Still longer than Docker's 10s stop grace; making the wait itself
# interruptible is Cortex-b4o.20. This bounds the damage, it does not end it.
MAX_SLEEP_BUDGET_S = 60.0


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
    """A failed call. `code` is Slack's error string when there was one, else a short reason.

    The one type the poller's per-channel handler contains, so every failure
    _post can produce is this or a subclass -- an HTTP status Slack would not
    retry, a body that was not JSON, a socket that dropped, or `ok: false`.
    """

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


class SlackTransportError(SlackError):
    """The network, not the request. Retrying later can succeed.

    Its own subclass so the backfill can tell "this conversation cannot be read"
    (mark the job failed) from "the network was down for half a minute" (release
    the job with its cursor and resume). SlackError carries no disposition, and
    retry.py's preamble is a record of what conflating the two costs.

    `retry_after` carries the quiet the server explicitly asked for, when it
    asked for one. Raising instead of sleeping keeps the process answerable to a
    signal, but discarding the number turned a request for 99999 seconds of
    quiet into a re-claim loop measured at 96 requests a minute -- worse for the
    server than the sleep it replaced. The caller decides how to honour it; it
    must not simply vanish with the exception.
    """

    def __init__(self, method: str, code: str, retry_after: float | None = None) -> None:
        super().__init__(method, code)
        self.retry_after = retry_after


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
        # Checked once here rather than on every request. urlopen honours
        # file:// and ftp://, and this value comes from the environment, so a
        # misconfigured SLACK_BASE_URL would otherwise turn every API call into
        # a local file read that returns something plausible-looking.
        if urllib.parse.urlparse(base_url).scheme not in ("https", "http"):
            raise ValueError(f"SLACK_BASE_URL must be http(s): {base_url!r}")
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._bucket = _TokenBucket(rate, burst)

    def _post(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """One API call, with rate limiting and retries. Raises SlackError, never below it.

        Refuses admin endpoints outright. Every request in this client goes
        through here, so the check cannot be sidestepped by adding a method
        later and forgetting the rule.

        Every failure leaves here as a SlackError (or the AuthError subclass),
        because that is the one type the poller's per-channel handler contains.
        A BrokenPipeError from the TLS socket used to leave as itself: it
        passed the HTTPError clause, passed poll_once's `except SlackError`,
        left main(), and the process died -- to be revived by the restart
        policy, which is the crash-loop shape this codebase spent a PR removing
        from gmail-sync. Measured: one restart, on the first pass after a
        deploy, for a single dropped socket.
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
                with urllib.request.urlopen(  # nosec B310 - scheme checked in __init__
                    request, timeout=self._timeout
                ) as response:
                    payload = json.loads(response.read())
            except urllib.error.HTTPError as exc:
                delay = self._http_retry_delay(method, exc, attempt)
                logger.warning(
                    "slack http error, retrying", method=method, status=exc.code, delay_s=delay
                )
                time.sleep(delay)
                continue
            except (OSError, http.client.HTTPException, ValueError) as exc:
                # Below HTTP: the connect failed, the socket dropped mid-read, or
                # the body stopped before it was JSON. ValueError rather than
                # JSONDecodeError because json.loads on non-UTF-8 bytes raises
                # UnicodeDecodeError, a ValueError that is not a JSONDecodeError.
                # All transient by nature, none a statement about the request.
                # Ordered after HTTPError on purpose -- HTTPError is a URLError
                # is an OSError, and the reverse order would swallow every 4xx
                # and 5xx here.
                if attempt == self._max_retries:
                    raise SlackTransportError(
                        method,
                        f"transport failed after {attempt + 1} attempts: "
                        f"{type(exc).__name__}: {exc}",
                    ) from exc
                delay = min(TRANSPORT_MAX_DELAY_S, TRANSPORT_FIRST_DELAY_S * 2**attempt)
                logger.warning(
                    "slack transport error, retrying",
                    method=method,
                    error=type(exc).__name__,
                    delay_s=delay,
                )
                time.sleep(delay)
                continue

            result = self._interpret(method, payload, retry_allowed=attempt < self._max_retries)
            if result is not None:
                return result

        # Unreachable for any max_retries >= 0: every `continue` above is
        # guarded by `attempt < self._max_retries`, so the final iteration
        # always returns or raises. Kept because the function must terminate
        # for the type checker, and because a caller passing a negative
        # max_retries would otherwise fall off the end returning None.
        raise SlackError(method, "retries exhausted")

    def _http_retry_delay(self, method: str, exc: urllib.error.HTTPError, attempt: int) -> float:
        """Seconds to wait before retrying an HTTP failure, or raise if it is over.

        Two dispositions, which _retry_after already separates and an earlier
        version of this collapsed. A status Slack will not change its mind about
        -- 404, 400, any 4xx but 429 -- is terminal. A 429 or 5xx that outlives
        the budget is the network being unavailable, the same disposition as a
        dropped socket, so it raises the transport type and the backfill
        releases the job with its cursor instead of failing it and re-reading
        the conversation from page 1.

        Either way it leaves as a SlackError. Raw, it was an OSError that passed
        poll_once's handler and ended the process -- a Slack outage being
        precisely when that happens.
        """
        delay = _retry_after(exc)
        if delay is None:
            raise SlackError(method, f"HTTP {exc.code}") from exc
        if attempt == self._max_retries:
            raise SlackTransportError(
                method, f"HTTP {exc.code} after {attempt + 1} attempts", delay
            ) from exc
        if delay > MAX_SLEEP_BUDGET_S:
            raise SlackTransportError(
                method, f"HTTP {exc.code} asked for {delay:.0f}s; resuming later", delay
            ) from exc
        return delay

    def _interpret(
        self, method: str, payload: Any, *, retry_allowed: bool
    ) -> dict[str, Any] | None:
        """Turn a decoded body into a result, an exception, or None meaning "try again".

        None is returned only for a body-level `ratelimited`, after sleeping
        the interval Slack asked for. Slack can signal that in the body with a
        200, not only as a 429, so the HTTPError path does not cover it.
        """
        if not isinstance(payload, dict):
            raise SlackError(method, f"non-object response: {type(payload).__name__}")
        if payload.get("ok"):
            return payload

        code = str(payload.get("error", "unknown"))
        if code in _AUTH_CODES:
            raise SlackAuthError(method, code)
        if code == "ratelimited":
            # Normalised, not merely parsed: this runs outside _post's try, so
            # a value time.sleep refuses would leave as a raw ValueError or
            # OverflowError -- the class of leak the docstring rules out.
            # Computed before the budget check so the exhausted path can carry
            # it too: this class's own docstring says the delay must not vanish
            # with the exception, and dropping it here left the runner with
            # nothing to wait on precisely when a server had said how long.
            delay = sleep_seconds(payload.get("retry_after"))
            if not retry_allowed:
                # Rate limiting is the one 4xx retry.py singles out as "a retry
                # fixes this", so an exhausted budget is the network, not the
                # request -- the backfill must keep its cursor.
                raise SlackTransportError(method, "ratelimited past the retry budget", delay)
            if delay > MAX_SLEEP_BUDGET_S:
                raise SlackTransportError(
                    method, f"ratelimited for {delay:.0f}s; resuming later", delay
                )
            logger.warning("slack ratelimited, retrying", method=method, delay_s=delay)
            time.sleep(delay)
            return None
        raise SlackError(method, code)

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
        return sleep_seconds(exc.headers.get("Retry-After") if exc.headers else None)
    if 500 <= exc.code < 600:
        return 5.0
    return None


def sleep_seconds(raw: Any, default: float = 30.0) -> float:
    """A delay someone else asked for, made safe to hand to time.sleep().

    Parsing is not enough. -1, "nan" and "inf" all float() cleanly and then
    time.sleep raises ValueError, ValueError and OverflowError respectively --
    and OverflowError is not a ValueError, so it escapes every handler in this
    module. Retry-After reaches us from any intermediary, not only from Slack,
    and the body's retry_after is whatever the response said.

    A value that is absent, unparseable, negative or NaN says nothing usable, so
    it takes the default. Anything else is returned as asked, including `inf`:
    deciding whether a usable-but-long delay is worth waiting for belongs to the
    caller, which has somewhere better to be. Rejecting NaN here is what keeps
    it out of the caller's comparison, where it would compare false against the
    budget and reach time.sleep.
    """
    try:
        delay = default if raw is None else float(raw)
    except (TypeError, ValueError):
        return default
    if delay != delay or delay < 0.0:  # NaN compares false against everything
        return default
    return delay
