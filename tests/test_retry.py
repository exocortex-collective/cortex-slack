"""Tests for failure classification and backoff."""

from __future__ import annotations

import ssl
import urllib.error
from datetime import UTC, datetime, timedelta

import pytest

from cortex_slack.retry import (
    MAX_ATTEMPTS,
    Disposition,
    classify,
    next_attempt_at,
)


def _http(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://x", code, "nope", {}, None)  # type: ignore[arg-type]


# -- classification ------------------------------------------------------------


@pytest.mark.parametrize("code", [400, 404, 410, 422])
def test_client_errors_are_terminal(code: int) -> None:
    """The request is the problem; repeating it unchanged cannot fix that.

    Enumerating the statuses seen in the wild is how 400 was missed after 401,
    403, 404 and 410 had been handled -- 16 files reached 867 attempts.
    """
    assert classify(_http(code)).terminal


def test_rate_limiting_is_transient() -> None:
    """The one 4xx a retry fixes."""
    assert classify(_http(429)).disposition is Disposition.TRANSIENT


@pytest.mark.parametrize("code", [500, 502, 503, 504])
def test_server_errors_are_transient(code: int) -> None:
    assert classify(_http(code)).disposition is Disposition.TRANSIENT


@pytest.mark.parametrize(
    "exc",
    [
        TimeoutError("timed out"),
        urllib.error.URLError("connection refused"),
        ConnectionResetError(54, "reset by peer"),
        BrokenPipeError(32, "broken pipe"),
        ssl.SSLError("UNEXPECTED_EOF_WHILE_READING"),
    ],
)
def test_network_faults_are_transient(exc: Exception) -> None:
    """These are the world being briefly unavailable, not a bad request."""
    assert classify(exc).disposition is Disposition.TRANSIENT


def test_an_http_error_is_not_swallowed_by_the_oserror_clause() -> None:
    """HTTPError subclasses OSError via URLError.

    If the broad OSError check came first, every 404 would be classified
    transient and retried against a file that no longer exists.
    """
    assert classify(_http(404)).terminal


def test_an_unrecognised_failure_is_terminal() -> None:
    """Retrying something uncategorised spends a rate-limited budget on a guess."""
    assert classify(ValueError("who knows")).terminal


def test_the_reason_survives_classification() -> None:
    """It ends up in fetch_error, which is all anyone has later."""
    assert "404" in classify(_http(404)).reason


# -- backoff -------------------------------------------------------------------


def test_the_first_retry_is_not_immediate() -> None:
    """The whole point: five attempts inside one outage is not a retry policy."""
    now = datetime.now(UTC)

    due = next_attempt_at(1, now=now)

    assert due is not None and due > now + timedelta(seconds=30)


def test_the_delay_grows() -> None:
    now = datetime.now(UTC)

    delays = [next_attempt_at(n, now=now) for n in range(1, 5)]

    assert all(d is not None for d in delays)
    assert delays == sorted(delays), f"backoff did not grow: {delays}"  # type: ignore[type-var]


def test_the_delay_is_capped() -> None:
    """An unbounded double reaches days, which abandons a recoverable file."""
    now = datetime.now(UTC)

    due = next_attempt_at(MAX_ATTEMPTS - 1, now=now)

    assert due is not None and due < now + timedelta(hours=8)


def test_the_budget_runs_out() -> None:
    """None is how the caller learns to stop, rather than retrying forever."""
    assert next_attempt_at(MAX_ATTEMPTS) is None


def test_retries_are_jittered() -> None:
    """Everything knocked out by one outage must not come back in one instant.

    Without jitter the retry wave reproduces the load that caused the outage.
    """
    now = datetime.now(UTC)

    dues = {next_attempt_at(2, now=now) for _ in range(20)}

    assert len(dues) > 1, "every retry scheduled for the same moment"


def test_a_truncated_read_is_transient() -> None:
    """http.client.HTTPException is NOT an OSError.

    It descends straight from Exception, so the broad OSError clause never
    caught it and IncompleteRead -- a connection dropped mid-body, the most
    likely failure for a large file over a CDN -- was classified terminal and
    the file permanently abandoned.
    """
    import http.client

    assert not issubclass(http.client.HTTPException, OSError)
    assert classify(http.client.IncompleteRead(b"partial")).disposition is Disposition.TRANSIENT
    assert classify(http.client.BadStatusLine("garbage")).disposition is Disposition.TRANSIENT


@pytest.mark.parametrize("code", [401, 403])
def test_authorization_failures_are_session_scoped_not_terminal(code: int) -> None:
    """Phase 1's premise is that the token expires and is replaced by hand.

    "This session cannot have it" is not a permanent fact about the file, and
    treating it as one abandoned 35 files a fresh token would fetch.
    """
    assert classify(_http(code)).disposition is Disposition.TRANSIENT


def test_the_delay_cap_is_reachable() -> None:
    """A cap the budget can never reach is a docstring that lies.

    MAX_ATTEMPTS = 6 doubling from one minute tops out at sixteen, so a
    six-hour MAX_DELAY described a behaviour that could not occur.
    """
    from cortex_slack.retry import FIRST_DELAY, MAX_DELAY

    largest = FIRST_DELAY * (2 ** (MAX_ATTEMPTS - 2))
    assert largest >= MAX_DELAY, f"cap {MAX_DELAY} unreachable; largest delay is {largest}"
