"""Tests for failure classification and backoff."""

from __future__ import annotations

import ssl
import urllib.error
from datetime import timedelta

import pytest

from cortex_slack.retry import (
    MAX_ATTEMPTS,
    Disposition,
    classify,
    retry_delay_seconds,
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
    delay = retry_delay_seconds(1)

    assert delay is not None and delay > 30


def test_the_delay_grows() -> None:
    delays = [retry_delay_seconds(n) for n in range(1, 5)]

    assert all(d is not None for d in delays)
    assert delays == sorted(delays), f"backoff did not grow: {delays}"  # type: ignore[type-var]


def test_the_delay_is_capped() -> None:
    """An unbounded double reaches days, which abandons a recoverable file."""
    delay = retry_delay_seconds(MAX_ATTEMPTS - 1)

    assert delay is not None and delay < timedelta(hours=8).total_seconds()


def test_the_budget_runs_out() -> None:
    """None is how the caller learns to stop, rather than retrying forever."""
    assert retry_delay_seconds(MAX_ATTEMPTS) is None


def test_retries_are_jittered() -> None:
    """Everything knocked out by one outage must not come back in one instant.

    Without jitter the retry wave reproduces the load that caused the outage.
    """
    delays = {retry_delay_seconds(2) for _ in range(20)}

    assert len(delays) > 1, "every retry scheduled for the same moment"


def test_the_delay_is_a_duration_not_a_timestamp() -> None:
    """The server does the arithmetic, so one clock decides.

    Returning an absolute time put the Python clock into a column that the work
    query compares against Postgres NOW(), in the same UPDATE that sets
    last_attempt_at from NOW() -- one statement, two clocks.
    """
    delay = retry_delay_seconds(1)

    assert isinstance(delay, float)


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


@pytest.mark.parametrize("code", [301, 302, 303, 307, 308])
def test_a_refused_redirect_is_session_scoped(code: int) -> None:
    """files.slack.com bounces an unauthenticated request to the sign-in page.

    The media worker refuses to follow it, so the bounce arrives as an
    HTTPError. Classified TRANSIENT it spent the file's budget on the session's
    problem and abandoned the file -- round 3's finding, re-created by the fix
    for the credential leak.
    """
    exc = urllib.error.HTTPError(
        "https://files.slack.com/f/F1/x",
        code,
        "Found",
        {"Location": "https://valideval.slack.com/?redir=%2Ffiles-pri%2F"},  # type: ignore[arg-type]
        None,
    )

    decision = classify(exc)

    assert decision.disposition is Disposition.SESSION
    assert "valideval.slack.com" in decision.reason, "the redirect target belongs in the log"


@pytest.mark.parametrize("code", [401, 403])
def test_authorization_failures_are_session_scoped(code: int) -> None:
    """Not terminal, and not transient either.

    Calling them transient routed them through the per-file retry budget, whose
    exhaustion marks the file unfetchable anyway -- so the policy was negated 31
    minutes after being stated, and 66 files were abandoned in a 2m35s window.
    The remedy is a human pasting a new token, which takes days.
    """
    decision = classify(_http(code))

    assert decision.disposition is Disposition.SESSION
    assert not decision.terminal
    assert decision.session_scoped


def test_the_delay_cap_is_reachable() -> None:
    """A cap the budget can never reach is a docstring that lies.

    MAX_ATTEMPTS = 6 doubling from one minute tops out at sixteen, so a
    six-hour MAX_DELAY described a behaviour that could not occur.
    """
    from cortex_slack.retry import FIRST_DELAY, MAX_DELAY

    largest = FIRST_DELAY * (2 ** (MAX_ATTEMPTS - 2))
    assert largest >= MAX_DELAY, f"cap {MAX_DELAY} unreachable; largest delay is {largest}"
