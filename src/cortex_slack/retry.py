"""Deciding whether a failure is worth trying again, and when.

Two questions, and conflating them is how both of this service's retry bugs
happened. "Is this worth retrying?" is about the failure. "When?" is about how
long the cause is likely to last. Answering only the first gives you five
attempts fired during the same thirty-second outage, all of them certain to
fail, after which the file is given up on for a reason that had already passed.

Terminal means the request itself is the problem: repeating it unchanged cannot
succeed. Transient means the world was briefly unavailable.
"""

from __future__ import annotations

import http.client
import random
import urllib.error
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum

# Rate limiting is the one 4xx a retry fixes; every other client error says the
# request is wrong. Enumerating the ones seen in the wild is how 400 was missed
# after 401, 403, 404 and 410 had been handled -- 16 files accumulated 867
# attempts before anyone noticed.
RETRYABLE_STATUS = frozenset({429})

# Authorization failures. Scoped to the credential rather than the request, so a
# refreshed token changes the answer -- which makes them retryable, not terminal.
SESSION_SCOPED_STATUS = frozenset({401, 403})

FIRST_DELAY = timedelta(minutes=1)
# Reachable on purpose. With MAX_ATTEMPTS = 6 and doubling from one minute the
# largest delay is 1 * 2**4 = 16 minutes, so a six-hour cap was unreachable and
# the docstring describing it was simply wrong. Capping at the value the budget
# actually produces keeps the two honest.
MAX_DELAY = timedelta(minutes=16)
MAX_ATTEMPTS = 6

# Spread retries that failed together so they do not return together. Without
# it, everything knocked out by one outage comes back in the same instant and
# reproduces the load that caused it.
JITTER = 0.25


class Disposition(StrEnum):
    TERMINAL = "terminal"  # never going to work; stop asking
    TRANSIENT = "transient"  # try again later
    SESSION = "session"  # about the credential, not this item


@dataclass(frozen=True)
class Decision:
    disposition: Disposition
    reason: str

    @property
    def terminal(self) -> bool:
        return self.disposition is Disposition.TERMINAL

    @property
    def session_scoped(self) -> bool:
        """The credential is the problem; this file's budget must not pay for it."""
        return self.disposition is Disposition.SESSION


def classify(exc: BaseException) -> Decision:
    """Decide what a failure means. Never raises."""
    if isinstance(exc, urllib.error.HTTPError):
        if exc.code in RETRYABLE_STATUS:
            return Decision(Disposition.TRANSIENT, f"HTTP {exc.code}")
        if 300 <= exc.code < 400:
            # files.slack.com does not serve its sign-in page directly: an
            # unauthenticated request gets a 302 to it. The media worker refuses
            # to follow redirects (following one would carry the session to
            # whatever host it named), so that bounce arrives here as an
            # HTTPError -- and falling through to TRANSIENT charged the file's
            # budget for a credential problem, which is the abandonment round 3
            # fixed, re-created by the fix for the credential leak.
            location = exc.headers.get("Location", "?") if exc.headers else "?"
            return Decision(Disposition.SESSION, f"HTTP {exc.code} -> {location}")
        if exc.code in SESSION_SCOPED_STATUS:
            # Its own disposition, not TRANSIENT. Calling it transient routed it
            # through the per-file retry budget -- six attempts over about half
            # an hour -- and the end of that budget marks the file unfetchable
            # anyway. So the policy "a refreshed token changes the answer" was
            # negated 31 minutes after it was stated, and 66 files were
            # abandoned in a 2m35s window. The remedy here is a human pasting a
            # new token, which takes days, not minutes.
            return Decision(Disposition.SESSION, f"HTTP {exc.code}")
        if 400 <= exc.code < 500:
            return Decision(Disposition.TERMINAL, f"HTTP {exc.code}")
        # 5xx and anything else the server said: the server is having a
        # problem, which is the definition of transient.
        return Decision(Disposition.TRANSIENT, f"HTTP {exc.code}")

    if isinstance(
        exc,
        TimeoutError | urllib.error.URLError | ConnectionError | http.client.HTTPException,
    ):
        # http.client.HTTPException is NOT an OSError -- it descends straight
        # from Exception -- so the broad clause below never caught it. download()
        # calls response.read() inside the urlopen context and urllib does not
        # wrap read-time failures, so IncompleteRead (connection dropped
        # mid-body), BadStatusLine and LineTooLong arrived here raw, fell past
        # every check, and were classified TERMINAL. A truncated read is the most
        # likely failure for a 200MB ceiling over a CDN, and it was permanently
        # abandoning the file.
        #
        # RemoteDisconnected happens to be safe because it multiply-inherits
        # ConnectionResetError, which is exactly what made the gap survive a
        # spot check.
        return Decision(Disposition.TRANSIENT, type(exc).__name__)

    if isinstance(exc, OSError):
        # Broad on purpose, and last: ssl.SSLError, BrokenPipeError and socket
        # errors all land here, and every one of them is the network rather than
        # the request. Placed after the checks above because several of those
        # are OSError subclasses and would otherwise be swallowed here.
        return Decision(Disposition.TRANSIENT, type(exc).__name__)

    # An unrecognised failure is terminal rather than transient. Retrying
    # something we cannot categorise burns a rate-limited budget on a guess, and
    # the attempt counter would eventually give up anyway -- just later, and
    # with less in the log.
    return Decision(Disposition.TERMINAL, f"{type(exc).__name__}: {exc}")


def retry_delay_seconds(attempts: int) -> float | None:
    """How long to wait before a failure tried `attempts` times is tried again.

    None means the budget is spent. Doubling from one minute across six
    attempts, so the last wait is sixteen minutes and the whole budget spans
    about half an hour: long enough that each retry samples a genuinely
    different moment, short enough that a file whose channel was briefly
    unreachable is not abandoned for a day.
    """
    if attempts >= MAX_ATTEMPTS:
        return None

    delay = min(FIRST_DELAY * (2 ** max(attempts - 1, 0)), MAX_DELAY)
    # Seconds, not an absolute time. Computing the timestamp here put the Python
    # clock into a column that _PENDING then compares against Postgres NOW(), in
    # the same UPDATE that sets last_attempt_at from NOW() -- one statement, two
    # clocks. Harmless while both are the same kernel; this repo's own backfill
    # docstring is a warning about what that runtime's clock does across a
    # suspend.
    #
    # nosec: jitter spreads retries so a fleet does not return in lockstep. It
    # is load shaping, not a security decision -- nothing about the schedule
    # needs to be unpredictable to an attacker.
    jitter = 1 + random.random() * JITTER  # nosec B311
    return float(delay.total_seconds() * jitter)
