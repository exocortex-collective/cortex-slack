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

import random
import urllib.error
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

# Rate limiting is the one 4xx a retry fixes; every other client error says the
# request is wrong. Enumerating the ones seen in the wild is how 400 was missed
# after 401, 403, 404 and 410 had been handled -- 16 files accumulated 867
# attempts before anyone noticed.
RETRYABLE_STATUS = frozenset({429})

FIRST_DELAY = timedelta(minutes=1)
MAX_DELAY = timedelta(hours=6)
MAX_ATTEMPTS = 6

# Spread retries that failed together so they do not return together. Without
# it, everything knocked out by one outage comes back in the same instant and
# reproduces the load that caused it.
JITTER = 0.25


class Disposition(StrEnum):
    TERMINAL = "terminal"  # never going to work; stop asking
    TRANSIENT = "transient"  # try again later
    FATAL = "fatal"  # not about this item at all; stop the whole run


@dataclass(frozen=True)
class Decision:
    disposition: Disposition
    reason: str

    @property
    def terminal(self) -> bool:
        return self.disposition is Disposition.TERMINAL


def classify(exc: BaseException) -> Decision:
    """Decide what a failure means. Never raises."""
    if isinstance(exc, urllib.error.HTTPError):
        if exc.code in RETRYABLE_STATUS:
            return Decision(Disposition.TRANSIENT, f"HTTP {exc.code}")
        if 400 <= exc.code < 500:
            return Decision(Disposition.TERMINAL, f"HTTP {exc.code}")
        # 5xx and anything else the server said: the server is having a
        # problem, which is the definition of transient.
        return Decision(Disposition.TRANSIENT, f"HTTP {exc.code}")

    if isinstance(exc, TimeoutError | urllib.error.URLError | ConnectionError):
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


def next_attempt_at(attempts: int, *, now: datetime | None = None) -> datetime | None:
    """When a failure that has been tried `attempts` times may be tried again.

    None means the budget is spent. Doubling from one minute, capped at six
    hours: long enough that a retry samples a genuinely different moment, short
    enough that a file whose channel was briefly unreachable is not abandoned
    for a day.
    """
    if attempts >= MAX_ATTEMPTS:
        return None

    delay = min(FIRST_DELAY * (2 ** max(attempts - 1, 0)), MAX_DELAY)
    jittered = delay.total_seconds() * (1 + random.random() * JITTER)
    return (now or datetime.now(UTC)) + timedelta(seconds=jittered)
