"""Long-running incremental sync.

    python -m cortex_slack.services.run_poller

Refreshes the channel and user dimensions on each pass -- they are two requests
and re-reading them removes a whole class of staleness bug -- then polls every
readable conversation from its own watermark.
"""

from __future__ import annotations

import os
import signal
import sys
import time
from typing import Any

import psycopg2
from cortex_utils.logging import configure_logging, get_logger
from cortex_utils.metrics import start_metrics_server

from cortex_slack.config import apply_schema, dsn_from_env, stealth_tokens
from cortex_slack.services.dimensions import upsert_channels, upsert_users
from cortex_slack.services.poller import poll_once
from cortex_slack.slack.api import SlackAuthError, SlackClient, SlackError

logger = get_logger(__name__)

# How often the sleep between passes looks up to notice a stop request.
#
# time.sleep() is the wrong primitive for the whole interval: since PEP 475 a
# sleep interrupted by a signal is resumed with the remaining time once the
# handler returns, so a SIGTERM one second into a five-minute wait still waits
# out the rest. Measured on cortex-gmail-sync: a 2s sleep signalled at 0.3s
# returned at 2.0s. Docker's default stop grace is 10 seconds.
STOP_POLL_SECONDS = 0.5


def main() -> int:
    configure_logging("slack-sync", os.environ.get("LOG_LEVEL", "INFO"))
    stop = {"requested": False}

    def _request_stop(_signum: int, _frame: Any) -> None:
        # One lock-free store and nothing else. A handler runs on the main
        # thread, so anything here that takes a lock the main thread may already
        # hold deadlocks the process against itself.
        stop["requested"] = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _request_stop)

    base_url = os.environ.get("SLACK_BASE_URL")
    if not base_url:
        raise SystemExit("SLACK_BASE_URL is not set (e.g. https://yourteam.slack.com)")

    interval = int(os.environ.get("SLACK_POLL_INTERVAL_S", "300"))
    overlap = int(os.environ.get("SLACK_OVERLAP_SECONDS", "300"))
    start_metrics_server(int(os.environ.get("METRICS_PORT", "8000")))

    xoxc, xoxd = stealth_tokens()
    client = SlackClient(
        xoxc, xoxd, base_url, rate=float(os.environ.get("SLACK_RATE_PER_SECOND", "1.0"))
    )
    conn = psycopg2.connect(dsn_from_env())
    apply_schema(conn)

    while not stop["requested"]:
        started = time.monotonic()
        try:
            try:
                upsert_channels(conn, list(client.list_channels()))
                upsert_users(conn, list(client.list_users()))
            except SlackAuthError:
                raise  # a dead credential is not a staleness problem
            except SlackError as exc:
                # A staleness optimisation must not be able to stop the sync
                # whose gaps are permanent. Rate limiting surviving five
                # retries, or a fatal_error on either list call, used to
                # propagate out of main() and crash-loop with no backoff --
                # while poll_once goes to real trouble to survive exactly this
                # class of error per channel.
                logger.warning(
                    "dimension refresh failed; polling on last cycle's dimensions",
                    error=str(exc),
                )
                conn.rollback()
            stats = poll_once(conn, client, overlap_seconds=overlap)
        except SlackAuthError as exc:
            # The expected end of phase 1, not a crash. Say so plainly and stop:
            # every watermark is intact and the next run with a fresh token
            # resumes from exactly here.
            logger.error("slack credentials are no longer valid; stopping", error=str(exc))
            return 2

        logger.info(
            "poll finished",
            channels=stats.channels,
            stored=stats.stored,
            replies=stats.replies,
            swept_threads=stats.swept_threads,
            updated=stats.updated,
            unchanged=stats.unchanged,
            errors=stats.errors,
            failed=stats.failed_channels[:5],
            clean=stats.clean,
            elapsed_s=round(time.monotonic() - started, 1),
        )
        _interruptible_sleep(interval, stop)

    logger.info("stopping")
    return 0


def _interruptible_sleep(seconds: float, stop: dict[str, bool]) -> None:
    deadline = time.monotonic() + seconds
    while not stop["requested"]:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(STOP_POLL_SECONDS, remaining))


if __name__ == "__main__":
    sys.exit(main())
