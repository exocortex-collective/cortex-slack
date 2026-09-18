"""Entry point for the phase-1 historical backfill.

Runs until every conversation is done or the credential dies. Safe to stop and
restart at any point: jobs are claimed one at a time and each page commits with
its own resume point.

    python -m cortex_slack.services.run_backfill [--max-jobs N]
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import psycopg2
from cortex_utils.logging import configure_logging, get_logger

from cortex_slack.config import apply_schema, dsn_from_env, stealth_tokens
from cortex_slack.services.backfill import ChannelResult, run_next_job, seed_jobs
from cortex_slack.services.dimensions import upsert_channels, upsert_users
from cortex_slack.slack.api import SlackAuthError, SlackClient

# The longest the runner will pause before re-claiming a job the network
# released. Long enough to stop a hot re-claim loop, short enough that a
# manually-run backfill stays interruptible -- the client refuses to sleep at
# all beyond MAX_SLEEP_BUDGET_S for the same reason.
MAX_RECLAIM_WAIT_S = 60.0


def reclaim_wait(result: ChannelResult) -> float | None:
    """Seconds to pause before re-claiming, or None to carry straight on.

    Its own function because nothing imports run_backfill: the module is an
    entry point, the suite never loads it, and all four mutants of the inline
    version survived 222 green tests -- including deleting the wait entirely,
    which restores the 96 requests a minute round 6 measured and fixed.

    _claim orders by created_at and _release does not touch it, so a job the
    network released is handed straight back. Capped because a manually-run
    backfill has to stay interruptible; the client refuses to sleep past
    MAX_SLEEP_BUDGET_S for the same reason.
    """
    if not result.retry_after:
        return None
    return min(result.retry_after, MAX_RECLAIM_WAIT_S)


logger = get_logger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill Slack history")
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=None,
        help="stop after this many conversations (for sampling the rate)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=float(os.environ.get("SLACK_RATE_PER_SECOND", "1.0")),
        help="requests per second; deliberately conservative by default",
    )
    args = parser.parse_args()

    configure_logging("slack-backfill", os.environ.get("LOG_LEVEL", "INFO"))

    base_url = os.environ.get("SLACK_BASE_URL")
    if not base_url:
        raise SystemExit("SLACK_BASE_URL is not set (e.g. https://yourteam.slack.com)")

    xoxc, xoxd = stealth_tokens()
    client = SlackClient(xoxc, xoxd, base_url, rate=args.rate)
    conn = psycopg2.connect(dsn_from_env())
    apply_schema(conn)

    channels = list(client.list_channels())
    upsert_channels(conn, channels)
    logger.info("channels recorded", count=len(channels))

    users = list(client.list_users())
    upsert_users(conn, users)
    logger.info("users recorded", count=len(users))

    queued = seed_jobs(conn, channels)
    logger.info("jobs seeded", new=queued, conversations=len(channels))

    started = time.monotonic()
    done = messages = 0
    while args.max_jobs is None or done < args.max_jobs:
        try:
            result = run_next_job(conn, client)
        except SlackAuthError as exc:
            # Expected end of phase 1, not a crash. The job it was working is
            # already back on the queue; say so plainly and stop.
            logger.error(
                "slack credentials are no longer valid; stopping",
                error=str(exc),
                conversations_done=done,
            )
            return 2
        if result is None:
            logger.info("backfill complete", conversations=done, messages=messages)
            return 0
        wait = reclaim_wait(result)
        if wait is not None:
            logger.warning(
                "waiting before re-claiming the released job",
                channel=result.channel_id,
                asked_s=result.retry_after,
                waiting_s=wait,
            )
            time.sleep(wait)
            continue
        done += 1
        messages += result.seen
        elapsed = time.monotonic() - started
        logger.info(
            "progress",
            conversations=done,
            messages=messages,
            elapsed_s=round(elapsed, 1),
            msgs_per_min=round(messages / max(elapsed, 1) * 60, 1),
        )

    logger.info("stopping at --max-jobs", conversations=done, messages=messages)
    return 0


if __name__ == "__main__":
    sys.exit(main())
