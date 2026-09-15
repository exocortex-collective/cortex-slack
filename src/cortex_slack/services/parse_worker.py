"""Turn raw Slack payloads into structured, tagged rows.

Finds work by query rather than by queue: a raw row with no parsed row IS the
record that parsing is outstanding, so the queue would carry no information the
database does not already have. That choice is what let a 1.5M-message backfill
run without adding a single row to the shared queue the email pipeline claims
from.

Idempotent by construction. Re-parsing a message rewrites the same values from
the same payload, so the worker can be stopped, restarted, or run twice without
coordination -- and a parser bug is fixed by deleting the parsed rows and
letting it run again, with the raw archive untouched.
"""

from __future__ import annotations

import os
import signal
import sys
import time
from typing import Any

import psycopg2
from cortex_utils.logging import configure_logging, get_logger
from psycopg2.extras import Json

from cortex_slack.config import apply_schema, dsn_from_env
from cortex_slack.parse import derive_tags, edited_at, posted_at

logger = get_logger(__name__)

BATCH_SIZE = 2000

# Raw rows with no parsed row, in primary-key order, resuming after a key.
#
# The `(channel_id, ts) > (%s, %s)` clause is the whole point. Without it the
# planner runs a merge anti-join from the start of the table on every batch, so
# each batch re-scans everything already parsed: measured mid-run at 389,500
# rows and 91,680 buffer reads for a single 500-row batch, 362ms and climbing
# linearly. Across 1.5M rows that is quadratic, and the throughput visibly
# decayed (232k/min to 216k/min) while it ran.
#
# With the keyset the index scan starts where the last batch stopped, so a pass
# over the table costs one scan rather than one per batch.
_PENDING = """
SELECT r.channel_id, r.ts, r.raw, r.fetched_at
  FROM slack_messages_raw r
  LEFT JOIN slack_messages m ON m.channel_id = r.channel_id AND m.ts = r.ts
 -- Unparsed, OR parsed from a different raw version than the one held now.
 -- The second half was missing: ingest goes to real trouble to detect a
 -- changed message and return UPDATED, and that outcome stopped at the raw
 -- tier. Measured live -- 11 rows whose raw carried an edited_ts their parsed
 -- row did not, and nothing would ever have revisited them.
 --
 -- Version, not time. "parsed_at < fetched_at" compared two transaction-start
 -- clocks, and the poller's transaction spans a whole channel: a row rewritten
 -- late in it carries a fetched_at from its start, so a parse that ran in
 -- between looked newer than the rewrite and the row read as current forever.
 -- parsed_from is the fetched_at the parser read; a rewrite in any other
 -- transaction changes it, whichever way the clocks fell.
 WHERE (m.ts IS NULL OR m.parsed_from IS DISTINCT FROM r.fetched_at)
   AND (r.channel_id, r.ts) > (%s, %s)
 ORDER BY r.channel_id, r.ts
 LIMIT %s
"""

# Before the first row of a pass. Slack channel ids are uppercase alphanumeric,
# so the empty string sorts before every real key.
START = ("", "")


def parse_batch(
    conn: psycopg2.extensions.connection,
    limit: int = BATCH_SIZE,
    after: tuple[str, str] = START,
) -> tuple[int, tuple[str, str] | None]:
    """Parse up to `limit` unparsed messages after `after`.

    Returns (written, next_key). A next_key of None means this pass reached the
    end of the table; the caller restarts from START, which is what picks up
    rows that arrived behind the cursor.
    """
    with conn.cursor() as cur:
        cur.execute(_PENDING, (after[0], after[1], limit))
        rows = cur.fetchall()

    if not rows:
        # End the transaction the SELECT opened. psycopg2 starts one on the
        # first statement, so returning here without this leaves the connection
        # "idle in transaction" holding ACCESS SHARE on slack_messages -- which
        # blocks any ALTER TABLE forever. Observed: a sibling worker's
        # apply_schema() waited on an AccessExclusiveLock that was never granted
        # because this worker was politely idling with a transaction open.
        conn.rollback()
        return 0, None

    # The last key READ, not the last written: a row that failed to parse must
    # still advance the cursor, or the pass stops dead on it and re-reads the
    # same failing row forever.
    next_key = (rows[-1][0], rows[-1][1])

    written = 0
    with conn.cursor() as cur:
        for channel_id, ts, raw, fetched_at in rows:
            try:
                values = _row_for(channel_id, ts, raw)
            except Exception as exc:  # noqa: BLE001
                # One malformed payload must not stall the other 1.5M. It stays
                # unparsed and will be retried on the next pass, so a fix to the
                # parser picks it up with no bookkeeping.
                logger.warning("message parse failed", channel=channel_id, ts=ts, error=str(exc))
                continue
            cur.execute(
                """
                INSERT INTO slack_messages
                    (channel_id, ts, thread_ts, user_id, subtype, text, blocks,
                     reactions, reply_count, edited_at, posted_at, tags, parsed_from)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (channel_id, ts) DO UPDATE
                   SET thread_ts = EXCLUDED.thread_ts,
                       user_id = EXCLUDED.user_id,
                       subtype = EXCLUDED.subtype,
                       text = EXCLUDED.text,
                       blocks = EXCLUDED.blocks,
                       reactions = EXCLUDED.reactions,
                       reply_count = EXCLUDED.reply_count,
                       edited_at = EXCLUDED.edited_at,
                       posted_at = EXCLUDED.posted_at,
                       tags = EXCLUDED.tags,
                       parsed_from = EXCLUDED.parsed_from,
                       parsed_at = NOW()
                """,
                (*values, fetched_at),
            )
            written += 1
    conn.commit()
    return written, next_key


def _row_for(channel_id: str, ts: str, raw: dict[str, Any]) -> tuple[Any, ...]:
    # A file_comment keeps the person's words in raw.comment.comment and their
    # id in raw.comment.user; the top-level `text` is Slack's wrapper prose
    # ("<@U1> commented on <@U2>'s file") and the top-level `user` is absent on
    # all 69 of them. Reading only the top level stored the wrapper and lost the
    # comment.
    raw_comment = raw.get("comment")
    comment: dict[str, Any] = raw_comment if isinstance(raw_comment, dict) else {}
    is_file_comment = raw.get("subtype") == "file_comment"

    return (
        channel_id,
        ts,
        raw.get("thread_ts"),
        # A bot post carries no user; the column stays null rather than
        # inventing an id, and the `automated` tag is how those are found.
        raw.get("user") or comment.get("user"),
        raw.get("subtype"),
        comment.get("comment") if is_file_comment else raw.get("text"),
        Json(raw.get("blocks")) if raw.get("blocks") is not None else None,
        Json(raw.get("reactions")) if raw.get("reactions") is not None else None,
        int(raw.get("reply_count") or 0),
        edited_at(raw),
        posted_at(ts),
        derive_tags(raw),
    )


def main() -> int:
    configure_logging("slack-parse-worker", os.environ.get("LOG_LEVEL", "INFO"))
    stop = {"requested": False}

    def _request_stop(_signum: int, _frame: Any) -> None:
        stop["requested"] = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _request_stop)

    conn = psycopg2.connect(dsn_from_env())
    apply_schema(conn)
    idle_sleep = float(os.environ.get("SLACK_PARSE_IDLE_S", "30"))
    total = 0
    started = time.monotonic()
    after = START

    while not stop["requested"]:
        written, next_key = parse_batch(conn, after=after)
        # None means end of table: restart rather than idle there, because rows
        # arrive behind the cursor constantly -- the backfill walks each channel
        # newest-first and the poller writes into the middle.
        after = START if next_key is None else next_key

        if written == 0:
            # Caught up. Sleep in slices so a stop is noticed promptly rather
            # than after the full interval.
            waited = 0.0
            while waited < idle_sleep and not stop["requested"]:
                time.sleep(min(0.5, idle_sleep - waited))
                waited += 0.5
            continue
        total += written
        elapsed = time.monotonic() - started
        logger.info(
            "parsed", batch=written, total=total, msgs_per_min=round(total / max(elapsed, 1) * 60)
        )

    logger.info("stopping", total=total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
