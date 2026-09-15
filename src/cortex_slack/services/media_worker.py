"""Pull Slack file attachments into object storage.

Time-sensitive in a way the rest of this service is not. Slack file URLs are
authenticated against the session that fetched them, and files get deleted:
19 of the 4,163 references already in this archive are tombstones, and nothing
can recover those. Every day this does not run, some fraction of the remaining
4,044 becomes permanently unavailable.

Query-driven like the parse worker, for the same reason -- a slack_files row
with a null s3_key IS the record that a download is outstanding.

Discovery and download are separate passes. Discovery reads the raw archive and
is cheap, local, and safe to re-run; download talks to Slack and is rate-limited
and fallible. Keeping them apart means a credential problem stops the fetching
without also stopping the archive from learning which files exist -- and the
metadata alone is worth having, since it is the record that a file was there
even when the bytes are gone.
"""

from __future__ import annotations

import os
import signal
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

import psycopg2
from cortex_utils.logging import configure_logging, get_logger
from psycopg2.extras import Json

from cortex_slack.config import apply_schema, dsn_from_env, stealth_tokens
from cortex_slack.slack.api import SlackClient
from cortex_slack.storage import S3Client

logger = get_logger(__name__)

BATCH_SIZE = 50
MAX_BYTES = 200 * 1024 * 1024

# Files Slack has already deleted. The metadata row is kept -- it is the only
# remaining evidence the file existed -- but there is nothing to fetch, and
# retrying forever would spend the rate budget on certain failure.
TOMBSTONE_MODES = frozenset({"tombstone"})


class FileTooLarge(RuntimeError):
    """The file exceeds the size ceiling.

    Deliberately not a ValueError, for the same reason CredentialsRejected is
    not a PermissionError: the broad transient-failure clause catches ValueError,
    so raising one here classified a permanent condition as retryable and a
    single oversized file would sit at the head of the queue forever, blocking
    every file behind it.
    """


class CredentialsRejected(RuntimeError):
    """Slack served a sign-in page instead of the file.

    Deliberately NOT a PermissionError. That is an OSError subclass, and the
    download path catches OSError broadly for transient network faults -- so a
    PermissionError would be swallowed by the clause meant for retryable errors
    and the credential failure would never stop anything.
    """


_DISCOVER = """
SELECT channel_id, ts, raw->'files' AS files
  FROM slack_messages_raw
 WHERE raw ? 'files'
   AND (channel_id, ts) > (%s, %s)
 ORDER BY channel_id, ts
 LIMIT %s
"""

_PENDING = """
SELECT file_id, raw, attempts
  FROM slack_files
 WHERE s3_key IS NULL
   AND fetch_error IS NULL
   AND raw->>'mode' IS DISTINCT FROM 'tombstone'
 -- Untried files first. Ordering by fetched_at alone let a handful of
 -- persistently failing files sit at the head of the queue and starve
 -- everything behind them.
 ORDER BY last_attempt_at NULLS FIRST, fetched_at
 LIMIT %s
"""

# HTTP statuses that will never succeed on a retry. 404 and 410 mean the file is
# gone from Slack; 401 and 403 mean this session cannot have it, which for a
# session-scoped credential is not going to change by asking again. Everything
# else -- timeouts, 5xx, connection resets -- is transient and stays pending.
# Client-error statuses that will never succeed on a retry. Any 4xx except 429
# qualifies: the request itself is the problem, and repeating it unchanged
# cannot fix that. 400 was the omission that mattered -- 16 files answering it
# accumulated 867 attempts before this was noticed.
PERMANENT_STATUSES = frozenset(range(400, 429)) | frozenset(range(430, 500))

# How many times a transient failure is retried before the file is given up on.
# "Retryable" without a bound is not a retry policy, it is a loop.
MAX_ATTEMPTS = 5

# How long to wait when caught up before looking for new work, and how often to
# re-scan the archive for newly-arrived files.
#
# Being caught up is not a reason to exit. Under `restart: unless-stopped` a
# worker that returns when idle is restarted immediately, finds nothing, and
# returns again -- four restarts in two minutes, each logging "all files
# stored". That is the same fault cortex-gmail-sync had, where a recoverable
# condition was handled by dying and letting the restart policy cope.
IDLE_SECONDS = 60.0
DISCOVERY_INTERVAL_SECONDS = 600.0
STOP_POLL_SECONDS = 0.5

# How many files in a row must answer with a sign-in page before the session is
# CHECKED. Not before it is presumed dead -- inferring session health from file
# responses does not work, and two versions of this tried.
#
# Observed, in order: one sign-in page stopped a run in which 863 files had just
# succeeded; then a threshold of five stopped a run in which 2,326 had. Files are
# ordered by discovery, so they cluster by channel and by age, and a run of
# awkward ones is ordinary rather than diagnostic -- 24 files were individually
# flagged across that run and the session was fine throughout.
#
# So this is a trigger to go and ask. auth.test answers the question directly
# instead of guessing from a proxy.
CREDENTIAL_FAILURES_BEFORE_CHECKING = 5


def discover_files(conn: psycopg2.extensions.connection, limit: int = 500) -> int:
    """Record every file referenced by the raw archive. Returns NEW files found.

    Refreshes the payload of a file not yet stored, so a later tombstone is
    picked up, but does not count that as a discovery.
    """
    written = 0
    after = ("", "")
    while True:
        with conn.cursor() as cur:
            cur.execute(_DISCOVER, (after[0], after[1], limit))
            rows = cur.fetchall()
        if not rows:
            return written

        with conn.cursor() as cur:
            for channel_id, ts, files in rows:
                for file in files or []:
                    if not isinstance(file, dict) or not file.get("id"):
                        continue
                    cur.execute(
                        """
                        INSERT INTO slack_files
                            (file_id, channel_id, ts, name, title, mimetype, size_bytes, raw)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (file_id) DO UPDATE
                           SET raw = EXCLUDED.raw
                         WHERE slack_files.s3_key IS NULL
                        RETURNING (xmax = 0) AS inserted
                        """,
                        (
                            file["id"],
                            channel_id,
                            ts,
                            file.get("name"),
                            file.get("title"),
                            file.get("mimetype"),
                            _size(file),
                            Json(file),
                        ),
                    )
                    # xmax = 0 distinguishes an insert from a conflict-update.
                    # rowcount counts both, so re-running discovery over an
                    # unchanged archive reported every known file as new -- and
                    # "0 new files" is the only way to tell a finished pass from
                    # one that is still finding things.
                    row = cur.fetchone()
                    if row is not None and row[0]:
                        written += 1
        conn.commit()
        after = (rows[-1][0], rows[-1][1])


def _size(file: dict[str, Any]) -> int | None:
    raw = file.get("size")
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def s3_key_for(file_id: str, name: str | None) -> str:
    """Sharded by the file id's leading characters.

    Flat keys put thousands of objects in one prefix, which makes listing and
    lifecycle rules slow on every S3 implementation including MinIO. The name is
    appended for human legibility only -- the id alone is the identity.
    """
    safe = (name or "file").replace("/", "_")[:120]
    return f"slack/{file_id[:2]}/{file_id}/{safe}"


def download(url: str, xoxc: str, xoxd: str, timeout: int = 120) -> bytes:
    """Fetch a private Slack file.

    Needs BOTH credentials: the bearer token authorizes the API, and the d
    cookie authorizes the file endpoint, which is a different host path with its
    own auth. Sending only the token returns an HTML sign-in page with status
    200 -- not an error, just the wrong bytes, silently stored as if it were the
    file.
    """
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {xoxc}", "Cookie": f"d={xoxd}"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        content_type = response.headers.get("Content-Type", "")
        data: bytes = response.read(MAX_BYTES + 1)

    if len(data) > MAX_BYTES:
        raise FileTooLarge(f"file exceeds {MAX_BYTES} bytes")
    if content_type.startswith("text/html"):
        # The sign-in page. Storing it would look like success forever.
        raise CredentialsRejected("got an HTML page, not the file")
    return data


def store_pending(
    conn: psycopg2.extensions.connection,
    s3: Any,
    xoxc: str,
    xoxd: str,
    limit: int = BATCH_SIZE,
    verify_session: Callable[[], bool] | None = None,
) -> tuple[int, int, int]:
    """Download and store pending files.

    Returns (stored, failed, selected). `selected` is how many rows the work
    query returned, and it is the ONLY safe termination signal: stored and
    failed are both zero for a batch that was entirely unfetchable, so treating
    (0, 0) as "nothing left to do" ends the run while thousands of files are
    still pending. That is not a hypothetical -- it happened on the first real
    run, which logged "all files stored" with 3,203 outstanding.
    """
    with conn.cursor() as cur:
        cur.execute(_PENDING, (limit,))
        rows = cur.fetchall()

    stored = failed = 0
    consecutive_html = 0
    for file_id, raw, attempts in rows:
        url = raw.get("url_private_download") or raw.get("url_private")
        if not url:
            # No URL at all: nothing to fetch, ever. Left pending would make it
            # a permanent resident of the work queue.
            _mark_unfetchable(conn, file_id, "no download url")
            continue
        try:
            data = download(url, xoxc, xoxd)
        except urllib.error.HTTPError as exc:
            if exc.code in PERMANENT_STATUSES:
                # Retrying this is spending a rate-limited request on a
                # guaranteed failure, every pass, forever.
                _mark_unfetchable(conn, file_id, f"HTTP {exc.code}")
                continue
            failed += 1
            logger.warning("file download failed", file_id=file_id, error=str(exc))
            continue
        except FileTooLarge as exc:
            _mark_unfetchable(conn, file_id, str(exc))
            continue
        except CredentialsRejected as exc:
            # MUST precede the broad handler: this used to be a PermissionError,
            # an OSError subclass, so the clause below caught it first and this
            # branch was unreachable.
            consecutive_html += 1
            if consecutive_html >= CREDENTIAL_FAILURES_BEFORE_CHECKING:
                if verify_session is not None and verify_session():
                    # The session is fine; these files are individually
                    # unreachable. Reset so a later genuine expiry still trips.
                    logger.info(
                        "several sign-in pages, but the session is still valid",
                        consecutive=consecutive_html,
                    )
                    consecutive_html = 0
                else:
                    logger.error(
                        "sign-in pages and auth.test failed; session is dead, stopping",
                        file_id=file_id,
                        consecutive=consecutive_html,
                    )
                    raise
            _mark_unfetchable(conn, file_id, f"sign-in page: {exc}")
            continue
        except (urllib.error.URLError, OSError, ValueError) as exc:
            failed += 1
            _record_attempt(conn, file_id, attempts + 1, str(exc))
            logger.warning(
                "file download failed",
                file_id=file_id,
                attempt=attempts + 1,
                error=str(exc),
            )
            continue

        consecutive_html = 0
        key = s3_key_for(file_id, raw.get("name"))
        s3.put_bytes(key, data)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE slack_files SET s3_key = %s, stored_at = NOW() WHERE file_id = %s",
                (key, file_id),
            )
        conn.commit()
        stored += 1
    return stored, failed, len(rows)


def _record_attempt(
    conn: psycopg2.extensions.connection, file_id: str, attempts: int, why: str
) -> None:
    """Count a transient failure, and give up once the budget is spent.

    Touching last_attempt_at is what moves the row out of the head of the work
    queue, so a failing file stops blocking the ones behind it even before its
    attempts run out.
    """
    if attempts >= MAX_ATTEMPTS:
        _mark_unfetchable(conn, file_id, f"failed {attempts} times, last: {why[:200]}")
        return
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE slack_files SET attempts = %s, last_attempt_at = NOW() WHERE file_id = %s",
            (attempts, file_id),
        )
    conn.commit()


def _mark_unfetchable(conn: psycopg2.extensions.connection, file_id: str, why: str) -> None:
    """Record that this file can never be fetched, so it stops being pending.

    Writes fetch_error rather than rewriting raw. raw is what Slack returned and
    stays that way: an earlier version set raw->>'mode' to 'tombstone', which
    both corrupted the archive and made "Slack deleted this" indistinguishable
    from "we could not download it".
    """
    with conn.cursor() as cur:
        cur.execute("UPDATE slack_files SET fetch_error = %s WHERE file_id = %s", (why, file_id))
    conn.commit()
    logger.info("file unfetchable", file_id=file_id, reason=why)


def main() -> int:
    configure_logging("slack-media-worker", os.environ.get("LOG_LEVEL", "INFO"))
    stop = {"requested": False}

    def _request_stop(signum: int, _frame: Any) -> None:
        stop["requested"] = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _request_stop)

    conn = psycopg2.connect(dsn_from_env())
    apply_schema(conn)
    xoxc, xoxd = stealth_tokens()
    base_url = os.environ.get("SLACK_BASE_URL")
    if not base_url:
        raise SystemExit("SLACK_BASE_URL is not set")
    client = SlackClient(xoxc, xoxd, base_url)
    s3 = S3Client(
        endpoint_url=os.environ.get("S3_ENDPOINT"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", ""),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", ""),
        bucket_name=os.environ.get("S3_BUCKET", "cortex-slack-files"),
    )

    def _session_is_valid() -> bool:
        """Ask Slack directly rather than inferring from file responses."""
        try:
            client._post("auth.test", {})
        except Exception:  # noqa: BLE001 - any failure here means "cannot confirm"
            return False
        return True

    total = 0
    last_discovery = 0.0

    while not stop["requested"]:
        # Re-scan periodically: the poller keeps adding messages, and any of
        # them may carry files. Not every cycle -- it is a scan of the whole raw
        # archive, and there is no index for "has files".
        if time.monotonic() - last_discovery > DISCOVERY_INTERVAL_SECONDS:
            found = discover_files(conn)
            last_discovery = time.monotonic()
            if found:
                logger.info("files discovered", new=found)

        stored, failed, selected = store_pending(
            conn, s3, xoxc, xoxd, verify_session=_session_is_valid
        )
        if selected == 0:
            # Caught up. Wait for more rather than exiting: `selected == 0` is
            # the only honest completion signal, but completion is temporary --
            # the poller is still running.
            _interruptible_sleep(IDLE_SECONDS, stop)
            continue

        total += stored
        logger.info(
            "stored", batch=stored, failed=failed, skipped=selected - stored - failed, total=total
        )
        _interruptible_sleep(1.0, stop)

    logger.info("stopping", total=total)
    return 0


def _interruptible_sleep(seconds: float, stop: dict[str, bool]) -> None:
    """Wait, noticing a stop request.

    time.sleep() is resumed with the remaining time after a signal handler
    returns (PEP 475), so a SIGTERM early in a sixty-second wait would otherwise
    still wait out the rest -- against Docker's ten-second stop grace.
    """
    deadline = time.monotonic() + seconds
    while not stop["requested"]:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(STOP_POLL_SECONDS, remaining))


if __name__ == "__main__":
    sys.exit(main())
