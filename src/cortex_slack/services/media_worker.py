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
from typing import Any

import psycopg2
from cortex_utils.logging import configure_logging, get_logger
from psycopg2.extras import Json

from cortex_slack.config import apply_schema, dsn_from_env, stealth_tokens
from cortex_slack.storage import S3Client

logger = get_logger(__name__)

BATCH_SIZE = 50
MAX_BYTES = 200 * 1024 * 1024

# Files Slack has already deleted. The metadata row is kept -- it is the only
# remaining evidence the file existed -- but there is nothing to fetch, and
# retrying forever would spend the rate budget on certain failure.
TOMBSTONE_MODES = frozenset({"tombstone"})


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
SELECT file_id, raw
  FROM slack_files
 WHERE s3_key IS NULL
   AND raw->>'mode' IS DISTINCT FROM 'tombstone'
 ORDER BY fetched_at
 LIMIT %s
"""


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
        raise ValueError(f"file exceeds {MAX_BYTES} bytes")
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
) -> tuple[int, int]:
    """Download and store pending files. Returns (stored, failed)."""
    with conn.cursor() as cur:
        cur.execute(_PENDING, (limit,))
        rows = cur.fetchall()

    stored = failed = 0
    for file_id, raw in rows:
        url = raw.get("url_private_download") or raw.get("url_private")
        if not url:
            # No URL at all: nothing to fetch, ever. Left pending would make it
            # a permanent resident of the work queue.
            _mark_unavailable(conn, file_id, "no download url")
            continue
        try:
            data = download(url, xoxc, xoxd)
        except CredentialsRejected as exc:
            # MUST precede the broad handler. This was a PermissionError, which
            # is an OSError subclass, so the clause below caught it first and the
            # stop-everything branch was unreachable -- a dead credential would
            # have been logged as an ordinary failure while the loop ground
            # through four thousand files that could never succeed.
            logger.error("credentials rejected; stopping", file_id=file_id, error=str(exc))
            raise
        except (urllib.error.URLError, OSError, ValueError) as exc:
            failed += 1
            logger.warning("file download failed", file_id=file_id, error=str(exc))
            continue

        key = s3_key_for(file_id, raw.get("name"))
        s3.put_bytes(key, data)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE slack_files SET s3_key = %s, stored_at = NOW() WHERE file_id = %s",
                (key, file_id),
            )
        conn.commit()
        stored += 1
    return stored, failed


def _mark_unavailable(conn: psycopg2.extensions.connection, file_id: str, why: str) -> None:
    """Record that this file can never be fetched, so it stops being pending."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE slack_files SET raw = jsonb_set(raw, '{mode}', '\"tombstone\"'), "
            "stored_at = NOW() WHERE file_id = %s",
            (file_id,),
        )
    conn.commit()
    logger.info("file unavailable", file_id=file_id, reason=why)


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
    s3 = S3Client(
        endpoint_url=os.environ.get("S3_ENDPOINT"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", ""),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", ""),
        bucket_name=os.environ.get("S3_BUCKET", "cortex-slack-files"),
    )

    found = discover_files(conn)
    logger.info("files discovered", new=found)

    total = 0
    while not stop["requested"]:
        stored, failed = store_pending(conn, s3, xoxc, xoxd)
        if stored == 0 and failed == 0:
            logger.info("all files stored", total=total)
            return 0
        total += stored
        logger.info("stored", batch=stored, failed=failed, total=total)
        time.sleep(1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
