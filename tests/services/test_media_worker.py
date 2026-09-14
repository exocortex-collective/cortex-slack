"""Tests for the media worker.

The file URLs this fetches die with the Slack session and the files themselves
get deleted -- 19 of 4,163 already have. The failure worth guarding is the quiet
one: storing something that is not the file.
"""

from __future__ import annotations

import urllib.error
from typing import Any

import psycopg2
import pytest

from cortex_slack.services import media_worker
from cortex_slack.services.media_worker import (
    CredentialsRejected,
    discover_files,
    s3_key_for,
    store_pending,
)


class _FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_bytes(self, key: str, data: bytes) -> None:
        self.objects[key] = data


def _ingest_with_file(conn: psycopg2.extensions.connection, ts: str, file: dict[str, Any]) -> None:
    from cortex_slack.ingest import ingest_message

    ingest_message(
        conn,
        "C1",
        {"type": "message", "ts": ts, "user": "U1", "text": "see this", "files": [file]},
        source="backfill",
        enqueue_parse=False,
    )
    conn.commit()


def _file(file_id: str = "F1", **extra: Any) -> dict[str, Any]:
    return {
        "id": file_id,
        "name": "diagram.png",
        "mimetype": "image/png",
        "size": 1234,
        "url_private_download": "https://files.slack.com/f/F1/diagram.png",
        **extra,
    }


def _pending(conn: psycopg2.extensions.connection) -> int:
    """Files still worth attempting: not stored, and not permanently failed.

    Must match the worker's own _PENDING predicate. It did not, once: this
    counted only `s3_key IS NULL`, so a file correctly marked unfetchable still
    read as outstanding and the test disagreed with the code it was checking.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM slack_files WHERE s3_key IS NULL AND fetch_error IS NULL")
        row = cur.fetchone()
        return 0 if row is None else int(row[0])


def test_files_are_discovered_from_the_raw_archive(
    clean: psycopg2.extensions.connection,
) -> None:
    _ingest_with_file(clean, "1725379200.000001", _file())

    assert discover_files(clean) == 1
    assert _pending(clean) == 1


def test_discovery_is_idempotent(clean: psycopg2.extensions.connection) -> None:
    _ingest_with_file(clean, "1725379200.000001", _file())
    discover_files(clean)

    assert discover_files(clean) == 0, "re-queued a file already known"


def test_a_stored_file_is_recorded_with_its_key(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ingest_with_file(clean, "1725379200.000001", _file())
    discover_files(clean)
    monkeypatch.setattr(media_worker, "download", lambda *a, **k: b"PNGDATA")
    s3 = _FakeS3()

    stored, failed = store_pending(clean, s3, "xoxc", "xoxd")

    assert (stored, failed) == (1, 0)
    assert list(s3.objects.values()) == [b"PNGDATA"]
    assert _pending(clean) == 0


def test_a_sign_in_page_stops_everything_rather_than_being_stored(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The failure that would otherwise look like success.

    Slack answers an unauthenticated file request with an HTML sign-in page and
    status 200. Stored, it is indistinguishable from the file having been
    archived -- so it must stop the pass, not count as one more failure among
    thousands.
    """
    for i in range(3):
        _ingest_with_file(clean, f"172537920{i}.000001", _file(f"F{i}"))
    discover_files(clean)

    def _sign_in_page(*_a: Any, **_k: Any) -> bytes:
        raise CredentialsRejected("got an HTML page, not the file")

    monkeypatch.setattr(media_worker, "download", _sign_in_page)

    with pytest.raises(CredentialsRejected):
        store_pending(clean, _FakeS3(), "xoxc", "xoxd")

    assert _pending(clean) == 3, "marked files done despite storing nothing"


def test_credentials_rejected_is_not_an_oserror() -> None:
    """It must not be catchable by the transient-network handler.

    PermissionError was the first choice and is an OSError subclass, so the
    broad `except OSError` for retryable faults swallowed it and the
    stop-everything branch below was unreachable.
    """
    assert not issubclass(CredentialsRejected, OSError)


def test_a_transient_failure_leaves_the_file_pending(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ingest_with_file(clean, "1725379200.000001", _file())
    discover_files(clean)

    def _boom(*_a: Any, **_k: Any) -> bytes:
        raise urllib.error.URLError("connection reset")

    monkeypatch.setattr(media_worker, "download", _boom)

    stored, failed = store_pending(clean, _FakeS3(), "xoxc", "xoxd")

    assert (stored, failed) == (0, 1)
    assert _pending(clean) == 1, "a retryable failure must stay retryable"


def test_a_tombstoned_file_is_never_fetched(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Slack already deleted it. Retrying spends the rate budget on certain
    failure, and the metadata row stays as the only evidence it existed."""
    _ingest_with_file(clean, "1725379200.000001", _file(mode="tombstone"))
    discover_files(clean)
    monkeypatch.setattr(
        media_worker, "download", lambda *a, **k: pytest.fail("fetched a tombstone")
    )

    stored, failed = store_pending(clean, _FakeS3(), "xoxc", "xoxd")

    assert (stored, failed) == (0, 0)


def test_keys_are_sharded_not_flat() -> None:
    """Thousands of objects under one prefix makes listing slow on every S3."""
    key = s3_key_for("F01ABCDEF", "diagram.png")

    assert key.startswith("slack/F0/F01ABCDEF/")
    assert "/" not in s3_key_for("F1", "a/b/c.png").rsplit("/", 1)[1]


def _fetch_error(conn: psycopg2.extensions.connection, file_id: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT fetch_error FROM slack_files WHERE file_id = %s", (file_id,))
        row = cur.fetchone()
        return None if row is None else row[0]


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://x", code, "nope", {}, None)  # type: ignore[arg-type]


@pytest.mark.parametrize("code", [401, 403, 404, 410])
def test_a_permanently_dead_file_stops_being_pending(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch, code: int
) -> None:
    """Retrying these spends a rate-limited request on a guaranteed failure.

    Measured on the real archive: 40 of the first 800 attempts were 404 or 410,
    and each would have been re-requested on every pass forever.
    """
    _ingest_with_file(clean, "1725379200.000001", _file())
    discover_files(clean)
    monkeypatch.setattr(
        media_worker, "download", lambda *a, **k: (_ for _ in ()).throw(_http_error(code))
    )

    stored, failed = store_pending(clean, _FakeS3(), "xoxc", "xoxd")

    assert (stored, failed) == (0, 0), "counted a permanent failure as retryable"
    assert _fetch_error(clean, "F1") == f"HTTP {code}"
    assert _pending(clean) == 0


@pytest.mark.parametrize("code", [500, 502, 503])
def test_a_transient_http_failure_stays_pending(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch, code: int
) -> None:
    _ingest_with_file(clean, "1725379200.000001", _file())
    discover_files(clean)
    monkeypatch.setattr(
        media_worker, "download", lambda *a, **k: (_ for _ in ()).throw(_http_error(code))
    )

    stored, failed = store_pending(clean, _FakeS3(), "xoxc", "xoxd")

    assert (stored, failed) == (0, 1)
    assert _fetch_error(clean, "F1") is None
    assert _pending(clean) == 1, "a retryable failure must stay retryable"


def test_marking_a_file_unfetchable_does_not_rewrite_what_slack_said(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """raw is the record of the API response and must stay that way.

    An earlier version set raw->>'mode' to 'tombstone', which corrupted the
    archive and made "Slack deleted this" indistinguishable from "we could not
    download it".
    """
    _ingest_with_file(clean, "1725379200.000001", _file(mode="hosted"))
    discover_files(clean)
    monkeypatch.setattr(
        media_worker, "download", lambda *a, **k: (_ for _ in ()).throw(_http_error(410))
    )

    store_pending(clean, _FakeS3(), "xoxc", "xoxd")

    with clean.cursor() as cur:
        cur.execute("SELECT raw->>'mode' FROM slack_files WHERE file_id = 'F1'")
        row = cur.fetchone()
    assert row is not None and row[0] == "hosted", "rewrote the archived payload"
