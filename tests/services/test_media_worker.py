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

    stored, failed, _selected = store_pending(clean, s3, "xoxc", "xoxd")

    assert (stored, failed) == (1, 0)
    assert list(s3.objects.values()) == [b"PNGDATA"]
    assert _pending(clean) == 0


def test_a_sign_in_page_is_never_stored_as_if_it_were_the_file(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The failure that would otherwise look like success.

    Slack answers an unauthenticated file request with an HTML sign-in page and
    status 200. Stored, it is indistinguishable from the file having been
    archived -- the row says s3_key, the object exists, and nobody finds out
    until they open it.

    Whether it also stops the run is a separate question, answered by how many
    arrive in a row; what must never happen is writing the page to the bucket.
    """
    _ingest_with_file(clean, "1725379200.000001", _file())
    discover_files(clean)
    monkeypatch.setattr(
        media_worker,
        "download",
        lambda *a, **k: (_ for _ in ()).throw(media_worker.CredentialsRejected("html")),
    )
    s3 = _FakeS3()

    stored, _failed, _selected = store_pending(clean, s3, "xoxc", "xoxd")

    assert stored == 0
    assert s3.objects == {}, "wrote a sign-in page into the bucket"
    assert _pending(clean) == 0, "left it pending, so it will be re-fetched forever"


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

    stored, failed, _selected = store_pending(clean, _FakeS3(), "xoxc", "xoxd")

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

    stored, failed, _selected = store_pending(clean, _FakeS3(), "xoxc", "xoxd")

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

    stored, failed, _selected = store_pending(clean, _FakeS3(), "xoxc", "xoxd")

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

    stored, failed, _selected = store_pending(clean, _FakeS3(), "xoxc", "xoxd")

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


def test_a_batch_of_only_unfetchable_files_is_not_completion(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(0 stored, 0 failed) does not mean there is no work left.

    Files skipped as permanently unfetchable increment neither counter, so a
    batch where every file is dead looks identical to an empty queue. The worker
    treated that as completion and exited -- on the first real run it logged
    "all files stored" with 3,203 files still pending, and the log was the
    reassuring half of the disagreement.
    """
    for i in range(3):
        _ingest_with_file(clean, f"172537920{i}.000001", _file(f"F{i}"))
    discover_files(clean)
    monkeypatch.setattr(
        media_worker, "download", lambda *a, **k: (_ for _ in ()).throw(_http_error(410))
    )

    stored, failed, selected = store_pending(clean, _FakeS3(), "xoxc", "xoxd")

    assert (stored, failed) == (0, 0)
    assert selected == 3, "the only signal that work was attempted"


def test_an_empty_queue_is_completion(clean: psycopg2.extensions.connection) -> None:
    stored, failed, selected = store_pending(clean, _FakeS3(), "xoxc", "xoxd")

    assert (stored, failed, selected) == (0, 0, 0)


def test_an_oversized_file_is_permanent_not_transient(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A size ceiling will not be met by trying again.

    It raised ValueError, which the broad transient clause catches, so one
    oversized file would sit at the head of the fetched_at ordering forever and
    block every file behind it.
    """
    _ingest_with_file(clean, "1725379200.000001", _file())
    discover_files(clean)
    monkeypatch.setattr(
        media_worker,
        "download",
        lambda *a, **k: (_ for _ in ()).throw(media_worker.FileTooLarge("too big")),
    )

    stored, failed, selected = store_pending(clean, _FakeS3(), "xoxc", "xoxd")

    assert (stored, failed, selected) == (0, 0, 1)
    assert _pending(clean) == 0, "an oversized file stayed pending and blocks the queue"


def test_file_too_large_is_not_a_value_error() -> None:
    """Or the transient handler catches it and the fix does nothing."""
    assert not issubclass(media_worker.FileTooLarge, ValueError)


def test_one_sign_in_page_does_not_stop_the_run(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A single HTML response says nothing about the session.

    Observed: 863 files downloaded cleanly and the 864th returned a sign-in
    page, which stopped the whole run. A file whose url_private redirects
    offsite, or one in a channel whose access changed, is a per-file condition.
    """
    for i in range(3):
        _ingest_with_file(clean, f"172537920{i}.000001", _file(f"F{i}"))
    discover_files(clean)
    calls = {"n": 0}

    def _one_bad_file(*_a: Any, **_k: Any) -> bytes:
        calls["n"] += 1
        if calls["n"] == 1:
            raise media_worker.CredentialsRejected("got an HTML page, not the file")
        return b"PNGDATA"

    monkeypatch.setattr(media_worker, "download", _one_bad_file)

    stored, failed, selected = store_pending(clean, _FakeS3(), "xoxc", "xoxd")

    assert stored == 2, "one odd file stopped the other two"
    assert selected == 3
    assert _fetch_error(clean, "F0") is not None and "sign-in page" in _fetch_error(clean, "F0")


def test_a_run_of_sign_in_pages_stops_only_if_the_session_is_really_dead(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """auth.test answers the question; the run of failures only triggers asking."""
    for i in range(8):
        _ingest_with_file(clean, f"17253792{i:02d}.000001", _file(f"F{i}"))
    discover_files(clean)
    monkeypatch.setattr(
        media_worker,
        "download",
        lambda *a, **k: (_ for _ in ()).throw(media_worker.CredentialsRejected("html")),
    )

    with pytest.raises(media_worker.CredentialsRejected):
        store_pending(clean, _FakeS3(), "xoxc", "xoxd", verify_session=lambda: False)


def test_a_run_of_sign_in_pages_continues_when_the_session_is_valid(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Files cluster by channel and age, so a run of awkward ones is ordinary.

    Twice this stopped a healthy run: once after 863 successful downloads, once
    after 2,326, with 24 individually-flagged files and a perfectly good token.
    Inferring session health from file responses does not work; asking does.
    """
    for i in range(8):
        _ingest_with_file(clean, f"17253792{i:02d}.000001", _file(f"F{i}"))
    discover_files(clean)
    monkeypatch.setattr(
        media_worker,
        "download",
        lambda *a, **k: (_ for _ in ()).throw(media_worker.CredentialsRejected("html")),
    )

    stored, failed, selected = store_pending(
        clean, _FakeS3(), "xoxc", "xoxd", verify_session=lambda: True
    )

    assert selected == 8, "stopped a run whose session was confirmed healthy"
    assert stored == 0 and failed == 0
    assert _pending(clean) == 0, "all eight should be marked, not left to retry forever"


def test_the_session_is_only_checked_after_a_run_of_failures(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """auth.test costs a rate-limited request; one odd file must not spend it."""
    _ingest_with_file(clean, "1725379200.000001", _file())
    discover_files(clean)
    monkeypatch.setattr(
        media_worker,
        "download",
        lambda *a, **k: (_ for _ in ()).throw(media_worker.CredentialsRejected("html")),
    )
    checks = {"n": 0}

    def _count() -> bool:
        checks["n"] += 1
        return True

    store_pending(clean, _FakeS3(), "xoxc", "xoxd", verify_session=_count)

    assert checks["n"] == 0, "checked the session for a single odd file"


def test_a_success_resets_the_credential_failure_run(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scattered odd files must not accumulate into a false 'session dead'."""
    for i in range(9):
        _ingest_with_file(clean, f"17253792{i:02d}.000001", _file(f"F{i}"))
    discover_files(clean)
    calls = {"n": 0}

    def _alternating(*_a: Any, **_k: Any) -> bytes:
        calls["n"] += 1
        if calls["n"] % 2:
            raise media_worker.CredentialsRejected("html")
        return b"PNGDATA"

    monkeypatch.setattr(media_worker, "download", _alternating)

    stored, _failed, _selected = store_pending(clean, _FakeS3(), "xoxc", "xoxd")

    assert stored == 4, "alternating failures were mistaken for a dead session"


def test_a_permanently_failing_file_is_eventually_given_up_on(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ "Retryable" without a bound is a loop, not a retry policy.

    Observed: 16 files answering HTTP 400 accumulated 867 download attempts
    across restarts, sitting at the head of the queue the whole time.
    """
    _ingest_with_file(clean, "1725379200.000001", _file())
    discover_files(clean)
    monkeypatch.setattr(
        media_worker,
        "download",
        lambda *a, **k: (_ for _ in ()).throw(urllib.error.URLError("reset")),
    )

    for _ in range(media_worker.MAX_ATTEMPTS):
        store_pending(clean, _FakeS3(), "xoxc", "xoxd")

    assert _pending(clean) == 0, "retried past its budget"
    error = _fetch_error(clean, "F1")
    assert error is not None and "failed" in error


def test_a_transient_failure_moves_out_of_the_queue_head(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Untried files must be reached before retries.

    Ordering by fetched_at alone let a handful of persistent failures starve
    everything behind them.
    """
    _ingest_with_file(clean, "1725379200.000001", _file("FBAD"))
    _ingest_with_file(clean, "1725379200.000002", _file("FGOOD"))
    discover_files(clean)
    monkeypatch.setattr(
        media_worker,
        "download",
        lambda *a, **k: (_ for _ in ()).throw(urllib.error.URLError("reset")),
    )
    store_pending(clean, _FakeS3(), "xoxc", "xoxd", limit=1)

    with clean.cursor() as cur:
        cur.execute(
            "SELECT file_id FROM slack_files WHERE s3_key IS NULL AND fetch_error IS NULL "
            "ORDER BY last_attempt_at NULLS FIRST, fetched_at LIMIT 1"
        )
        row = cur.fetchone()

    assert row is not None and row[0] == "FGOOD", "the failing file still blocks the queue"


@pytest.mark.parametrize("code", [400, 402, 405, 422])
def test_other_client_errors_are_permanent_too(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch, code: int
) -> None:
    """Any 4xx but 429 means the request is the problem; repeating it cannot help."""
    _ingest_with_file(clean, "1725379200.000001", _file())
    discover_files(clean)
    monkeypatch.setattr(
        media_worker, "download", lambda *a, **k: (_ for _ in ()).throw(_http_error(code))
    )

    store_pending(clean, _FakeS3(), "xoxc", "xoxd")

    assert _fetch_error(clean, "F1") == f"HTTP {code}"


def test_rate_limiting_is_not_treated_as_permanent() -> None:
    """429 is the one 4xx that a retry fixes."""
    assert 429 not in media_worker.PERMANENT_STATUSES
    assert 400 in media_worker.PERMANENT_STATUSES
