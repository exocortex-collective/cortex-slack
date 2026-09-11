"""Shared fixtures.

Live tests take a real Postgres from CORTEX_TEST_DSN rather than a mock. The
defects this suite exists to catch -- a watermark that advances after a failure,
an ON CONFLICT that fires when it should not, a dedup_key that does not dedup --
are all invisible to a mock, which hands back whatever the code asked for.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator

import psycopg2
import pytest
from cortex_utils.queue import ensure_queue_schema

from cortex_slack.config import schema_sql


@pytest.fixture(scope="session")
def dsn() -> str:
    """The live-Postgres DSN, or a skip -- unless CORTEX_REQUIRE_LIVE forbids it.

    `-p no:skipping` does NOT make this fail. That flag disables the plugin
    handling the `skip`/`skipif` MARKERS; a runtime `pytest.skip()` raised
    inside a fixture goes through the outcome machinery untouched, and the run
    still exits 0. Measured: with the flag and no DSN, "1 skipped", exit 0 --
    a guard against silent skipping that silently skipped.

    So the guard is an explicit contract instead of a plugin trick. CI sets
    CORTEX_REQUIRE_LIVE, and then a missing DSN is a failure rather than a skip.
    """
    value = os.environ.get("CORTEX_TEST_DSN")
    if value:
        _refuse_non_test_database(value)
        return value
    if os.environ.get("CORTEX_REQUIRE_LIVE"):
        pytest.fail(
            "CORTEX_TEST_DSN is unset but CORTEX_REQUIRE_LIVE is set: the live "
            "tests are required here and must not be skipped"
        )
    pytest.skip("CORTEX_TEST_DSN is not set")


# Exact names that are disposable by definition: `postgres` is the CI service
# container's default database, created and destroyed per job.
_DISPOSABLE_DB_NAMES = frozenset({"postgres"})
# A dbname is also disposable if one of its _/- separated segments is this.
_TEST_SEGMENT = "test"


def _refuse_non_test_database(dsn: str) -> None:
    """Refuse a DSN whose dbname is not obviously disposable.

    The `clean` fixture runs `DELETE FROM queue WHERE queue_name =
    'slack_parse'` against the SHARED queue table -- the one gmail-sync,
    parse-worker, attachment-worker and triage-worker all use. Pointed at the
    real local stack it would delete live jobs, and the local stack listens on
    the same host and port as a scratch database, so the two differ by one word
    in an env var.

    This is not hypothetical: it happened during development, and was "fixed"
    by a line of documentation, which is not a guard.
    """
    dbname = ""
    for part in dsn.split():
        key, _, val = part.partition("=")
        if key == "dbname":
            dbname = val
    if not dbname:
        raise RuntimeError(f"CORTEX_TEST_DSN names no dbname: {dsn!r}")
    if not _is_disposable(dbname):
        raise RuntimeError(
            f"refusing to run destructive fixtures against dbname={dbname!r}; "
            f"CORTEX_TEST_DSN must name a disposable database -- exactly "
            f"{sorted(_DISPOSABLE_DB_NAMES)}, or one with a '{_TEST_SEGMENT}' "
            f"segment such as 'cortex_test'"
        )


def _is_disposable(dbname: str) -> bool:
    """Segment match, not substring.

    `"test" in dbname` was the first version of this, and it admits `protest`,
    `greatest` and `attestation` -- a guard whose whole reason for existing is
    that production and scratch "differ by one word in an env var", defeated by
    a word that merely contains the marker. `postgres` as a substring marker was
    worse still: it let `postgres_prod` through.
    """
    lowered = dbname.lower()
    if lowered in _DISPOSABLE_DB_NAMES:
        return True
    return _TEST_SEGMENT in re.split(r"[_-]", lowered)


@pytest.fixture
def conn(dsn: str) -> Iterator[psycopg2.extensions.connection]:
    """A connection that always rolls back.

    Every live test shares one database, so a test that commits leaks state into
    whichever test runs next -- and the resulting failure lands on the innocent
    one. Schema setup that genuinely needs committing does it explicitly.
    """
    connection = psycopg2.connect(dsn)
    try:
        yield connection
    finally:
        connection.rollback()
        connection.close()


@pytest.fixture
def schema(conn: psycopg2.extensions.connection) -> psycopg2.extensions.connection:
    """A connection with the queue schema and this service's tables in place.

    Committed, not rolled back: `ensure_queue_schema` creates partitions, which
    needs its own transaction, and every test in the session shares the result.
    Test DATA still rolls back -- only the DDL persists.
    """
    ensure_queue_schema(conn)
    with conn.cursor() as cur:
        # The same accessor the service uses at boot. Rebuilding a parallel
        # path here is how the packaging bug survived: the tests proved a file
        # the deployed code would never read.
        cur.execute(schema_sql())
    conn.commit()
    return conn


@pytest.fixture
def clean(schema: psycopg2.extensions.connection) -> psycopg2.extensions.connection:
    """Schema in place and this service's tables empty.

    The raw table cascades to every derived table, so one TRUNCATE clears the
    lot. The shared `queue` table is filtered by queue_name rather than
    truncated -- other Cortex services use it, and a test that truncates it
    would delete their work.
    """
    with schema.cursor() as cur:
        # Every table this service owns, not only the one the derived rows
        # cascade from: slack_backfill_jobs and slack_sync_state are written
        # directly, so leaving them would leak a previous test's jobs and
        # watermarks into the next one -- and the failure lands on the innocent
        # test, which is how this cost an afternoon the first time.
        cur.execute("TRUNCATE slack_messages_raw CASCADE")
        cur.execute("TRUNCATE slack_backfill_jobs")
        cur.execute("TRUNCATE slack_sync_state")
        cur.execute("DELETE FROM queue WHERE queue_name = 'slack_parse'")
    schema.commit()
    return schema
