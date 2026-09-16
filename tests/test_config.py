"""Tests for configuration loading."""

from __future__ import annotations

from typing import Any

import psycopg2
import pytest

from cortex_slack import config
from cortex_slack.config import MIGRATIONS_TABLE, apply_schema, dsn_from_env, migrations


def test_dsn_is_a_connectable_string() -> None:
    """PostgresConfig.dsn is a property, not a method.

    Calling it returned a str and then tried to call that, which mypy could not
    catch: cortex-utils ships no py.typed marker, so the strict-mode override
    that lets it import at all also blinds the checker to its shape. The only
    thing that catches this is exercising it.
    """
    dsn = dsn_from_env()

    assert isinstance(dsn, str)
    assert "dbname=" in dsn
    assert "host=" in dsn


def test_migrations_are_packaged_in_filename_order() -> None:
    """Read as package data, not from a repo-relative path.

    An installed container has no repo; cortex-granola shipped a wheel whose
    schema could not be found and only discovered it at boot.
    """
    found = migrations()
    names = [name for name, _ in found]

    assert names == sorted(names) and names[0].startswith("001_")
    assert "CREATE TABLE IF NOT EXISTS slack_messages_raw" in found[0][1]
    assert any("CREATE TABLE IF NOT EXISTS slack_sync_state" in sql for _, sql in found)


def _recorded(conn: psycopg2.extensions.connection) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT name FROM {MIGRATIONS_TABLE}")
        return {row[0] for row in cur.fetchall()}


def test_a_migration_runs_once_and_a_new_one_runs_on_the_next_boot(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every earlier boot replayed all the DDL, exclusive locks included.

    A probe migration that is not idempotent proves the file runs exactly once
    across two boots, and a file added afterwards runs on the boot after that
    and no earlier.
    """
    real = migrations()
    probe = (
        "900_probe.sql",
        "CREATE TABLE slack_probe (n INT); INSERT INTO slack_probe VALUES (1);",
    )
    later = ("901_probe_two.sql", "INSERT INTO slack_probe VALUES (2);")

    def _drop_probe() -> None:
        # rollback FIRST: a failed apply_schema leaves the connection in an
        # aborted transaction, where the DROP itself raises and the probe table
        # -- committed by the first boot -- survives to fail every later run.
        # Reproduced: a mutation check left slack_probe behind and the suite
        # was red until someone dropped it by hand. Run before as well as
        # after, so a poisoned database cannot poison this run.
        clean.rollback()
        with clean.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS slack_probe")
            cur.execute(
                f"DELETE FROM {MIGRATIONS_TABLE} WHERE name IN (%s, %s)", (probe[0], later[0])
            )
        clean.commit()

    _drop_probe()
    try:
        monkeypatch.setattr(config, "migrations", lambda: [*real, probe])
        apply_schema(clean)
        apply_schema(clean)  # a second boot: CREATE TABLE without IF NOT EXISTS would raise

        monkeypatch.setattr(config, "migrations", lambda: [*real, probe, later])
        apply_schema(clean)

        with clean.cursor() as cur:
            cur.execute("SELECT n FROM slack_probe ORDER BY n")
            rows = [r[0] for r in cur.fetchall()]
        assert rows == [1, 2], f"a migration ran more or less than once: {rows}"
        assert {probe[0], later[0]} <= _recorded(clean)
        assert {name for name, _ in real} <= _recorded(clean), "the real files were not recorded"
    finally:
        _drop_probe()


def test_a_recorded_migration_is_not_executed_again(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The point of the ledger: a boot against a current schema runs no DDL.

    The first production redeploy deadlocked apply_schema against the parse
    worker's ordinary query, because no-op ALTERs still take ACCESS EXCLUSIVE.
    A recorded file whose SQL cannot succeed proves it is skipped by name, not
    re-run and tolerated.
    """
    poison = ("950_recorded_elsewhere.sql", "SELECT 1/0")
    with clean.cursor() as cur:
        # ON CONFLICT: a previous failed run leaves the row behind, and this
        # test must then fail at its assertion, not at its setup.
        cur.execute(
            f"INSERT INTO {MIGRATIONS_TABLE} (name) VALUES (%s) ON CONFLICT DO NOTHING",
            (poison[0],),
        )
    clean.commit()
    monkeypatch.setattr(config, "migrations", lambda: [*migrations(), poison])
    try:
        # The assertion is "did not raise": executing the recorded file raises
        # DivisionByZero, and nothing else in apply_schema can.
        apply_schema(clean)
    finally:
        clean.rollback()  # a failure leaves the transaction aborted; end it first
        with clean.cursor() as cur:
            cur.execute(f"DELETE FROM {MIGRATIONS_TABLE} WHERE name = %s", (poison[0],))
        clean.commit()


def test_a_deadlock_at_boot_is_retried_not_fatal(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Postgres kills one side of a deadlock; the other finishes.

    Before this the killed side was the whole service, and the restart policy
    brought it back -- the crash-loop recovery this codebase disavows.
    """
    real_apply = config._apply_pending
    calls = {"n": 0}

    def _deadlock_once(conn: Any) -> list[str]:
        calls["n"] += 1
        if calls["n"] == 1:
            # Leave the connection the way a real deadlock does: inside an
            # aborted transaction. Raising from a clean connection would let a
            # retry loop with no rollback pass this test and then fail in
            # production with InFailedSqlTransaction -- a slower crash.
            with conn.cursor() as cur, pytest.raises(psycopg2.errors.DivisionByZero):
                cur.execute("SELECT 1/0")
            raise psycopg2.errors.DeadlockDetected("deadlock detected")
        return real_apply(conn)

    monkeypatch.setattr(config, "_apply_pending", _deadlock_once)
    monkeypatch.setattr(config.time, "sleep", lambda _s: None)

    apply_schema(clean)

    assert calls["n"] == 2


def test_a_deadlock_that_never_clears_is_raised(
    clean: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _always(conn: Any) -> list[str]:
        raise psycopg2.errors.DeadlockDetected("deadlock detected")

    monkeypatch.setattr(config, "_apply_pending", _always)
    monkeypatch.setattr(config.time, "sleep", lambda _s: None)

    with pytest.raises(psycopg2.errors.DeadlockDetected):
        apply_schema(clean)
