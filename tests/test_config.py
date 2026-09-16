"""Tests for configuration loading."""

from __future__ import annotations

from cortex_slack.config import dsn_from_env, schema_sql


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


def test_schema_sql_is_packaged_and_non_empty() -> None:
    """Read as package data, not from a repo-relative path.

    An installed container has no repo; cortex-granola shipped a wheel whose
    schema could not be found and only discovered it at boot.
    """
    sql = schema_sql()

    assert "CREATE TABLE IF NOT EXISTS slack_messages_raw" in sql
    assert "CREATE TABLE IF NOT EXISTS slack_sync_state" in sql
