"""Configuration and schema bootstrap.

Env reading is local; anything shared with the rest of the family comes from
cortex_utils, per CLAUDE.md's "shared code goes in cortex-utils".
"""

from __future__ import annotations

import os
import re
import time
from importlib import resources

import psycopg2
import psycopg2.errors
from cortex_utils.config import PostgresConfig
from cortex_utils.logging import get_logger

logger = get_logger(__name__)

SCHEMA_LOCK_NAME = "cortex_slack_schema"
MIGRATIONS_TABLE = "slack_schema_migrations"

# A boot that deadlocks with a running sibling is killed by Postgres and, before
# this, revived by the restart policy -- the crash-loop shape this codebase
# disavows everywhere else. Postgres kills exactly one side of a deadlock, so
# the other proceeds and a retry here succeeds once it has finished.
DEADLOCK_RETRIES = 5
DEADLOCK_FIRST_DELAY_S = 0.5

# Slack's browser-session tokens. Phase 1 only: an internal Slack app replaces
# these in phase 2 and the token file goes away with them.
# \Z rather than $, for the same reason as TS_PATTERN in slack/api.py: `$` also
# matches before a trailing newline, so a token with one would validate here and
# then authenticate as nobody. These values are pasted by hand, so that is the
# likely shape of a bad one.
_TOKEN_PATTERNS = {
    "SLACK_XOXC_TOKEN": re.compile(r"^xoxc-[A-Za-z0-9-]+\Z"),
    "SLACK_XOXD_TOKEN": re.compile(r"^xoxd-[A-Za-z0-9%/+=-]+\Z"),
}


def dsn_from_env() -> str:
    """The Postgres DSN, without requiring any Slack credential.

    Its own function because the parse worker needs a database and no token.
    Bundling the DSN with the Slack credentials would force a credential on a
    service that never calls Slack, and compose would then mount the secret into
    a container with no use for it.
    """
    dsn: str = PostgresConfig(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "cortex"),
        user=os.environ.get("POSTGRES_USER", "cortex"),
        password=os.environ.get("POSTGRES_PASSWORD", "cortex"),
    ).dsn
    return dsn


def migrations() -> list[tuple[str, str]]:
    """The migration files, as (name, sql) in filename order, read as package data.

    Not `Path(__file__).parents[2] / "migrations"`. That walks up to a
    repo-root sibling of `src/`, which exists in a source checkout and nowhere
    else -- an installed container would raise FileNotFoundError at boot rather
    than failing at build. cortex-granola shipped exactly that bug.

    The name is the identity apply_schema() records. Editing a file that has
    already been applied does nothing anywhere it has run; a change to the
    schema is a new file.
    """
    package = resources.files("cortex_slack.migrations")
    found = [
        (entry.name, entry.read_text(encoding="utf-8"))
        for entry in sorted(package.iterdir(), key=lambda e: e.name)
        if entry.name.endswith(".sql")
    ]
    if not found:
        raise RuntimeError("no .sql files packaged under cortex_slack.migrations")
    return found


def apply_schema(conn: psycopg2.extensions.connection) -> None:
    """Apply the migrations not yet recorded, under an advisory lock, in one transaction.

    COMMITS. Not composable inside a larger transaction -- the lock is
    xact-scoped, so holding it open would block every other service's boot for
    as long as the caller's transaction ran.

    Tracked by filename in slack_schema_migrations, so a boot against a current
    database executes no DDL at all. It used to re-run every file on every
    boot, and ALTER TABLE ... ADD COLUMN IF NOT EXISTS takes ACCESS EXCLUSIVE
    even when the column exists -- held to commit, across tables in file order.
    A sibling worker's ordinary query locking those tables in the other order
    deadlocked with it on the very first production redeploy, and both services
    came back only because Docker restarted them. On a database that predates
    the tracking table every file runs once more, exactly as before, and is
    recorded; after that a boot is one SELECT.

    The lock is not decoration. `CREATE TABLE IF NOT EXISTS` is not safe against
    a concurrent identical CREATE: both sessions pass the existence check, and
    the loser raises DuplicateTable. Three containers boot together here.
    pg_advisory_xact_lock blocks rather than failing and releases on commit or
    rollback, so a crash mid-apply cannot strand it.

    Retries a deadlock, with backoff. Postgres kills one side only; the other
    side finishes and releases its locks, and the next attempt goes through.
    """
    for attempt in range(DEADLOCK_RETRIES + 1):
        try:
            applied = _apply_pending(conn)
        except psycopg2.errors.DeadlockDetected as exc:
            conn.rollback()
            if attempt == DEADLOCK_RETRIES:
                raise
            delay = DEADLOCK_FIRST_DELAY_S * 2**attempt
            logger.warning(
                "schema apply deadlocked with another session; retrying",
                attempt=attempt + 1,
                delay_s=delay,
                error=str(exc).splitlines()[0],
            )
            time.sleep(delay)
            continue
        if applied:
            logger.info("schema migrations applied", migrations=applied)
        return


def _apply_pending(conn: psycopg2.extensions.connection) -> list[str]:
    """One attempt: lock, find what is unrecorded, run it, record it, commit."""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (SCHEMA_LOCK_NAME,))
        # The table name is written out rather than interpolated from
        # MIGRATIONS_TABLE, which exists so tests can name the same table:
        # bandit cannot tell a module constant from user input in an f-string.
        cur.execute(
            "CREATE TABLE IF NOT EXISTS slack_schema_migrations ("
            "  name TEXT PRIMARY KEY,"
            "  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
            ")"
        )
        cur.execute("SELECT name FROM slack_schema_migrations")
        recorded = {row[0] for row in cur.fetchall()}

        applied: list[str] = []
        for name, sql in migrations():
            if name in recorded:
                continue
            cur.execute(sql)
            cur.execute("INSERT INTO slack_schema_migrations (name) VALUES (%s)", (name,))
            applied.append(name)
    conn.commit()
    return applied


def _read_token_file(path: str) -> dict[str, str]:
    """Parse a shell-style env file holding the stealth tokens.

    Accepts `export NAME=value`, `NAME=value`, blank lines and `#` comments, and
    strips one layer of matching quotes. Refuses anything it cannot read
    unambiguously rather than guessing: a token silently read as the empty
    string authenticates as nobody, and the resulting 401 looks like a revoked
    session rather than a parse bug.
    """
    values: dict[str, str] = {}
    with open(path, encoding="utf-8") as handle:
        for lineno, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            match = re.match(r"^(?:export\s+)?([A-Z_][A-Z0-9_]*)\s*=\s*(.*)$", line)
            if match is None:
                raise ValueError(f"{path}:{lineno}: cannot parse {line!r}")
            name, value = match.group(1), match.group(2).strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            if name in values:
                raise ValueError(f"{path}:{lineno}: {name} set twice")
            values[name] = value
    return values


def stealth_tokens(path: str | None = None) -> tuple[str, str]:
    """The (xoxc, xoxd) pair for phase-1 backfill.

    Both are required together: xoxc is the API token and xoxd is the `d`
    cookie, and Slack rejects either alone. Validated against their known
    prefixes so a truncated paste fails here rather than as an opaque
    `invalid_auth` several thousand requests into a backfill.
    """
    token_path = path or os.environ.get("SLACK_TOKEN_FILE")
    if not token_path:
        raise RuntimeError("SLACK_TOKEN_FILE is not set")

    values = _read_token_file(token_path)
    resolved: list[str] = []
    for name, pattern in _TOKEN_PATTERNS.items():
        value = values.get(name, "")
        if not value:
            raise RuntimeError(f"{token_path}: {name} is missing or empty")
        if not pattern.match(value):
            raise RuntimeError(
                f"{token_path}: {name} does not look like a {name.split('_')[1]} token"
            )
        resolved.append(value)
    return resolved[0], resolved[1]
