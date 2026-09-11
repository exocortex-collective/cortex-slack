"""Configuration and schema bootstrap.

Env reading is local; anything shared with the rest of the family comes from
cortex_utils, per CLAUDE.md's "shared code goes in cortex-utils".
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from importlib import resources

import psycopg2
from cortex_utils.config import PostgresConfig

SCHEMA_LOCK_NAME = "cortex_slack_schema"

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
    Reaching for SlackSyncConfig.from_env() there would force a credential on a
    service that never calls Slack, and compose would then mount the secret into
    a container with no use for it.
    """
    dsn: str = PostgresConfig(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "cortex"),
        user=os.environ.get("POSTGRES_USER", "cortex"),
        password=os.environ.get("POSTGRES_PASSWORD", "cortex"),
    ).dsn()
    return dsn


def schema_sql() -> str:
    """The DDL this service applies at boot, read as package data.

    Not `Path(__file__).parents[2] / "migrations"`. That walks up to a
    repo-root sibling of `src/`, which exists in a source checkout and nowhere
    else -- an installed container would raise FileNotFoundError at boot rather
    than failing at build. cortex-granola shipped exactly that bug.

    Concatenated in filename order rather than applied file by file: every
    statement is individually idempotent, so the whole string is safe to re-run,
    and one string keeps boot a single execute rather than a loop with
    partial-failure states.
    """
    migrations = resources.files("cortex_slack.migrations")
    parts = [
        entry.read_text(encoding="utf-8")
        for entry in sorted(migrations.iterdir(), key=lambda e: e.name)
        if entry.name.endswith(".sql")
    ]
    if not parts:
        raise RuntimeError("no .sql files packaged under cortex_slack.migrations")
    return "\n".join(parts)


def apply_schema(conn: psycopg2.extensions.connection) -> None:
    """Apply the migrations under an advisory lock, in one transaction.

    COMMITS. Not composable inside a larger transaction -- the lock is
    xact-scoped, so holding it open would block every other service's boot for
    as long as the caller's transaction ran.

    The lock is not decoration. `CREATE TABLE IF NOT EXISTS` is not safe against
    a concurrent identical CREATE: both sessions pass the existence check, and
    the loser raises DuplicateTable. Three containers boot together here.
    pg_advisory_xact_lock blocks rather than failing and releases on commit or
    rollback, so a crash mid-apply cannot strand it.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (SCHEMA_LOCK_NAME,))
        cur.execute(schema_sql())
    conn.commit()


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


@dataclass(frozen=True)
class SlackSyncConfig:
    dsn: str
    xoxc_token: str
    xoxd_token: str
    poll_interval_s: int
    overlap_seconds: int
    deny_channels: frozenset[str]

    @classmethod
    def from_env(cls) -> SlackSyncConfig:
        xoxc, xoxd = stealth_tokens()
        return cls(
            dsn=dsn_from_env(),
            xoxc_token=xoxc,
            xoxd_token=xoxd,
            poll_interval_s=int(os.environ.get("SLACK_POLL_INTERVAL_S", "300")),
            # Re-read a little before the watermark on every poll. Slack can
            # deliver a message whose ts is older than one already seen, and an
            # exact-watermark read would skip it permanently.
            overlap_seconds=int(os.environ.get("SLACK_OVERLAP_SECONDS", "300")),
            deny_channels=_split(os.environ.get("SLACK_DENY_CHANNELS")),
        )


def _split(value: str | None) -> frozenset[str]:
    if not value:
        return frozenset()
    return frozenset(part.strip() for part in value.split(",") if part.strip())
