# cortex-slack

Slack archive ingest for Cortex. Part of the Cortex family; see `~/Cortex/CLAUDE.md`
for the multi-repo layout and `~/Cortex/docs/slack-integration-spec.md` for the
options audit and API-constraint background.

## What this is

A durable local archive of Slack conversations in Postgres, queryable by SQL
alongside `emails_parsed` and `granola_notes`. Once a message is here, agent
access to history is just SQL — no round-trip to answer "what did we decide
about X".

**Scope, settled and not a review topic:** everything Devon's own account can
see, DMs included. Same visibility as his own Slack search; no new exposure.

## Admin scope is refused, not merely unused

The phase-1 session token carries `admin` scope, because the account has it.
The archive must not spend it: `SlackClient._post` raises `AdminMethodRefused`
for any method beginning `admin.`, and every request in the client goes through
that one boundary.

This is a guard rather than a convention because "we agreed not to" is not a
property anyone can verify by reading the code — and the whole premise of the
archive is that it records only what the account could already read through
Slack's own UI. Admin endpoints would widen that to the workspace, quietly and
without any visible difference in the output.

Reading a public channel the account has not joined is **not** admin reach —
any member can open one — so those are in scope. Anything requiring `admin.*` is
not, and is not a judgement call to make per-endpoint.

## The constraint that shapes everything

Slack limited `conversations.history` and `conversations.replies` to **1
request/minute, 15 messages** for non-Marketplace distributed apps (announced
2025-05-29, enforced 2025-09-02).

**Internal, non-distributed apps are exempt** and keep 1,000 messages/request at
50+ req/min. This app is internal and must stay that way — the moment it is
distributed to another workspace the architecture stops working. It is not a
product.

## Phases

| Phase | Auth | Status |
|-------|------|--------|
| 1 | slackdump stealth tokens (`xoxc` + `xoxd`) | in progress |
| 2 | internal Slack app | not started — **stop and ask before creating it** |
| 3 | Socket Mode push adapter | not started |

Phase 1 exists so backfill can start without waiting on app creation. Both
tokens are required together: `xoxc` is the API token, `xoxd` is the `d` cookie,
and Slack rejects either alone.

## Architecture

Two tiers, as in cortex-granola:

```
adapters ──> ingest_message() ──> slack_messages_raw ──> parse worker ──> slack_messages
  backfill                                           └──> media worker  ──> MinIO + slack_files
  poller
  socket (phase 2)
```

`ingest.py` is the single boundary every arrival mechanism goes through.
An adapter decides only WHICH messages to ingest and WHEN — nothing downstream
varies by arrival, which is what keeps the next adapter cheap to add.

## Identity

A message is `(channel_id, ts)`. Never `ts` alone: Slack hands out `ts` per
channel, so collisions across channels are ordinary.

`ts` is a **string** — `"1725379200.001500"`. The sub-second suffix distinguishes
two messages posted in the same second and determines ordering. It is never
parsed into a float for identity or comparison.

## Watermarks

One per channel, in `slack_sync_state`. A single global watermark would either
re-read every channel or skip the quiet ones.

`last_ts` advances **only after a clean pass** over that channel, and to the
scan's start rather than to "now". A watermark that moves on a partial run
silently drops whatever the run missed — cortex-granola shipped that bug and it
took a four-hour soak to notice.

## Services

| Container | Port | Role |
|-----------|------|------|
| slack-sync | 8100 | backfill + poller |
| slack-parse-worker | 8101 | raw → structured |
| slack-media-worker | 8102 | files → MinIO |

Only `slack-sync` gets the token mount. The other two read from Postgres and
never call Slack, so handing them a credential the config object happens to
carry would be exposure for nothing.

## Development

```bash
uv sync --extra dev
uv run pytest          # needs CORTEX_TEST_DSN
uv run mypy src/cortex_slack
uv run ruff check .
```

Tests use a live Postgres from `CORTEX_TEST_DSN`, not a mock. The defects this
suite exists to catch — a watermark that advances after a failure, an
`ON CONFLICT` that fires when it should not, a `dedup_key` that does not dedup —
are invisible to a mock, which hands back whatever the code asked for.

The DSN must name a disposable database; `conftest.py` refuses anything else,
because the fixtures delete from the **shared** `queue` table that gmail-sync,
parse-worker and triage-worker all use.

`CORTEX_REQUIRE_LIVE=1` in CI turns a missing DSN into a failure rather than a
skip. `-p no:skipping` does not do this — it disables the skip *markers*, while a
runtime `pytest.skip()` in a fixture sails past and the run still exits 0.

## Migrations

`src/cortex_slack/migrations/*.sql` are applied by `apply_schema()` at every
service boot, once each, recorded by filename in `slack_schema_migrations`.
A boot against a current database runs no migration DDL. Consequences:

- A schema change is a **new file**. Editing a file that has already been
  applied does nothing anywhere it has run, production included.
- Every statement must still be safe to re-run. A database from before the
  ledger existed replays every file once on its next boot, and the test
  database is shared across runs.
- The ledger is not truncated by the `clean` fixture; delete a row from it (or
  drop the test database) to force a file to run again locally.

## Git workflow

All changes go through PRs with a `pr-review-loop` to convergence. Never
`git commit`/`git push` directly — use the skill's `commit-and-push.sh`.
