"""Channel and user dimension sync.

Small, slow-moving tables refreshed by a couple of requests. Kept apart from the
message path because they are the only thing here with no watermark: they are
re-read in full each time, which costs two paginated calls and removes a whole
class of staleness bug.
"""

from __future__ import annotations

from typing import Any

import psycopg2
from cortex_utils.logging import get_logger
from psycopg2.extras import Json

logger = get_logger(__name__)


def _kind(channel: dict[str, Any]) -> str:
    if channel.get("is_im"):
        return "im"
    if channel.get("is_mpim"):
        return "mpim"
    if channel.get("is_private"):
        return "private"
    return "public"


def upsert_channels(conn: psycopg2.extensions.connection, channels: list[dict[str, Any]]) -> int:
    """Record every conversation. Returns the count written.

    `is_member` is stored rather than inferred: a DM has no membership flag but
    is always readable, and a public channel may be visible without being
    joined. Downstream wants "can I read this", not "am I in it".
    """
    with conn.cursor() as cur:
        for channel in channels:
            readable = bool(
                channel.get("is_member") or channel.get("is_im") or channel.get("is_mpim")
            )
            cur.execute(
                """
                INSERT INTO slack_channels
                    (channel_id, name, kind, is_archived, is_member, topic, purpose, raw)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (channel_id) DO UPDATE
                   SET name = EXCLUDED.name,
                       kind = EXCLUDED.kind,
                       is_archived = EXCLUDED.is_archived,
                       is_member = EXCLUDED.is_member,
                       topic = EXCLUDED.topic,
                       purpose = EXCLUDED.purpose,
                       raw = EXCLUDED.raw,
                       updated_at = NOW()
                """,
                (
                    channel["id"],
                    channel.get("name"),
                    _kind(channel),
                    bool(channel.get("is_archived")),
                    readable,
                    (channel.get("topic") or {}).get("value"),
                    (channel.get("purpose") or {}).get("value"),
                    Json(channel),
                ),
            )
    conn.commit()
    return len(channels)


def upsert_users(conn: psycopg2.extensions.connection, users: list[dict[str, Any]]) -> int:
    """Record every user, so a message's author resolves to a name later.

    Email is kept because it is the join to the email corpus: the same person
    appears in emails_parsed by address and here by user id, and without this
    column nothing connects the two.
    """
    with conn.cursor() as cur:
        for user in users:
            profile = user.get("profile") or {}
            cur.execute(
                """
                INSERT INTO slack_users
                    (user_id, handle, display_name, real_name, email, is_bot, deleted, raw)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE
                   SET handle = EXCLUDED.handle,
                       display_name = EXCLUDED.display_name,
                       real_name = EXCLUDED.real_name,
                       email = EXCLUDED.email,
                       is_bot = EXCLUDED.is_bot,
                       deleted = EXCLUDED.deleted,
                       raw = EXCLUDED.raw,
                       updated_at = NOW()
                """,
                (
                    user["id"],
                    user.get("name"),
                    profile.get("display_name") or None,
                    profile.get("real_name") or None,
                    profile.get("email") or None,
                    bool(user.get("is_bot")),
                    bool(user.get("deleted")),
                    Json(user),
                ),
            )
    conn.commit()
    return len(users)
