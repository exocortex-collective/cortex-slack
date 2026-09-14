"""Derive structured rows and tags from a raw Slack message.

Tags here are facts, not policy. Every one is computed from the payload and is
reproducible by re-running this function over the same row -- there is no config
to version, no rule to get wrong, and no way for a later edit to quietly stop
`automated` meaning what the default query assumes it means.

That matters because the default query's usefulness rests entirely on it: 89% of
this workspace's 1.5M messages are machine-generated, and a single alert channel
is 82% of the archive. If `automated` drifts, the corpus drowns.

Judgment tags -- sensitivity, project, customer -- deliberately live nowhere yet.
Nothing needs them, and a rules engine built before it has a rule is a config
system to maintain for no return.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

# Subtypes that are Slack talking about itself rather than anyone saying
# anything: joins, leaves, retention notices, the tombstone left by a deleted
# message. Excluded from default reads for the same reason as automation -- they
# are structure, not content.
# NOT in this set, deliberately:
#
#   file_comment      A person commenting on a file. The `text` is Slack's
#                     wrapper prose ("<@U1> commented on <@U2>'s file"), but the
#                     comment itself is in raw.comment.comment and the author in
#                     raw.comment.user -- so treating it as system hid 69 human
#                     comments AND the parse dropped their text and author.
#   me_message        A person talking, in the third person.
#   thread_broadcast  A person talking, echoed to the channel.
SYSTEM_SUBTYPES = frozenset(
    {
        "channel_join",
        "channel_leave",
        "channel_topic",
        "channel_purpose",
        "channel_name",
        "channel_archive",
        "channel_unarchive",
        "group_join",
        "group_leave",
        "tombstone",
        "retention_threshold",
        "huddle_thread",
        # Slack describing its own furniture. Low volume -- 38 rows across the
        # archive -- but this constant is meant to be the single answer to "what
        # counts as noise", so leaking is worse than the count suggests.
        "pinned_item",
        "unpinned_item",
        "joiner_notification_for_inviter",
        "mpdm_move",
        "app_conversation_join",
        "channel_posting_permissions",
        "channel_convert_to_private",
        "channel_convert_to_public",
        "tabbed_canvas_updated",
        "tabbed_canvas_updated_unread_ignore",
        "automatic_ai_huddle_notes_enabled",
        "sh_room_created",
        "ekm_access_denied",
        "reminder_add",
        "bot_add",
        "bot_remove",
    }
)

TAG_AUTOMATED = "automated"
TAG_SYSTEM = "system"
TAG_THREAD_REPLY = "thread-reply"
TAG_EDITED = "edited"

# The tags a default read excludes. Kept as a constant rather than spelled into
# each query so that "what does default mean" has exactly one answer.
DEFAULT_EXCLUDED_TAGS = frozenset({TAG_AUTOMATED, TAG_SYSTEM})


def is_automated(message: dict[str, Any]) -> bool:
    """True when an integration posted this, not a person typing.

    `bot_id` is the signal, not the absence of a `user`. Measured on this
    workspace: 47,089 messages carry BOTH a bot_id and a user -- an app posting
    as someone, like a GIF integration or a CI notifier. Keying on "has no user"
    would classify every one of those as human and let the noisiest integrations
    straight into the default read.
    """
    return bool(message.get("bot_id")) or message.get("subtype") == "bot_message"


def is_system(message: dict[str, Any]) -> bool:
    subtype = message.get("subtype")
    return isinstance(subtype, str) and subtype in SYSTEM_SUBTYPES


def derive_tags(message: dict[str, Any]) -> list[str]:
    """Every tag this message earns, sorted for a stable row.

    Sorted because the list is stored and compared; an unstable order would make
    every re-parse look like a change.
    """
    tags: set[str] = set()
    if is_automated(message):
        tags.add(TAG_AUTOMATED)
    if is_system(message):
        tags.add(TAG_SYSTEM)
    if _is_thread_reply(message):
        tags.add(TAG_THREAD_REPLY)
    if isinstance(message.get("edited"), dict):
        tags.add(TAG_EDITED)
    return sorted(tags)


def _is_thread_reply(message: dict[str, Any]) -> bool:
    """A reply, not the message that started the thread.

    The parent carries thread_ts == ts. Treating any thread_ts as "reply" would
    tag every thread starter as a reply to itself.
    """
    thread_ts = message.get("thread_ts")
    return bool(thread_ts) and thread_ts != message.get("ts")


def posted_at(ts: str) -> datetime:
    """Slack's ts is epoch seconds with a microsecond suffix.

    Split rather than float(): float("1725379200.001500") loses precision at
    this magnitude, and the suffix is what orders two messages in the same
    second.
    """
    seconds, _, micros = ts.partition(".")
    return datetime.fromtimestamp(int(seconds), tz=UTC).replace(
        microsecond=int((micros or "0").ljust(6, "0")[:6])
    )


def edited_at(message: dict[str, Any]) -> datetime | None:
    edited = message.get("edited")
    if not isinstance(edited, dict):
        return None
    ts = edited.get("ts")
    return posted_at(ts) if isinstance(ts, str) and "." in ts else None
