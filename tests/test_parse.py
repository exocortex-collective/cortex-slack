"""Tests for tag derivation.

These tags are load-bearing: 89% of the archive is automated and one alert
channel is 82% of it, so the default read is only useful if `automated` is
exactly right.
"""

from __future__ import annotations

from datetime import UTC

import pytest

from cortex_slack.parse import (
    DEFAULT_EXCLUDED_TAGS,
    derive_tags,
    edited_at,
    is_automated,
    posted_at,
)


def test_a_plain_human_message_earns_no_tags() -> None:
    assert derive_tags({"ts": "1725379200.000100", "user": "U1", "text": "hi"}) == []


def test_a_bot_message_is_automated() -> None:
    assert is_automated({"bot_id": "B1", "subtype": "bot_message", "text": "alert"})


def test_an_app_posting_as_a_user_is_still_automated() -> None:
    """The signal is bot_id, not the absence of a user.

    Measured on this workspace: 47,089 messages carry both -- an app posting as
    someone, like a GIF integration or a CI notifier. Keying on "has no user"
    would call every one of them human and let the noisiest integrations into
    the default read.
    """
    gif_bot = {"ts": "1.000001", "user": "U44R29CJD", "bot_id": "B09ECSFHQT1", "app_id": "A1"}

    assert is_automated(gif_bot)
    assert "automated" in derive_tags(gif_bot)


def test_a_human_message_is_not_automated() -> None:
    assert not is_automated({"ts": "1.000001", "user": "U1", "text": "shipped v8.41"})


@pytest.mark.parametrize("subtype", ["channel_join", "tombstone", "retention_threshold"])
def test_slack_talking_about_itself_is_system(subtype: str) -> None:
    assert "system" in derive_tags({"ts": "1.000001", "user": "U1", "subtype": subtype})


def test_a_thread_reply_is_tagged_but_its_parent_is_not() -> None:
    """The parent carries thread_ts == ts.

    Treating any thread_ts as "reply" tags every thread starter as a reply to
    itself, which would hide the message that began the conversation.
    """
    parent = {"ts": "1725379200.000001", "thread_ts": "1725379200.000001"}
    reply = {"ts": "1725379201.000001", "thread_ts": "1725379200.000001"}

    assert "thread-reply" not in derive_tags(parent)
    assert "thread-reply" in derive_tags(reply)


def test_tags_are_sorted_so_a_reparse_is_not_a_change() -> None:
    tags = derive_tags(
        {"ts": "1.000001", "bot_id": "B1", "subtype": "channel_join", "edited": {"ts": "2.000001"}}
    )

    assert tags == sorted(tags)


def test_the_default_exclusion_is_defined_once() -> None:
    """Downstream reads go through this, not a re-spelled WHERE clause."""
    assert {"automated", "system"} == DEFAULT_EXCLUDED_TAGS


def test_timestamps_keep_their_sub_second_half() -> None:
    """float() loses precision at this magnitude, and the suffix is the ordering."""
    first = posted_at("1725379200.000001")
    second = posted_at("1725379200.000002")

    assert first.tzinfo is UTC
    assert first < second, "two messages in the same second collapsed together"
    assert first.microsecond == 1


def test_an_unedited_message_has_no_edit_time() -> None:
    assert edited_at({"ts": "1725379200.000001"}) is None


def test_an_edited_message_reports_the_edit_time_not_the_post_time() -> None:
    """The distinction is the whole point of the column."""
    message = {"ts": "1725379200.000001", "edited": {"ts": "1725379300.000000", "user": "U1"}}

    assert edited_at(message) == posted_at("1725379300.000000")
    assert edited_at(message) != posted_at(message["ts"])  # type: ignore[index]
    assert "edited" in derive_tags(message)
