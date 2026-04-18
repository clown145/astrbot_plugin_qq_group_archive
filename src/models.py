from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ArchivedSegment:
    index: int
    segment_type: str
    text: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    raw_type: str | None = None
    source_url: str | None = None
    original_name: str | None = None
    media_status: str | None = None
    local_path: str | None = None
    sha256: str | None = None
    mime_type: str | None = None
    file_size: int | None = None
    attachment_kind: str | None = None
    source_component: Any = None


@dataclass(slots=True)
class ArchivedMessage:
    platform_id: str
    bot_self_id: str
    group_id: str
    session_id: str
    group_name: str
    message_id: str | None
    sender_id: str | None
    sender_name: str | None
    sender_card: str | None
    direction: str
    post_type: str
    message_sub_type: str | None
    plain_text: str
    outline: str
    event_time: int
    archived_at: int
    raw_event: dict[str, Any] | None
    segments: list[ArchivedSegment] = field(default_factory=list)


@dataclass(slots=True)
class ArchivedNoticeEvent:
    event_key: str
    platform_id: str
    bot_self_id: str
    group_id: str
    session_id: str
    group_name: str
    notice_type: str
    sub_type: str | None
    actor_user_id: str | None
    operator_id: str | None
    target_id: str | None
    message_id: str | None
    reaction_code: str | None
    reaction_count: int | None
    event_time: int
    archived_at: int
    raw_event: dict[str, Any]


@dataclass(slots=True)
class ForwardNodeRecord:
    forward_id: str
    node_index: int
    sender_id: str | None
    sender_name: str | None
    sent_time: int | None
    content_text: str
    content_json: dict[str, Any]


@dataclass(slots=True)
class ProfileStats:
    incoming_message_count: int = 0
    outgoing_message_count: int = 0
    text_message_count: int = 0
    total_text_chars: int = 0
    image_count: int = 0
    record_count: int = 0
    video_count: int = 0
    file_count: int = 0
    forward_count: int = 0
    reply_count: int = 0
    at_count: int = 0
    raw_segment_count: int = 0
    media_message_count: int = 0
    recall_action_count: int = 0
    recalled_message_count: int = 0
    emoji_notice_count: int = 0

    def to_mapping(self) -> dict[str, int]:
        return {
            "incoming_message_count": self.incoming_message_count,
            "outgoing_message_count": self.outgoing_message_count,
            "text_message_count": self.text_message_count,
            "total_text_chars": self.total_text_chars,
            "image_count": self.image_count,
            "record_count": self.record_count,
            "video_count": self.video_count,
            "file_count": self.file_count,
            "forward_count": self.forward_count,
            "reply_count": self.reply_count,
            "at_count": self.at_count,
            "raw_segment_count": self.raw_segment_count,
            "media_message_count": self.media_message_count,
            "recall_action_count": self.recall_action_count,
            "recalled_message_count": self.recalled_message_count,
            "emoji_notice_count": self.emoji_notice_count,
        }


@dataclass(slots=True)
class InteractionRecord:
    platform_id: str
    group_id: str
    source_user_id: str
    target_user_id: str
    interaction_type: str
    event_time: int
    count: int = 1
