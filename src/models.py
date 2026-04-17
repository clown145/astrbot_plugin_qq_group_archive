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

