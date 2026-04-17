from __future__ import annotations

import json
from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from typing import Any

from astrbot.api.message_components import (
    At,
    Face,
    File,
    Forward,
    Image,
    Plain,
    Poke,
    Record,
    Reply,
    Video,
)

from .models import ArchivedNoticeEvent, ArchivedSegment, ForwardNodeRecord

HANDLED_RAW_SEGMENTS = {
    "at",
    "contact",
    "dice",
    "face",
    "file",
    "forward",
    "image",
    "json",
    "location",
    "markdown",
    "music",
    "poke",
    "record",
    "reply",
    "rps",
    "shake",
    "share",
    "text",
    "video",
}


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    if hasattr(value, "model_dump"):
        try:
            return json_safe(value.model_dump())
        except TypeError:
            pass
    if hasattr(value, "dict"):
        try:
            return json_safe(value.dict())
        except TypeError:
            pass
    if hasattr(value, "__dict__"):
        return {
            str(key): json_safe(item)
            for key, item in vars(value).items()
            if not str(key).startswith("_")
        }
    return str(value)


def serialize_raw_event(raw_event: Any) -> dict[str, Any]:
    serialized = json_safe(raw_event)
    if isinstance(serialized, dict):
        return serialized
    return {"raw": serialized}


def normalize_message_segments(
    components: list[Any],
    raw_event: dict[str, Any],
) -> list[ArchivedSegment]:
    segments: list[ArchivedSegment] = []
    for index, component in enumerate(components):
        segments.append(_normalize_component(index, component))

    raw_segments = raw_event.get("message")
    if isinstance(raw_segments, list):
        extra_index = len(segments)
        for raw_segment in raw_segments:
            if not isinstance(raw_segment, Mapping):
                continue
            raw_type = str(raw_segment.get("type", "")).strip().lower()
            if not raw_type or raw_type in HANDLED_RAW_SEGMENTS:
                continue
            data = json_safe(raw_segment.get("data", {}))
            text = ""
            if isinstance(data, dict):
                text = str(data.get("summary") or data.get("text") or "")
            segments.append(
                ArchivedSegment(
                    index=extra_index,
                    segment_type=f"raw_{raw_type}",
                    raw_type=raw_type,
                    text=text,
                    data=data if isinstance(data, dict) else {"value": data},
                    media_status="raw_only",
                )
            )
            extra_index += 1

    return segments


def build_plain_text(segments: list[ArchivedSegment]) -> str:
    parts: list[str] = []
    for segment in segments:
        if segment.segment_type == "text":
            parts.append(segment.text)
        elif segment.segment_type == "at":
            target = segment.data.get("name") or segment.data.get("qq") or "unknown"
            parts.append(f"@{target}")
        elif segment.segment_type == "face":
            parts.append("[face]")
        elif segment.segment_type == "image":
            parts.append("[image]")
        elif segment.segment_type == "record":
            parts.append("[record]")
        elif segment.segment_type == "video":
            parts.append("[video]")
        elif segment.segment_type == "file":
            parts.append("[file]")
        elif segment.segment_type == "reply":
            parts.append("[reply]")
        elif segment.segment_type == "forward":
            parts.append("[forward]")
        elif segment.segment_type == "poke":
            parts.append("[poke]")
        elif segment.segment_type.startswith("raw_"):
            parts.append(f"[{segment.segment_type}]")
    return "".join(parts).strip()


def build_outline(segments: list[ArchivedSegment]) -> str:
    return build_plain_text(segments)


def build_notice_record(
    *,
    raw_event: dict[str, Any],
    platform_id: str,
    bot_self_id: str,
    group_id: str,
    session_id: str,
    group_name: str,
    archived_at: int,
) -> ArchivedNoticeEvent:
    notice_type = str(raw_event.get("notice_type") or "")
    sub_type = _optional_text(raw_event.get("sub_type"))
    actor_user_id = _optional_text(raw_event.get("user_id"))
    operator_id = _optional_text(raw_event.get("operator_id"))
    target_id = _optional_text(raw_event.get("target_id"))
    message_id = _optional_text(raw_event.get("message_id"))
    reaction_code = _optional_text(raw_event.get("code"))
    reaction_count = _optional_int(raw_event.get("count"))

    if reaction_code is None and raw_event.get("likes") is not None:
        reaction_code = json.dumps(
            json_safe(raw_event.get("likes")),
            ensure_ascii=False,
            sort_keys=True,
        )

    event_time = _optional_int(raw_event.get("time")) or archived_at
    event_key = "|".join(
        [
            platform_id,
            group_id,
            notice_type,
            sub_type or "",
            actor_user_id or "",
            operator_id or "",
            target_id or "",
            message_id or "",
            str(event_time),
            reaction_code or "",
            str(reaction_count or ""),
        ]
    )

    return ArchivedNoticeEvent(
        event_key=event_key,
        platform_id=platform_id,
        bot_self_id=bot_self_id,
        group_id=group_id,
        session_id=session_id,
        group_name=group_name,
        notice_type=notice_type,
        sub_type=sub_type,
        actor_user_id=actor_user_id,
        operator_id=operator_id,
        target_id=target_id,
        message_id=message_id,
        reaction_code=reaction_code,
        reaction_count=reaction_count,
        event_time=event_time,
        archived_at=archived_at,
        raw_event=raw_event,
    )


def parse_forward_nodes(
    forward_id: str,
    response: Any,
) -> list[ForwardNodeRecord]:
    payload = json_safe(response)
    messages: list[Any] = []

    if isinstance(payload, dict):
        if isinstance(payload.get("messages"), list):
            messages = payload["messages"]
        elif isinstance(payload.get("data"), dict) and isinstance(
            payload["data"].get("messages"),
            list,
        ):
            messages = payload["data"]["messages"]
    elif isinstance(payload, list):
        messages = payload

    nodes: list[ForwardNodeRecord] = []
    for index, message in enumerate(messages):
        if not isinstance(message, dict):
            continue

        node_data = message
        if message.get("type") == "node" and isinstance(message.get("data"), dict):
            node_data = message["data"]

        sender_id = _optional_text(node_data.get("user_id") or node_data.get("uin"))
        sender_name = _optional_text(node_data.get("nickname") or node_data.get("name"))
        sent_time = _optional_int(node_data.get("time"))
        content = json_safe(node_data.get("content"))

        nodes.append(
            ForwardNodeRecord(
                forward_id=forward_id,
                node_index=index,
                sender_id=sender_id,
                sender_name=sender_name,
                sent_time=sent_time,
                content_text=_flatten_forward_content(content),
                content_json={"content": content},
            )
        )

    return nodes


def _normalize_component(index: int, component: Any) -> ArchivedSegment:
    if isinstance(component, Plain):
        return ArchivedSegment(
            index=index,
            segment_type="text",
            raw_type="text",
            text=component.text,
            data={"text": component.text},
        )
    if isinstance(component, At):
        return ArchivedSegment(
            index=index,
            segment_type="at",
            raw_type="at",
            data={
                "qq": str(component.qq),
                "name": str(component.name or ""),
            },
        )
    if isinstance(component, Face):
        return ArchivedSegment(
            index=index,
            segment_type="face",
            raw_type="face",
            data={"id": int(component.id)},
        )
    if isinstance(component, Reply):
        return ArchivedSegment(
            index=index,
            segment_type="reply",
            raw_type="reply",
            text=str(component.message_str or ""),
            data={
                "id": str(component.id),
                "sender_id": _optional_text(component.sender_id),
                "sender_nickname": _optional_text(component.sender_nickname),
                "time": _optional_int(component.time),
                "message_str": str(component.message_str or ""),
            },
        )
    if isinstance(component, Image):
        return ArchivedSegment(
            index=index,
            segment_type="image",
            raw_type="image",
            source_url=str(component.url or component.file or "") or None,
            original_name=_basename(component.file or component.url),
            data={
                "file": str(component.file or ""),
                "url": str(component.url or ""),
                "sub_type": _optional_text(component._type),
                "file_unique": _optional_text(component.file_unique),
            },
            attachment_kind="image",
            source_component=component,
        )
    if isinstance(component, Record):
        return ArchivedSegment(
            index=index,
            segment_type="record",
            raw_type="record",
            source_url=str(component.url or component.file or "") or None,
            original_name=_basename(component.file or component.url),
            data={
                "file": str(component.file or ""),
                "url": str(component.url or ""),
                "text": str(component.text or ""),
            },
            attachment_kind="record",
            source_component=component,
        )
    if isinstance(component, Video):
        return ArchivedSegment(
            index=index,
            segment_type="video",
            raw_type="video",
            source_url=str(component.file or "") or None,
            original_name=_basename(component.file),
            data={"file": str(component.file or ""), "cover": str(component.cover or "")},
            attachment_kind="video",
            source_component=component,
        )
    if isinstance(component, File):
        return ArchivedSegment(
            index=index,
            segment_type="file",
            raw_type="file",
            source_url=str(component.url or component.file or "") or None,
            original_name=str(component.name or "") or None,
            data={
                "name": str(component.name or ""),
                "file": str(component.file or ""),
                "url": str(component.url or ""),
            },
            attachment_kind="file",
            source_component=component,
        )
    if isinstance(component, Forward):
        return ArchivedSegment(
            index=index,
            segment_type="forward",
            raw_type="forward",
            data={"forward_id": str(component.id)},
        )
    if isinstance(component, Poke):
        return ArchivedSegment(
            index=index,
            segment_type="poke",
            raw_type="poke",
            data={"id": _optional_text(component.id), "type": _optional_text(component._type)},
        )

    type_name = _optional_text(getattr(getattr(component, "type", None), "value", None))
    if not type_name:
        type_name = component.__class__.__name__.lower()

    data = json_safe(component)
    if not isinstance(data, dict):
        data = {"value": data}

    return ArchivedSegment(
        index=index,
        segment_type=type_name,
        raw_type=type_name,
        data=data,
        text=str(data.get("text", "")),
    )


def _flatten_forward_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                parts.append(str(item))
                continue
            item_type = str(item.get("type", "")).lower()
            data = item.get("data", {})
            if item_type == "text" and isinstance(data, dict):
                parts.append(str(data.get("text", "")))
            elif item_type == "face":
                parts.append("[face]")
            elif item_type == "image":
                parts.append("[image]")
            else:
                parts.append(f"[{item_type or 'segment'}]")
        return "".join(parts).strip()
    return str(content or "")


def _basename(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return Path(text).name or None


def _optional_int(value: Any) -> int | None:
    if value in (None, "", False):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None

