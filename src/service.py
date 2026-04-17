from __future__ import annotations

import asyncio
import hashlib
import mimetypes
import os
import shutil
import time
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.message_components import Plain
from astrbot.core.platform.astr_message_event import AstrMessageEvent
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)

from .config import PluginSettings
from .models import ArchivedMessage, ArchivedSegment
from .normalizer import (
    build_notice_record,
    build_outline,
    build_plain_text,
    normalize_message_segments,
    parse_forward_nodes,
    serialize_raw_event,
)
from .storage import ArchiveDatabase


class QQGroupArchiveService:
    def __init__(self, *, data_dir: Path, db: ArchiveDatabase, config: Any):
        self.data_dir = data_dir
        self.db = db
        self.config = config
        self.media_dir = self.data_dir / "media"
        self._init_lock = asyncio.Lock()
        self._initialized = False

    async def initialize(self):
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            self.media_dir.mkdir(parents=True, exist_ok=True)
            await self.db.initialize()
            self._initialized = True
            logger.info("qq_group_archive initialized")

    async def archive_event(self, event: AiocqhttpMessageEvent):
        await self.initialize()
        settings = PluginSettings.from_mapping(self.config)
        if not settings.enabled:
            return
        if event.get_platform_name() != "aiocqhttp":
            return

        group_id = event.get_group_id()
        if not group_id:
            return
        if not settings.matches_group(
            platform_id=event.get_platform_id(),
            group_id=group_id,
            unified_msg_origin=event.unified_msg_origin,
            session_id=event.session_id,
        ):
            return

        raw_event = serialize_raw_event(getattr(event.message_obj, "raw_message", None))
        post_type = str(raw_event.get("post_type") or "")
        if post_type == "message":
            await self._archive_incoming_message(event, raw_event, settings)
            return
        if post_type == "notice":
            await self._archive_notice_event(event, raw_event, settings)

    async def archive_outgoing(self, event: AstrMessageEvent):
        await self.initialize()
        settings = PluginSettings.from_mapping(self.config)
        if not settings.enabled or not settings.capture_outgoing_messages:
            return
        if event.get_platform_name() != "aiocqhttp":
            return
        group_id = event.get_group_id()
        if not group_id:
            return
        if not settings.matches_group(
            platform_id=event.get_platform_id(),
            group_id=group_id,
            unified_msg_origin=event.unified_msg_origin,
            session_id=event.session_id,
        ):
            return

        result = event.get_result()
        if result is None or not result.chain:
            return

        now = int(time.time())
        group_name = await self.db.get_group_name(
            platform_id=event.get_platform_id(),
            group_id=group_id,
        )
        segments = normalize_message_segments(
            list(result.chain),
            raw_event={},
        )
        if settings.save_media_files:
            await self._persist_media_segments(segments, settings)

        message = ArchivedMessage(
            platform_id=event.get_platform_id(),
            bot_self_id=event.get_self_id(),
            group_id=group_id,
            session_id=event.session_id,
            group_name=group_name,
            message_id=None,
            sender_id=event.get_self_id(),
            sender_name="bot",
            sender_card=None,
            direction="outgoing",
            post_type="message_sent",
            message_sub_type=None,
            plain_text=build_plain_text(segments),
            outline=build_outline(segments),
            event_time=now,
            archived_at=now,
            raw_event=None,
            segments=segments,
        )
        await self.db.insert_message(message)

    async def get_group_status_text(self, event: AstrMessageEvent) -> str:
        await self.initialize()
        settings = PluginSettings.from_mapping(self.config)
        matched = settings.matches_group(
            platform_id=event.get_platform_id(),
            group_id=event.get_group_id(),
            unified_msg_origin=event.unified_msg_origin,
            session_id=event.session_id,
        )
        return "\n".join(
            [
                "QQ archive status",
                f"enabled: {settings.enabled}",
                f"group_id: {event.get_group_id()}",
                f"umo: {event.unified_msg_origin}",
                f"matched: {matched}",
                f"db: {self.db.db_path}",
                f"media_dir: {self.media_dir}",
            ]
        )

    async def get_group_stats_text(self, event: AstrMessageEvent, *, days: int) -> str:
        await self.initialize()
        days = max(int(days or 7), 1)
        since_ts = int(time.time()) - days * 24 * 60 * 60
        stats = await self.db.get_group_stats(
            platform_id=event.get_platform_id(),
            group_id=event.get_group_id(),
            since_ts=since_ts,
        )
        return "\n".join(
            [
                f"QQ archive stats ({days} day(s))",
                f"group_id: {event.get_group_id()}",
                f"incoming_messages: {stats['incoming_messages']}",
                f"outgoing_messages: {stats['outgoing_messages']}",
                f"recalled_messages: {stats['recalled_messages']}",
                f"notice_events: {stats['notice_events']}",
                f"emoji_reactions: {stats['emoji_reactions']}",
                f"forward_nodes: {stats['forward_nodes']}",
            ]
        )

    async def _archive_incoming_message(
        self,
        event: AiocqhttpMessageEvent,
        raw_event: dict[str, Any],
        settings: PluginSettings,
    ):
        archived_at = int(time.time())
        group_name = self._resolve_group_name(event, raw_event)
        if group_name:
            await self.db.upsert_group_name(
                platform_id=event.get_platform_id(),
                group_id=event.get_group_id(),
                group_name=group_name,
                updated_at=archived_at,
            )

        raw_sender = raw_event.get("sender") if isinstance(raw_event.get("sender"), dict) else {}
        sender_id = self._optional_text(raw_event.get("user_id")) or event.get_sender_id()
        sender_name = self._optional_text(raw_sender.get("nickname")) or event.get_sender_name()
        sender_card = self._optional_text(raw_sender.get("card"))

        segments = normalize_message_segments(event.get_messages(), raw_event)
        if settings.save_media_files:
            await self._persist_media_segments(segments, settings)

        message = ArchivedMessage(
            platform_id=event.get_platform_id(),
            bot_self_id=event.get_self_id(),
            group_id=event.get_group_id(),
            session_id=event.session_id,
            group_name=group_name,
            message_id=self._optional_text(raw_event.get("message_id"))
            or self._optional_text(getattr(event.message_obj, "message_id", None)),
            sender_id=sender_id,
            sender_name=sender_name,
            sender_card=sender_card,
            direction="incoming",
            post_type="message",
            message_sub_type=self._optional_text(raw_event.get("sub_type")),
            plain_text=build_plain_text(segments),
            outline=build_outline(segments),
            event_time=self._optional_int(raw_event.get("time")) or archived_at,
            archived_at=archived_at,
            raw_event=raw_event if settings.save_raw_event else None,
            segments=segments,
        )
        message_row_id, created = await self.db.insert_message(message)
        if not created:
            return

        if not settings.expand_forward_messages:
            return

        for segment in segments:
            if segment.segment_type != "forward":
                continue
            forward_id = self._optional_text(segment.data.get("forward_id"))
            if not forward_id:
                continue
            try:
                response = await event.bot.call_action(
                    action="get_forward_msg",
                    id=forward_id,
                )
                nodes = parse_forward_nodes(forward_id, response)
                await self.db.insert_forward_nodes(
                    message_row_id=message_row_id,
                    nodes=nodes,
                )
            except Exception as exc:
                logger.warning("forward expansion failed for %s: %s", forward_id, exc)

    async def _archive_notice_event(
        self,
        event: AiocqhttpMessageEvent,
        raw_event: dict[str, Any],
        settings: PluginSettings,
    ):
        notice_type = str(raw_event.get("notice_type") or "")
        interesting = notice_type in {"group_recall", "group_msg_emoji_like"}
        if not settings.record_all_notice_events and not interesting:
            return

        archived_at = int(time.time())
        group_name = self._resolve_group_name(event, raw_event)
        if not group_name:
            group_name = await self.db.get_group_name(
                platform_id=event.get_platform_id(),
                group_id=event.get_group_id(),
            )

        notice = build_notice_record(
            raw_event=raw_event,
            platform_id=event.get_platform_id(),
            bot_self_id=event.get_self_id(),
            group_id=event.get_group_id(),
            session_id=event.session_id,
            group_name=group_name,
            archived_at=archived_at,
        )
        _, created = await self.db.insert_notice(notice)
        if not created:
            return

        if notice.notice_type == "group_recall" and notice.message_id:
            await self.db.mark_message_recalled(
                platform_id=notice.platform_id,
                group_id=notice.group_id,
                message_id=notice.message_id,
                operator_id=notice.operator_id,
                recalled_at=notice.event_time,
            )

    async def _persist_media_segments(
        self,
        segments: list[ArchivedSegment],
        settings: PluginSettings,
    ):
        for segment in segments:
            if segment.attachment_kind is None or segment.source_component is None:
                continue

            try:
                source_path = await self._resolve_segment_source_path(segment)
            except Exception as exc:
                segment.media_status = f"error:{exc}"
                continue

            if not source_path or not os.path.exists(source_path):
                segment.media_status = "missing"
                continue

            digest, file_size = await asyncio.to_thread(self._hash_file, source_path)
            segment.file_size = file_size
            segment.sha256 = digest
            segment.mime_type = mimetypes.guess_type(source_path)[0] or None

            if file_size > settings.max_media_size_bytes:
                segment.media_status = "skipped_too_large"
                continue

            ext = Path(segment.original_name or source_path).suffix
            if not ext:
                ext = Path(source_path).suffix

            dated_dir = self.media_dir / segment.attachment_kind / time.strftime(
                "%Y%m%d",
                time.localtime(),
            )
            dated_dir.mkdir(parents=True, exist_ok=True)
            destination = dated_dir / f"{digest}{ext}"
            if not destination.exists():
                await asyncio.to_thread(shutil.copy2, source_path, destination)

            segment.local_path = str(destination.relative_to(self.data_dir))
            segment.media_status = "stored"

    async def _resolve_segment_source_path(self, segment: ArchivedSegment) -> str:
        component = segment.source_component
        if segment.attachment_kind == "image":
            return await component.convert_to_file_path()
        if segment.attachment_kind == "record":
            return await component.convert_to_file_path()
        if segment.attachment_kind == "video":
            return await component.convert_to_file_path()
        if segment.attachment_kind == "file":
            return await component.get_file()
        raise ValueError(f"unsupported attachment kind: {segment.attachment_kind}")

    def _resolve_group_name(
        self,
        event: AstrMessageEvent,
        raw_event: dict[str, Any],
    ) -> str:
        group = getattr(event.message_obj, "group", None)
        group_name = getattr(group, "group_name", None) if group else None
        if group_name:
            return str(group_name)
        return str(raw_event.get("group_name") or "")

    @staticmethod
    def _hash_file(path: str) -> tuple[str, int]:
        hasher = hashlib.sha256()
        size = 0
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                size += len(chunk)
                hasher.update(chunk)
        return hasher.hexdigest(), size

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        if value in (None, "", False):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _optional_text(value: Any) -> str | None:
        text = str(value or "").strip()
        return text or None

