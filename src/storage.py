from __future__ import annotations

import json
from pathlib import Path

import aiosqlite

from .models import ArchivedMessage, ArchivedNoticeEvent, ForwardNodeRecord


class ArchiveDatabase:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn: aiosqlite.Connection | None = None
        self._init_lock = None

    async def initialize(self):
        if self._conn is not None:
            return
        if self._init_lock is None:
            import asyncio

            self._init_lock = asyncio.Lock()

        async with self._init_lock:
            if self._conn is not None:
                return

            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = await aiosqlite.connect(self.db_path)
            self._conn.row_factory = aiosqlite.Row
            await self._conn.execute("PRAGMA journal_mode=WAL;")
            await self._conn.execute("PRAGMA synchronous=NORMAL;")
            await self._conn.execute("PRAGMA foreign_keys=ON;")
            await self._create_schema()
            await self._conn.commit()

    async def close(self):
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def insert_message(self, message: ArchivedMessage) -> tuple[int, bool]:
        await self.initialize()
        assert self._conn is not None

        if message.message_id is not None:
            row = await self._fetchone(
                """
                SELECT id
                FROM archived_messages
                WHERE platform_id = ? AND group_id = ? AND direction = ? AND message_id = ?
                """,
                (
                    message.platform_id,
                    message.group_id,
                    message.direction,
                    message.message_id,
                ),
            )
            if row is not None:
                return int(row["id"]), False

        cursor = await self._conn.execute(
            """
            INSERT INTO archived_messages (
                platform_id,
                bot_self_id,
                group_id,
                session_id,
                group_name,
                message_id,
                sender_id,
                sender_name,
                sender_card,
                direction,
                post_type,
                message_sub_type,
                plain_text,
                outline,
                event_time,
                archived_at,
                raw_event_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message.platform_id,
                message.bot_self_id,
                message.group_id,
                message.session_id,
                message.group_name,
                message.message_id,
                message.sender_id,
                message.sender_name,
                message.sender_card,
                message.direction,
                message.post_type,
                message.message_sub_type,
                message.plain_text,
                message.outline,
                message.event_time,
                message.archived_at,
                self._to_json(message.raw_event),
            ),
        )
        message_row_id = int(cursor.lastrowid)

        for segment in message.segments:
            await self._conn.execute(
                """
                INSERT INTO archived_segments (
                    message_row_id,
                    seg_index,
                    seg_type,
                    raw_type,
                    seg_text,
                    seg_data_json,
                    source_url,
                    original_name,
                    media_status,
                    local_path,
                    sha256,
                    mime_type,
                    file_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_row_id,
                    segment.index,
                    segment.segment_type,
                    segment.raw_type,
                    segment.text,
                    self._to_json(segment.data),
                    segment.source_url,
                    segment.original_name,
                    segment.media_status,
                    segment.local_path,
                    segment.sha256,
                    segment.mime_type,
                    segment.file_size,
                ),
            )

        await self._conn.commit()
        return message_row_id, True

    async def insert_notice(self, notice: ArchivedNoticeEvent) -> tuple[int, bool]:
        await self.initialize()
        assert self._conn is not None

        row = await self._fetchone(
            "SELECT id FROM archived_notice_events WHERE event_key = ?",
            (notice.event_key,),
        )
        if row is not None:
            return int(row["id"]), False

        cursor = await self._conn.execute(
            """
            INSERT INTO archived_notice_events (
                event_key,
                platform_id,
                bot_self_id,
                group_id,
                session_id,
                group_name,
                notice_type,
                sub_type,
                actor_user_id,
                operator_id,
                target_id,
                message_id,
                reaction_code,
                reaction_count,
                event_time,
                archived_at,
                raw_event_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                notice.event_key,
                notice.platform_id,
                notice.bot_self_id,
                notice.group_id,
                notice.session_id,
                notice.group_name,
                notice.notice_type,
                notice.sub_type,
                notice.actor_user_id,
                notice.operator_id,
                notice.target_id,
                notice.message_id,
                notice.reaction_code,
                notice.reaction_count,
                notice.event_time,
                notice.archived_at,
                self._to_json(notice.raw_event),
            ),
        )
        await self._conn.commit()
        return int(cursor.lastrowid), True

    async def mark_message_recalled(
        self,
        *,
        platform_id: str,
        group_id: str,
        message_id: str,
        operator_id: str | None,
        recalled_at: int,
    ):
        await self.initialize()
        assert self._conn is not None
        await self._conn.execute(
            """
            UPDATE archived_messages
            SET is_recalled = 1,
                recalled_at = ?,
                recalled_by = ?
            WHERE platform_id = ? AND group_id = ? AND direction = 'incoming' AND message_id = ?
            """,
            (recalled_at, operator_id, platform_id, group_id, message_id),
        )
        await self._conn.commit()

    async def insert_forward_nodes(
        self,
        *,
        message_row_id: int,
        nodes: list[ForwardNodeRecord],
    ):
        if not nodes:
            return
        await self.initialize()
        assert self._conn is not None

        for node in nodes:
            await self._conn.execute(
                """
                INSERT INTO archived_forward_nodes (
                    message_row_id,
                    forward_id,
                    node_index,
                    sender_id,
                    sender_name,
                    sent_time,
                    content_text,
                    content_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_row_id,
                    node.forward_id,
                    node.node_index,
                    node.sender_id,
                    node.sender_name,
                    node.sent_time,
                    node.content_text,
                    self._to_json(node.content_json),
                ),
            )
        await self._conn.commit()

    async def upsert_group_name(
        self,
        *,
        platform_id: str,
        group_id: str,
        group_name: str,
        updated_at: int,
    ):
        if not group_name:
            return
        await self.initialize()
        assert self._conn is not None
        await self._conn.execute(
            """
            INSERT INTO archived_groups (platform_id, group_id, group_name, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(platform_id, group_id) DO UPDATE
            SET group_name = excluded.group_name,
                updated_at = excluded.updated_at
            """,
            (platform_id, group_id, group_name, updated_at),
        )
        await self._conn.commit()

    async def get_group_name(self, *, platform_id: str, group_id: str) -> str:
        await self.initialize()
        row = await self._fetchone(
            """
            SELECT group_name
            FROM archived_groups
            WHERE platform_id = ? AND group_id = ?
            """,
            (platform_id, group_id),
        )
        if row is None:
            return ""
        return str(row["group_name"] or "")

    async def get_group_stats(
        self,
        *,
        platform_id: str,
        group_id: str,
        since_ts: int,
    ) -> dict[str, int]:
        await self.initialize()
        incoming = await self._fetch_value(
            """
            SELECT COUNT(*)
            FROM archived_messages
            WHERE platform_id = ? AND group_id = ? AND direction = 'incoming' AND event_time >= ?
            """,
            (platform_id, group_id, since_ts),
        )
        outgoing = await self._fetch_value(
            """
            SELECT COUNT(*)
            FROM archived_messages
            WHERE platform_id = ? AND group_id = ? AND direction = 'outgoing' AND event_time >= ?
            """,
            (platform_id, group_id, since_ts),
        )
        recalled = await self._fetch_value(
            """
            SELECT COUNT(*)
            FROM archived_messages
            WHERE platform_id = ? AND group_id = ? AND direction = 'incoming'
              AND event_time >= ? AND is_recalled = 1
            """,
            (platform_id, group_id, since_ts),
        )
        notices = await self._fetch_value(
            """
            SELECT COUNT(*)
            FROM archived_notice_events
            WHERE platform_id = ? AND group_id = ? AND event_time >= ?
            """,
            (platform_id, group_id, since_ts),
        )
        reactions = await self._fetch_value(
            """
            SELECT COUNT(*)
            FROM archived_notice_events
            WHERE platform_id = ? AND group_id = ? AND event_time >= ?
              AND notice_type = 'group_msg_emoji_like'
            """,
            (platform_id, group_id, since_ts),
        )
        forwards = await self._fetch_value(
            """
            SELECT COUNT(*)
            FROM archived_forward_nodes n
            JOIN archived_messages m ON m.id = n.message_row_id
            WHERE m.platform_id = ? AND m.group_id = ? AND m.event_time >= ?
            """,
            (platform_id, group_id, since_ts),
        )
        return {
            "incoming_messages": incoming,
            "outgoing_messages": outgoing,
            "recalled_messages": recalled,
            "notice_events": notices,
            "emoji_reactions": reactions,
            "forward_nodes": forwards,
        }

    async def _create_schema(self):
        assert self._conn is not None
        await self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS archived_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform_id TEXT NOT NULL,
                bot_self_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                group_name TEXT NOT NULL DEFAULT '',
                message_id TEXT,
                sender_id TEXT,
                sender_name TEXT,
                sender_card TEXT,
                direction TEXT NOT NULL,
                post_type TEXT NOT NULL,
                message_sub_type TEXT,
                plain_text TEXT NOT NULL DEFAULT '',
                outline TEXT NOT NULL DEFAULT '',
                event_time INTEGER NOT NULL,
                archived_at INTEGER NOT NULL,
                raw_event_json TEXT,
                is_recalled INTEGER NOT NULL DEFAULT 0,
                recalled_at INTEGER,
                recalled_by TEXT
            );

            CREATE UNIQUE INDEX IF NOT EXISTS uq_archived_message_identity
            ON archived_messages (platform_id, group_id, direction, message_id)
            WHERE message_id IS NOT NULL;

            CREATE TABLE IF NOT EXISTS archived_segments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_row_id INTEGER NOT NULL,
                seg_index INTEGER NOT NULL,
                seg_type TEXT NOT NULL,
                raw_type TEXT,
                seg_text TEXT NOT NULL DEFAULT '',
                seg_data_json TEXT NOT NULL DEFAULT '{}',
                source_url TEXT,
                original_name TEXT,
                media_status TEXT,
                local_path TEXT,
                sha256 TEXT,
                mime_type TEXT,
                file_size INTEGER,
                FOREIGN KEY(message_row_id) REFERENCES archived_messages(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_archived_segments_message_row_id
            ON archived_segments (message_row_id);

            CREATE TABLE IF NOT EXISTS archived_notice_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT NOT NULL UNIQUE,
                platform_id TEXT NOT NULL,
                bot_self_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                group_name TEXT NOT NULL DEFAULT '',
                notice_type TEXT NOT NULL,
                sub_type TEXT,
                actor_user_id TEXT,
                operator_id TEXT,
                target_id TEXT,
                message_id TEXT,
                reaction_code TEXT,
                reaction_count INTEGER,
                event_time INTEGER NOT NULL,
                archived_at INTEGER NOT NULL,
                raw_event_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_archived_notice_scope
            ON archived_notice_events (platform_id, group_id, event_time);

            CREATE TABLE IF NOT EXISTS archived_forward_nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_row_id INTEGER NOT NULL,
                forward_id TEXT NOT NULL,
                node_index INTEGER NOT NULL,
                sender_id TEXT,
                sender_name TEXT,
                sent_time INTEGER,
                content_text TEXT NOT NULL DEFAULT '',
                content_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY(message_row_id) REFERENCES archived_messages(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_archived_forward_nodes_message_row_id
            ON archived_forward_nodes (message_row_id);

            CREATE TABLE IF NOT EXISTS archived_groups (
                platform_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                group_name TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (platform_id, group_id)
            );
            """
        )

    async def _fetch_value(self, sql: str, params: tuple) -> int:
        row = await self._fetchone(sql, params)
        if row is None:
            return 0
        return int(list(row)[0] if not isinstance(row, aiosqlite.Row) else row[0])

    async def _fetchone(self, sql: str, params: tuple):
        assert self._conn is not None
        async with self._conn.execute(sql, params) as cursor:
            return await cursor.fetchone()

    @staticmethod
    def _to_json(value) -> str:
        return json.dumps(value or {}, ensure_ascii=False, sort_keys=True)

