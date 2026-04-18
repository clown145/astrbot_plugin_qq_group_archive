from __future__ import annotations

import json
import time
from pathlib import Path

import aiosqlite

from .models import (
    ArchivedMessage,
    ArchivedNoticeEvent,
    ForwardNodeRecord,
    InteractionRecord,
    ProfileStats,
)

PROFILE_STAT_COLUMNS = [
    "incoming_message_count",
    "outgoing_message_count",
    "text_message_count",
    "total_text_chars",
    "image_count",
    "record_count",
    "video_count",
    "file_count",
    "forward_count",
    "reply_count",
    "at_count",
    "raw_segment_count",
    "media_message_count",
    "recall_action_count",
    "recalled_message_count",
    "emoji_notice_count",
]


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
    ) -> dict[str, object] | None:
        await self.initialize()
        assert self._conn is not None
        row = await self._fetchone(
            """
            SELECT
                id,
                group_name,
                sender_id,
                sender_name,
                sender_card,
                event_time,
                is_recalled
            FROM archived_messages
            WHERE platform_id = ? AND group_id = ? AND direction = 'incoming' AND message_id = ?
            """,
            (platform_id, group_id, message_id),
        )
        if row is None:
            return None

        already_recalled = int(row["is_recalled"] or 0) == 1
        if not already_recalled:
            await self._conn.execute(
                """
                UPDATE archived_messages
                SET is_recalled = 1,
                    recalled_at = ?,
                    recalled_by = ?
                WHERE id = ?
                """,
                (recalled_at, operator_id, int(row["id"])),
            )
            await self._conn.commit()

        return {
            "message_row_id": int(row["id"]),
            "group_name": str(row["group_name"] or ""),
            "sender_id": str(row["sender_id"] or ""),
            "sender_name": str(row["sender_name"] or ""),
            "sender_card": str(row["sender_card"] or ""),
            "event_time": int(row["event_time"] or recalled_at),
            "already_recalled": already_recalled,
        }

    async def apply_user_profile_stats(
        self,
        *,
        platform_id: str,
        group_id: str,
        group_name: str,
        user_id: str | None,
        sender_name: str | None,
        sender_card: str | None,
        event_time: int,
        stats: ProfileStats,
        interactions: list[InteractionRecord] | None = None,
    ):
        await self.initialize()
        assert self._conn is not None

        clean_user_id = str(user_id or "").strip()
        clean_group_name = str(group_name or "")
        clean_sender_name = str(sender_name or "")
        clean_sender_card = str(sender_card or "")

        if clean_user_id and self._has_profile_stats(stats):
            stat_values = self._profile_stat_values(stats)
            stat_date = self._date_key(event_time)
            await self._conn.execute(
                self._profile_user_summary_sql(),
                (
                    platform_id,
                    clean_user_id,
                    clean_sender_name,
                    clean_sender_card,
                    event_time,
                    event_time,
                    group_id,
                    clean_group_name,
                    *stat_values,
                    event_time,
                ),
            )
            await self._conn.execute(
                self._profile_user_group_summary_sql(),
                (
                    platform_id,
                    group_id,
                    clean_group_name,
                    clean_user_id,
                    clean_sender_name,
                    clean_sender_card,
                    event_time,
                    event_time,
                    *stat_values,
                    event_time,
                ),
            )
            await self._conn.execute(
                self._profile_user_daily_stats_sql(),
                (
                    platform_id,
                    group_id,
                    clean_group_name,
                    clean_user_id,
                    stat_date,
                    clean_sender_name,
                    clean_sender_card,
                    event_time,
                    event_time,
                    *stat_values,
                    event_time,
                ),
            )

        for interaction in interactions or []:
            await self._conn.execute(
                """
                INSERT INTO profile_interactions (
                    platform_id,
                    group_id,
                    source_user_id,
                    target_user_id,
                    interaction_type,
                    interaction_count,
                    first_seen_at,
                    last_seen_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform_id, group_id, source_user_id, target_user_id, interaction_type)
                DO UPDATE SET
                    interaction_count = profile_interactions.interaction_count + excluded.interaction_count,
                    first_seen_at = MIN(profile_interactions.first_seen_at, excluded.first_seen_at),
                    last_seen_at = MAX(profile_interactions.last_seen_at, excluded.last_seen_at),
                    updated_at = excluded.updated_at
                """,
                (
                    interaction.platform_id,
                    interaction.group_id,
                    interaction.source_user_id,
                    interaction.target_user_id,
                    interaction.interaction_type,
                    interaction.count,
                    interaction.event_time,
                    interaction.event_time,
                    interaction.event_time,
                ),
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

    async def get_overview(self) -> dict[str, int | str | None]:
        await self.initialize()
        assert self._conn is not None

        total_groups = await self._fetch_value(
            """
            WITH grouped AS (
                SELECT platform_id, group_id FROM archived_messages
                UNION
                SELECT platform_id, group_id FROM archived_notice_events
            )
            SELECT COUNT(*) FROM grouped
            """,
            (),
        )
        incoming_messages = await self._fetch_value(
            """
            SELECT COUNT(*) FROM archived_messages WHERE direction = 'incoming'
            """,
            (),
        )
        outgoing_messages = await self._fetch_value(
            """
            SELECT COUNT(*) FROM archived_messages WHERE direction = 'outgoing'
            """,
            (),
        )
        recalled_messages = await self._fetch_value(
            """
            SELECT COUNT(*) FROM archived_messages WHERE is_recalled = 1
            """,
            (),
        )
        notice_events = await self._fetch_value(
            """
            SELECT COUNT(*) FROM archived_notice_events
            """,
            (),
        )
        emoji_reactions = await self._fetch_value(
            """
            SELECT COUNT(*) FROM archived_notice_events
            WHERE notice_type = 'group_msg_emoji_like'
            """,
            (),
        )
        forward_nodes = await self._fetch_value(
            """
            SELECT COUNT(*) FROM archived_forward_nodes
            """,
            (),
        )
        profile_users = await self._fetch_value(
            """
            SELECT COUNT(*) FROM profile_user_summary
            """,
            (),
        )
        profile_group_users = await self._fetch_value(
            """
            SELECT COUNT(*) FROM profile_user_group_summary
            """,
            (),
        )
        interaction_edges = await self._fetch_value(
            """
            SELECT COUNT(*) FROM profile_interactions
            """,
            (),
        )
        last_message_time = await self._fetch_value(
            "SELECT COALESCE(MAX(event_time), 0) FROM archived_messages",
            (),
        )
        last_notice_time = await self._fetch_value(
            "SELECT COALESCE(MAX(event_time), 0) FROM archived_notice_events",
            (),
        )

        return {
            "total_groups": total_groups,
            "incoming_messages": incoming_messages,
            "outgoing_messages": outgoing_messages,
            "recalled_messages": recalled_messages,
            "notice_events": notice_events,
            "emoji_reactions": emoji_reactions,
            "forward_nodes": forward_nodes,
            "profile_users": profile_users,
            "profile_group_users": profile_group_users,
            "interaction_edges": interaction_edges,
            "last_event_time": max(last_message_time, last_notice_time) or None,
            "db_path": str(self.db_path),
        }

    async def list_groups(
        self,
        *,
        limit: int = 200,
        offset: int = 0,
        search: str = "",
    ) -> dict[str, object]:
        await self.initialize()
        assert self._conn is not None

        limit = max(int(limit), 1)
        offset = max(int(offset), 0)
        pattern = f"%{search.strip()}%" if search.strip() else "%"

        total = await self._fetch_value(
            """
            WITH group_scope AS (
                SELECT platform_id, group_id, group_name FROM archived_groups
                UNION ALL
                SELECT platform_id, group_id, group_name FROM archived_messages
                UNION ALL
                SELECT platform_id, group_id, group_name FROM archived_notice_events
            ),
            collapsed AS (
                SELECT
                    platform_id,
                    group_id,
                    MAX(COALESCE(group_name, '')) AS group_name
                FROM group_scope
                GROUP BY platform_id, group_id
            )
            SELECT COUNT(*)
            FROM collapsed
            WHERE platform_id LIKE ? OR group_id LIKE ? OR group_name LIKE ?
            """,
            (pattern, pattern, pattern),
        )

        rows = await self._fetchall(
            """
            WITH group_scope AS (
                SELECT platform_id, group_id, group_name FROM archived_groups
                UNION ALL
                SELECT platform_id, group_id, group_name FROM archived_messages
                UNION ALL
                SELECT platform_id, group_id, group_name FROM archived_notice_events
            ),
            collapsed AS (
                SELECT
                    platform_id,
                    group_id,
                    MAX(COALESCE(group_name, '')) AS group_name
                FROM group_scope
                GROUP BY platform_id, group_id
            )
            SELECT
                c.platform_id,
                c.group_id,
                c.group_name,
                COALESCE((
                    SELECT COUNT(*)
                    FROM archived_messages m
                    WHERE m.platform_id = c.platform_id AND m.group_id = c.group_id
                ), 0) AS message_count,
                COALESCE((
                    SELECT COUNT(*)
                    FROM archived_messages m
                    WHERE m.platform_id = c.platform_id
                      AND m.group_id = c.group_id
                      AND m.direction = 'incoming'
                ), 0) AS incoming_count,
                COALESCE((
                    SELECT COUNT(*)
                    FROM archived_messages m
                    WHERE m.platform_id = c.platform_id
                      AND m.group_id = c.group_id
                      AND m.direction = 'outgoing'
                ), 0) AS outgoing_count,
                COALESCE((
                    SELECT COUNT(*)
                    FROM archived_messages m
                    WHERE m.platform_id = c.platform_id
                      AND m.group_id = c.group_id
                      AND m.is_recalled = 1
                ), 0) AS recalled_count,
                COALESCE((
                    SELECT COUNT(*)
                    FROM archived_notice_events n
                    WHERE n.platform_id = c.platform_id AND n.group_id = c.group_id
                ), 0) AS notice_count,
                COALESCE((
                    SELECT COUNT(*)
                    FROM archived_notice_events n
                    WHERE n.platform_id = c.platform_id
                      AND n.group_id = c.group_id
                      AND n.notice_type = 'group_msg_emoji_like'
                ), 0) AS reaction_count,
                COALESCE((
                    SELECT COUNT(*)
                    FROM archived_forward_nodes fn
                    JOIN archived_messages m ON m.id = fn.message_row_id
                    WHERE m.platform_id = c.platform_id AND m.group_id = c.group_id
                ), 0) AS forward_node_count,
                COALESCE((
                    SELECT COUNT(*)
                    FROM profile_user_group_summary p
                    WHERE p.platform_id = c.platform_id AND p.group_id = c.group_id
                ), 0) AS tracked_user_count,
                MAX(
                    COALESCE((
                        SELECT MAX(m.event_time)
                        FROM archived_messages m
                        WHERE m.platform_id = c.platform_id AND m.group_id = c.group_id
                    ), 0),
                    COALESCE((
                        SELECT MAX(n.event_time)
                        FROM archived_notice_events n
                        WHERE n.platform_id = c.platform_id AND n.group_id = c.group_id
                    ), 0)
                ) AS last_event_time
            FROM collapsed c
            WHERE c.platform_id LIKE ? OR c.group_id LIKE ? OR c.group_name LIKE ?
            ORDER BY last_event_time DESC, c.platform_id ASC, c.group_id ASC
            LIMIT ? OFFSET ?
            """,
            (pattern, pattern, pattern, limit, offset),
        )
        return {"items": [dict(row) for row in rows], "total": total}

    async def list_messages(
        self,
        *,
        platform_id: str,
        group_id: str,
        limit: int = 50,
        offset: int = 0,
        direction: str = "",
        search: str = "",
    ) -> dict[str, object]:
        await self.initialize()
        assert self._conn is not None

        limit = max(int(limit), 1)
        offset = max(int(offset), 0)
        direction = direction.strip().lower()
        pattern = f"%{search.strip()}%" if search.strip() else "%"
        if direction not in {"incoming", "outgoing"}:
            direction = ""

        where = [
            "platform_id = ?",
            "group_id = ?",
            "(plain_text LIKE ? OR sender_name LIKE ? OR sender_card LIKE ? OR outline LIKE ?)",
        ]
        params: list[object] = [platform_id, group_id, pattern, pattern, pattern, pattern]
        if direction:
            where.append("direction = ?")
            params.append(direction)
        where_sql = " AND ".join(where)

        total = await self._fetch_value(
            f"SELECT COUNT(*) FROM archived_messages WHERE {where_sql}",
            tuple(params),
        )
        rows = await self._fetchall(
            f"""
            SELECT
                id,
                platform_id,
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
                is_recalled,
                recalled_at,
                recalled_by,
                (
                    SELECT COUNT(*) FROM archived_segments s WHERE s.message_row_id = archived_messages.id
                ) AS segment_count,
                (
                    SELECT COUNT(*) FROM archived_forward_nodes fn WHERE fn.message_row_id = archived_messages.id
                ) AS forward_node_count
            FROM archived_messages
            WHERE {where_sql}
            ORDER BY event_time DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            tuple([*params, limit, offset]),
        )
        return {"items": [dict(row) for row in rows], "total": total}

    async def list_notices(
        self,
        *,
        platform_id: str,
        group_id: str,
        limit: int = 50,
        offset: int = 0,
        notice_type: str = "",
    ) -> dict[str, object]:
        await self.initialize()
        assert self._conn is not None

        limit = max(int(limit), 1)
        offset = max(int(offset), 0)
        where = ["platform_id = ?", "group_id = ?"]
        params: list[object] = [platform_id, group_id]
        if notice_type.strip():
            where.append("notice_type = ?")
            params.append(notice_type.strip())
        where_sql = " AND ".join(where)

        total = await self._fetch_value(
            f"SELECT COUNT(*) FROM archived_notice_events WHERE {where_sql}",
            tuple(params),
        )
        rows = await self._fetchall(
            f"""
            SELECT
                id,
                event_key,
                platform_id,
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
                archived_at
            FROM archived_notice_events
            WHERE {where_sql}
            ORDER BY event_time DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            tuple([*params, limit, offset]),
        )
        return {"items": [dict(row) for row in rows], "total": total}

    async def get_message_detail(self, message_row_id: int) -> dict[str, object] | None:
        await self.initialize()
        assert self._conn is not None

        row = await self._fetchone(
            """
            SELECT *
            FROM archived_messages
            WHERE id = ?
            """,
            (int(message_row_id),),
        )
        if row is None:
            return None

        segments = await self._fetchall(
            """
            SELECT *
            FROM archived_segments
            WHERE message_row_id = ?
            ORDER BY seg_index ASC, id ASC
            """,
            (int(message_row_id),),
        )
        forward_nodes = await self._fetchall(
            """
            SELECT *
            FROM archived_forward_nodes
            WHERE message_row_id = ?
            ORDER BY node_index ASC, id ASC
            """,
            (int(message_row_id),),
        )

        message = dict(row)
        message["raw_event"] = self._from_json(message.pop("raw_event_json", None))
        message["segments"] = [self._segment_row_to_dict(item) for item in segments]
        message["forward_nodes"] = [
            self._forward_node_row_to_dict(item) for item in forward_nodes
        ]
        return message

    async def get_notice_detail(self, notice_row_id: int) -> dict[str, object] | None:
        await self.initialize()
        assert self._conn is not None

        row = await self._fetchone(
            """
            SELECT *
            FROM archived_notice_events
            WHERE id = ?
            """,
            (int(notice_row_id),),
        )
        if row is None:
            return None

        notice = dict(row)
        notice["raw_event"] = self._from_json(notice.pop("raw_event_json", None))
        return notice

    async def list_group_profile_users(
        self,
        *,
        platform_id: str,
        group_id: str,
        limit: int = 50,
        offset: int = 0,
        search: str = "",
    ) -> dict[str, object]:
        await self.initialize()
        assert self._conn is not None

        limit = max(int(limit), 1)
        offset = max(int(offset), 0)
        pattern = f"%{search.strip()}%" if search.strip() else "%"

        total = await self._fetch_value(
            """
            SELECT COUNT(*)
            FROM profile_user_group_summary
            WHERE platform_id = ?
              AND group_id = ?
              AND (user_id LIKE ? OR last_sender_name LIKE ? OR last_sender_card LIKE ?)
            """,
            (platform_id, group_id, pattern, pattern, pattern),
        )
        rows = await self._fetchall(
            """
            SELECT
                platform_id,
                group_id,
                group_name,
                user_id,
                last_sender_name,
                last_sender_card,
                first_seen_at,
                last_seen_at,
                incoming_message_count,
                outgoing_message_count,
                text_message_count,
                total_text_chars,
                image_count,
                record_count,
                video_count,
                file_count,
                forward_count,
                reply_count,
                at_count,
                raw_segment_count,
                media_message_count,
                recall_action_count,
                recalled_message_count,
                emoji_notice_count,
                (incoming_message_count + outgoing_message_count) AS total_message_count
            FROM profile_user_group_summary
            WHERE platform_id = ?
              AND group_id = ?
              AND (user_id LIKE ? OR last_sender_name LIKE ? OR last_sender_card LIKE ?)
            ORDER BY total_message_count DESC, last_seen_at DESC, user_id ASC
            LIMIT ? OFFSET ?
            """,
            (platform_id, group_id, pattern, pattern, pattern, limit, offset),
        )
        return {"items": [dict(row) for row in rows], "total": total}

    async def get_group_profile_summary(
        self,
        *,
        platform_id: str,
        group_id: str,
    ) -> dict[str, object]:
        await self.initialize()
        assert self._conn is not None

        summary = await self._fetchone(
            """
            SELECT
                COUNT(*) AS tracked_users,
                COALESCE(MAX(group_name), '') AS group_name,
                COALESCE(SUM(incoming_message_count), 0) AS incoming_message_count,
                COALESCE(SUM(outgoing_message_count), 0) AS outgoing_message_count,
                COALESCE(SUM(text_message_count), 0) AS text_message_count,
                COALESCE(SUM(total_text_chars), 0) AS total_text_chars,
                COALESCE(SUM(image_count), 0) AS image_count,
                COALESCE(SUM(record_count), 0) AS record_count,
                COALESCE(SUM(video_count), 0) AS video_count,
                COALESCE(SUM(file_count), 0) AS file_count,
                COALESCE(SUM(forward_count), 0) AS forward_count,
                COALESCE(SUM(reply_count), 0) AS reply_count,
                COALESCE(SUM(at_count), 0) AS at_count,
                COALESCE(SUM(raw_segment_count), 0) AS raw_segment_count,
                COALESCE(SUM(media_message_count), 0) AS media_message_count,
                COALESCE(SUM(recall_action_count), 0) AS recall_action_count,
                COALESCE(SUM(recalled_message_count), 0) AS recalled_message_count,
                COALESCE(SUM(emoji_notice_count), 0) AS emoji_notice_count,
                COALESCE(MAX(last_seen_at), 0) AS last_seen_at
            FROM profile_user_group_summary
            WHERE platform_id = ? AND group_id = ?
            """,
            (platform_id, group_id),
        )
        daily_rows = await self._fetchall(
            """
            SELECT
                stat_date,
                SUM(incoming_message_count + outgoing_message_count) AS total_message_count,
                SUM(total_text_chars) AS total_text_chars,
                SUM(image_count) AS image_count,
                SUM(file_count) AS file_count,
                SUM(recall_action_count) AS recall_action_count,
                SUM(recalled_message_count) AS recalled_message_count,
                SUM(emoji_notice_count) AS emoji_notice_count
            FROM profile_user_daily_stats
            WHERE platform_id = ? AND group_id = ?
            GROUP BY stat_date
            ORDER BY stat_date DESC
            LIMIT 30
            """,
            (platform_id, group_id),
        )
        interaction_rows = await self._fetchall(
            """
            SELECT
                i.source_user_id,
                COALESCE(src.last_sender_card, src.last_sender_name, i.source_user_id) AS source_label,
                i.target_user_id,
                COALESCE(dst.last_sender_card, dst.last_sender_name, i.target_user_id) AS target_label,
                i.interaction_type,
                i.interaction_count,
                i.last_seen_at
            FROM profile_interactions i
            LEFT JOIN profile_user_group_summary src
                ON src.platform_id = i.platform_id
               AND src.group_id = i.group_id
               AND src.user_id = i.source_user_id
            LEFT JOIN profile_user_group_summary dst
                ON dst.platform_id = i.platform_id
               AND dst.group_id = i.group_id
               AND dst.user_id = i.target_user_id
            WHERE i.platform_id = ? AND i.group_id = ?
            ORDER BY i.interaction_count DESC, i.last_seen_at DESC
            LIMIT 20
            """,
            (platform_id, group_id),
        )

        return {
            "summary": dict(summary) if summary is not None else {},
            "daily_stats": [dict(row) for row in reversed(daily_rows)],
            "top_interactions": [dict(row) for row in interaction_rows],
        }

    async def get_user_group_profile(
        self,
        *,
        platform_id: str,
        group_id: str,
        user_id: str,
    ) -> dict[str, object] | None:
        await self.initialize()
        assert self._conn is not None

        summary = await self._fetchone(
            """
            SELECT
                *,
                (incoming_message_count + outgoing_message_count) AS total_message_count
            FROM profile_user_group_summary
            WHERE platform_id = ? AND group_id = ? AND user_id = ?
            """,
            (platform_id, group_id, user_id),
        )
        if summary is None:
            return None

        global_summary = await self._fetchone(
            """
            SELECT
                *,
                (incoming_message_count + outgoing_message_count) AS total_message_count
            FROM profile_user_summary
            WHERE platform_id = ? AND user_id = ?
            """,
            (platform_id, user_id),
        )
        daily_rows = await self._fetchall(
            """
            SELECT
                stat_date,
                incoming_message_count,
                outgoing_message_count,
                text_message_count,
                total_text_chars,
                image_count,
                record_count,
                video_count,
                file_count,
                forward_count,
                reply_count,
                at_count,
                raw_segment_count,
                media_message_count,
                recall_action_count,
                recalled_message_count,
                emoji_notice_count,
                first_event_at,
                last_event_at
            FROM profile_user_daily_stats
            WHERE platform_id = ? AND group_id = ? AND user_id = ?
            ORDER BY stat_date DESC
            LIMIT 30
            """,
            (platform_id, group_id, user_id),
        )
        outgoing_rows = await self._fetchall(
            """
            SELECT
                i.target_user_id,
                COALESCE(dst.last_sender_card, dst.last_sender_name, i.target_user_id) AS target_label,
                i.interaction_type,
                i.interaction_count,
                i.last_seen_at
            FROM profile_interactions i
            LEFT JOIN profile_user_group_summary dst
                ON dst.platform_id = i.platform_id
               AND dst.group_id = i.group_id
               AND dst.user_id = i.target_user_id
            WHERE i.platform_id = ? AND i.group_id = ? AND i.source_user_id = ?
            ORDER BY i.interaction_count DESC, i.last_seen_at DESC
            LIMIT 20
            """,
            (platform_id, group_id, user_id),
        )
        incoming_rows = await self._fetchall(
            """
            SELECT
                i.source_user_id,
                COALESCE(src.last_sender_card, src.last_sender_name, i.source_user_id) AS source_label,
                i.interaction_type,
                i.interaction_count,
                i.last_seen_at
            FROM profile_interactions i
            LEFT JOIN profile_user_group_summary src
                ON src.platform_id = i.platform_id
               AND src.group_id = i.group_id
               AND src.user_id = i.source_user_id
            WHERE i.platform_id = ? AND i.group_id = ? AND i.target_user_id = ?
            ORDER BY i.interaction_count DESC, i.last_seen_at DESC
            LIMIT 20
            """,
            (platform_id, group_id, user_id),
        )

        return {
            "summary": dict(summary),
            "global_summary": dict(global_summary) if global_summary is not None else None,
            "daily_stats": [dict(row) for row in reversed(daily_rows)],
            "outgoing_interactions": [dict(row) for row in outgoing_rows],
            "incoming_interactions": [dict(row) for row in incoming_rows],
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

            CREATE INDEX IF NOT EXISTS idx_archived_messages_scope
            ON archived_messages (platform_id, group_id, event_time DESC);

            CREATE INDEX IF NOT EXISTS idx_archived_messages_sender_scope
            ON archived_messages (platform_id, group_id, sender_id, event_time DESC);

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

            CREATE TABLE IF NOT EXISTS profile_user_summary (
                platform_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                last_sender_name TEXT NOT NULL DEFAULT '',
                last_sender_card TEXT NOT NULL DEFAULT '',
                first_seen_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL,
                last_group_id TEXT NOT NULL DEFAULT '',
                last_group_name TEXT NOT NULL DEFAULT '',
                incoming_message_count INTEGER NOT NULL DEFAULT 0,
                outgoing_message_count INTEGER NOT NULL DEFAULT 0,
                text_message_count INTEGER NOT NULL DEFAULT 0,
                total_text_chars INTEGER NOT NULL DEFAULT 0,
                image_count INTEGER NOT NULL DEFAULT 0,
                record_count INTEGER NOT NULL DEFAULT 0,
                video_count INTEGER NOT NULL DEFAULT 0,
                file_count INTEGER NOT NULL DEFAULT 0,
                forward_count INTEGER NOT NULL DEFAULT 0,
                reply_count INTEGER NOT NULL DEFAULT 0,
                at_count INTEGER NOT NULL DEFAULT 0,
                raw_segment_count INTEGER NOT NULL DEFAULT 0,
                media_message_count INTEGER NOT NULL DEFAULT 0,
                recall_action_count INTEGER NOT NULL DEFAULT 0,
                recalled_message_count INTEGER NOT NULL DEFAULT 0,
                emoji_notice_count INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (platform_id, user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_profile_user_summary_last_seen
            ON profile_user_summary (platform_id, last_seen_at DESC);

            CREATE TABLE IF NOT EXISTS profile_user_group_summary (
                platform_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                group_name TEXT NOT NULL DEFAULT '',
                user_id TEXT NOT NULL,
                last_sender_name TEXT NOT NULL DEFAULT '',
                last_sender_card TEXT NOT NULL DEFAULT '',
                first_seen_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL,
                incoming_message_count INTEGER NOT NULL DEFAULT 0,
                outgoing_message_count INTEGER NOT NULL DEFAULT 0,
                text_message_count INTEGER NOT NULL DEFAULT 0,
                total_text_chars INTEGER NOT NULL DEFAULT 0,
                image_count INTEGER NOT NULL DEFAULT 0,
                record_count INTEGER NOT NULL DEFAULT 0,
                video_count INTEGER NOT NULL DEFAULT 0,
                file_count INTEGER NOT NULL DEFAULT 0,
                forward_count INTEGER NOT NULL DEFAULT 0,
                reply_count INTEGER NOT NULL DEFAULT 0,
                at_count INTEGER NOT NULL DEFAULT 0,
                raw_segment_count INTEGER NOT NULL DEFAULT 0,
                media_message_count INTEGER NOT NULL DEFAULT 0,
                recall_action_count INTEGER NOT NULL DEFAULT 0,
                recalled_message_count INTEGER NOT NULL DEFAULT 0,
                emoji_notice_count INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (platform_id, group_id, user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_profile_user_group_summary_last_seen
            ON profile_user_group_summary (platform_id, group_id, last_seen_at DESC);

            CREATE TABLE IF NOT EXISTS profile_user_daily_stats (
                platform_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                group_name TEXT NOT NULL DEFAULT '',
                user_id TEXT NOT NULL,
                stat_date TEXT NOT NULL,
                last_sender_name TEXT NOT NULL DEFAULT '',
                last_sender_card TEXT NOT NULL DEFAULT '',
                first_event_at INTEGER NOT NULL,
                last_event_at INTEGER NOT NULL,
                incoming_message_count INTEGER NOT NULL DEFAULT 0,
                outgoing_message_count INTEGER NOT NULL DEFAULT 0,
                text_message_count INTEGER NOT NULL DEFAULT 0,
                total_text_chars INTEGER NOT NULL DEFAULT 0,
                image_count INTEGER NOT NULL DEFAULT 0,
                record_count INTEGER NOT NULL DEFAULT 0,
                video_count INTEGER NOT NULL DEFAULT 0,
                file_count INTEGER NOT NULL DEFAULT 0,
                forward_count INTEGER NOT NULL DEFAULT 0,
                reply_count INTEGER NOT NULL DEFAULT 0,
                at_count INTEGER NOT NULL DEFAULT 0,
                raw_segment_count INTEGER NOT NULL DEFAULT 0,
                media_message_count INTEGER NOT NULL DEFAULT 0,
                recall_action_count INTEGER NOT NULL DEFAULT 0,
                recalled_message_count INTEGER NOT NULL DEFAULT 0,
                emoji_notice_count INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (platform_id, group_id, user_id, stat_date)
            );

            CREATE INDEX IF NOT EXISTS idx_profile_user_daily_stats_scope
            ON profile_user_daily_stats (platform_id, group_id, stat_date DESC);

            CREATE TABLE IF NOT EXISTS profile_interactions (
                platform_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                source_user_id TEXT NOT NULL,
                target_user_id TEXT NOT NULL,
                interaction_type TEXT NOT NULL,
                interaction_count INTEGER NOT NULL DEFAULT 0,
                first_seen_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (platform_id, group_id, source_user_id, target_user_id, interaction_type)
            );

            CREATE INDEX IF NOT EXISTS idx_profile_interactions_source
            ON profile_interactions (platform_id, group_id, source_user_id, interaction_count DESC, last_seen_at DESC);

            CREATE INDEX IF NOT EXISTS idx_profile_interactions_target
            ON profile_interactions (platform_id, group_id, target_user_id, interaction_count DESC, last_seen_at DESC);
            """
        )

    @staticmethod
    def _has_profile_stats(stats: ProfileStats) -> bool:
        return any(value > 0 for value in stats.to_mapping().values())

    @staticmethod
    def _profile_stat_values(stats: ProfileStats) -> list[int]:
        mapping = stats.to_mapping()
        return [int(mapping[column]) for column in PROFILE_STAT_COLUMNS]

    @staticmethod
    def _profile_user_summary_sql() -> str:
        stat_columns_sql = ", ".join(PROFILE_STAT_COLUMNS)
        stat_placeholders_sql = ", ".join("?" for _ in PROFILE_STAT_COLUMNS)
        stat_updates_sql = ",\n                    ".join(
            f"{column} = profile_user_summary.{column} + excluded.{column}"
            for column in PROFILE_STAT_COLUMNS
        )
        return f"""
            INSERT INTO profile_user_summary (
                platform_id,
                user_id,
                last_sender_name,
                last_sender_card,
                first_seen_at,
                last_seen_at,
                last_group_id,
                last_group_name,
                {stat_columns_sql},
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, {stat_placeholders_sql}, ?)
            ON CONFLICT(platform_id, user_id) DO UPDATE SET
                last_sender_name = CASE
                    WHEN excluded.last_sender_name <> '' THEN excluded.last_sender_name
                    ELSE profile_user_summary.last_sender_name
                END,
                last_sender_card = CASE
                    WHEN excluded.last_sender_card <> '' THEN excluded.last_sender_card
                    ELSE profile_user_summary.last_sender_card
                END,
                first_seen_at = MIN(profile_user_summary.first_seen_at, excluded.first_seen_at),
                last_seen_at = MAX(profile_user_summary.last_seen_at, excluded.last_seen_at),
                last_group_id = excluded.last_group_id,
                last_group_name = CASE
                    WHEN excluded.last_group_name <> '' THEN excluded.last_group_name
                    ELSE profile_user_summary.last_group_name
                END,
                {stat_updates_sql},
                updated_at = excluded.updated_at
        """

    @staticmethod
    def _profile_user_group_summary_sql() -> str:
        stat_columns_sql = ", ".join(PROFILE_STAT_COLUMNS)
        stat_placeholders_sql = ", ".join("?" for _ in PROFILE_STAT_COLUMNS)
        stat_updates_sql = ",\n                    ".join(
            f"{column} = profile_user_group_summary.{column} + excluded.{column}"
            for column in PROFILE_STAT_COLUMNS
        )
        return f"""
            INSERT INTO profile_user_group_summary (
                platform_id,
                group_id,
                group_name,
                user_id,
                last_sender_name,
                last_sender_card,
                first_seen_at,
                last_seen_at,
                {stat_columns_sql},
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, {stat_placeholders_sql}, ?)
            ON CONFLICT(platform_id, group_id, user_id) DO UPDATE SET
                group_name = CASE
                    WHEN excluded.group_name <> '' THEN excluded.group_name
                    ELSE profile_user_group_summary.group_name
                END,
                last_sender_name = CASE
                    WHEN excluded.last_sender_name <> '' THEN excluded.last_sender_name
                    ELSE profile_user_group_summary.last_sender_name
                END,
                last_sender_card = CASE
                    WHEN excluded.last_sender_card <> '' THEN excluded.last_sender_card
                    ELSE profile_user_group_summary.last_sender_card
                END,
                first_seen_at = MIN(profile_user_group_summary.first_seen_at, excluded.first_seen_at),
                last_seen_at = MAX(profile_user_group_summary.last_seen_at, excluded.last_seen_at),
                {stat_updates_sql},
                updated_at = excluded.updated_at
        """

    @staticmethod
    def _profile_user_daily_stats_sql() -> str:
        stat_columns_sql = ", ".join(PROFILE_STAT_COLUMNS)
        stat_placeholders_sql = ", ".join("?" for _ in PROFILE_STAT_COLUMNS)
        stat_updates_sql = ",\n                    ".join(
            f"{column} = profile_user_daily_stats.{column} + excluded.{column}"
            for column in PROFILE_STAT_COLUMNS
        )
        return f"""
            INSERT INTO profile_user_daily_stats (
                platform_id,
                group_id,
                group_name,
                user_id,
                stat_date,
                last_sender_name,
                last_sender_card,
                first_event_at,
                last_event_at,
                {stat_columns_sql},
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, {stat_placeholders_sql}, ?)
            ON CONFLICT(platform_id, group_id, user_id, stat_date) DO UPDATE SET
                group_name = CASE
                    WHEN excluded.group_name <> '' THEN excluded.group_name
                    ELSE profile_user_daily_stats.group_name
                END,
                last_sender_name = CASE
                    WHEN excluded.last_sender_name <> '' THEN excluded.last_sender_name
                    ELSE profile_user_daily_stats.last_sender_name
                END,
                last_sender_card = CASE
                    WHEN excluded.last_sender_card <> '' THEN excluded.last_sender_card
                    ELSE profile_user_daily_stats.last_sender_card
                END,
                first_event_at = MIN(profile_user_daily_stats.first_event_at, excluded.first_event_at),
                last_event_at = MAX(profile_user_daily_stats.last_event_at, excluded.last_event_at),
                {stat_updates_sql},
                updated_at = excluded.updated_at
        """

    @staticmethod
    def _date_key(event_time: int) -> str:
        return time.strftime("%Y-%m-%d", time.localtime(event_time))

    async def _fetch_value(self, sql: str, params: tuple) -> int:
        row = await self._fetchone(sql, params)
        if row is None:
            return 0
        return int(list(row)[0] if not isinstance(row, aiosqlite.Row) else row[0])

    async def _fetchall(self, sql: str, params: tuple):
        assert self._conn is not None
        async with self._conn.execute(sql, params) as cursor:
            return await cursor.fetchall()

    async def _fetchone(self, sql: str, params: tuple):
        assert self._conn is not None
        async with self._conn.execute(sql, params) as cursor:
            return await cursor.fetchone()

    @staticmethod
    def _to_json(value) -> str:
        return json.dumps(value or {}, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _from_json(value):
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    def _segment_row_to_dict(self, row) -> dict[str, object]:
        payload = dict(row)
        payload["seg_data"] = self._from_json(payload.pop("seg_data_json", None))
        return payload

    def _forward_node_row_to_dict(self, row) -> dict[str, object]:
        payload = dict(row)
        payload["content"] = self._from_json(payload.pop("content_json", None))
        return payload
