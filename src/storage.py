from __future__ import annotations

from collections import defaultdict
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
        profile_claims = await self._fetch_value(
            """
            SELECT COUNT(*) FROM profile_claims
            """,
            (),
        )
        profile_attributes = await self._fetch_value(
            """
            SELECT COUNT(*) FROM profile_attributes
            """,
            (),
        )
        profile_jobs_completed = await self._fetch_value(
            """
            SELECT COUNT(*) FROM profile_extraction_jobs WHERE status = 'completed'
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
        last_profile_update_time = await self._fetch_value(
            "SELECT COALESCE(MAX(updated_at), 0) FROM profile_claims",
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
            "profile_claims": profile_claims,
            "profile_attributes": profile_attributes,
            "profile_jobs_completed": profile_jobs_completed,
            "last_event_time": max(last_message_time, last_notice_time) or None,
            "last_profile_update_time": last_profile_update_time or None,
            "db_path": str(self.db_path),
        }

    async def get_profile_pipeline_status(self) -> dict[str, object]:
        await self.initialize()
        assert self._conn is not None

        archived_incoming_messages = await self._fetch_value(
            """
            SELECT COUNT(*) FROM archived_messages WHERE direction = 'incoming'
            """,
            (),
        )
        archived_groups = await self._fetch_value(
            """
            SELECT COUNT(*)
            FROM (
                SELECT platform_id, group_id
                FROM archived_messages
                WHERE direction = 'incoming'
                GROUP BY platform_id, group_id
            )
            """,
            (),
        )
        total_blocks = await self._fetch_value(
            "SELECT COUNT(*) FROM profile_message_blocks",
            (),
        )
        total_jobs = await self._fetch_value(
            "SELECT COUNT(*) FROM profile_extraction_jobs",
            (),
        )
        total_claims = await self._fetch_value(
            "SELECT COUNT(*) FROM profile_claims",
            (),
        )
        total_attributes = await self._fetch_value(
            "SELECT COUNT(*) FROM profile_attributes",
            (),
        )
        block_status_rows = await self._fetchall(
            """
            SELECT status, COUNT(*) AS count
            FROM profile_message_blocks
            GROUP BY status
            ORDER BY status ASC
            """,
            (),
        )
        job_status_rows = await self._fetchall(
            """
            SELECT status, COUNT(*) AS count
            FROM profile_extraction_jobs
            GROUP BY status
            ORDER BY status ASC
            """,
            (),
        )
        latest_jobs = await self._fetchall(
            """
            SELECT
                j.id,
                j.block_id,
                j.status,
                j.attempt_count,
                j.scheduled_at,
                j.started_at,
                j.finished_at,
                j.updated_at,
                j.last_error,
                j.workflow_state_json,
                j.result_summary_json,
                b.block_key,
                b.platform_id,
                b.group_id,
                b.group_name,
                b.message_count,
                b.approx_text_chars,
                b.first_event_at,
                b.last_event_at
            FROM profile_extraction_jobs j
            JOIN profile_message_blocks b ON b.id = j.block_id
            ORDER BY j.updated_at DESC, j.id DESC
            LIMIT 12
            """,
            (),
        )
        latest_claims = await self._fetchall(
            """
            SELECT
                id,
                platform_id,
                group_id,
                group_name,
                subject_user_id,
                attribute_type,
                COALESCE(
                    NULLIF(json_extract(payload_json, '$.attribute_label'), ''),
                    NULLIF(json_extract(payload_json, '$.label'), ''),
                    attribute_type
                ) AS attribute_label,
                normalized_value,
                source_kind,
                confidence,
                status,
                updated_at
            FROM profile_claims
            ORDER BY updated_at DESC, id DESC
            LIMIT 12
            """,
            (),
        )
        cursors = await self._fetchall(
            """
            SELECT state_key, state_value, updated_at
            FROM profile_pipeline_state
            WHERE state_key LIKE 'profile_pipeline_cursor:%'
            ORDER BY updated_at DESC
            LIMIT 20
            """,
            (),
        )
        last_message = await self._fetchone(
            """
            SELECT id, platform_id, group_id, event_time
            FROM archived_messages
            WHERE direction = 'incoming'
            ORDER BY id DESC
            LIMIT 1
            """,
            (),
        )

        return {
            "archived_incoming_messages": archived_incoming_messages,
            "archived_groups": archived_groups,
            "total_blocks": total_blocks,
            "total_jobs": total_jobs,
            "total_claims": total_claims,
            "total_attributes": total_attributes,
            "block_statuses": [dict(row) for row in block_status_rows],
            "job_statuses": [dict(row) for row in job_status_rows],
            "latest_jobs": [
                {
                    **dict(row),
                    "workflow_state": self._from_json(row["workflow_state_json"]),
                    "result_summary": self._from_json(row["result_summary_json"]),
                }
                for row in latest_jobs
            ],
            "latest_claims": [dict(row) for row in latest_claims],
            "cursors": [dict(row) for row in cursors],
            "last_message": dict(last_message) if last_message is not None else None,
        }

    async def reset_profile_pipeline(self, *, clear_claims: bool = True) -> dict[str, int]:
        await self.initialize()
        assert self._conn is not None

        counts: dict[str, int] = {}
        for table in (
            "profile_message_blocks",
            "profile_extraction_jobs",
            "profile_claims",
            "profile_attributes",
            "profile_attribute_history",
        ):
            counts[table] = int(
                await self._fetch_value(f"SELECT COUNT(*) FROM {table}", ()) or 0
            )

        await self._conn.execute(
            """
            DELETE FROM profile_pipeline_state
            WHERE state_key LIKE 'profile_pipeline_cursor:%'
            """
        )
        await self._conn.execute("DELETE FROM profile_extraction_jobs")
        await self._conn.execute("DELETE FROM profile_message_blocks")
        if clear_claims:
            await self._conn.execute("DELETE FROM profile_claim_evidence")
            await self._conn.execute("DELETE FROM profile_attribute_history")
            await self._conn.execute("DELETE FROM profile_attributes")
            await self._conn.execute("DELETE FROM profile_claims")
        await self._conn.commit()
        return counts

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
        attribute_rows = await self._fetchall(
            """
            SELECT
                attribute_type,
                MAX(COALESCE(
                    NULLIF(json_extract(payload_json, '$.attribute_label'), ''),
                    attribute_type
                )) AS attribute_label,
                COUNT(*) AS user_count,
                ROUND(AVG(confidence), 4) AS avg_confidence,
                MAX(updated_at) AS last_updated_at
            FROM profile_attributes
            WHERE platform_id = ? AND group_id = ?
            GROUP BY attribute_type
            ORDER BY user_count DESC, last_updated_at DESC, attribute_type ASC
            LIMIT 20
            """,
            (platform_id, group_id),
        )
        claim_status_rows = await self._fetchall(
            """
            SELECT
                attribute_type,
                MAX(COALESCE(
                    NULLIF(json_extract(payload_json, '$.attribute_label'), ''),
                    NULLIF(json_extract(payload_json, '$.label'), ''),
                    attribute_type
                )) AS attribute_label,
                status,
                COUNT(*) AS claim_count,
                MAX(updated_at) AS last_updated_at
            FROM profile_claims
            WHERE platform_id = ? AND group_id = ?
            GROUP BY attribute_type, status
            ORDER BY claim_count DESC, last_updated_at DESC, attribute_type ASC, status ASC
            LIMIT 30
            """,
            (platform_id, group_id),
        )

        return {
            "summary": dict(summary) if summary is not None else {},
            "daily_stats": [dict(row) for row in reversed(daily_rows)],
            "top_interactions": [dict(row) for row in interaction_rows],
            "top_attributes": [dict(row) for row in attribute_rows],
            "claim_statuses": [dict(row) for row in claim_status_rows],
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
        attribute_rows = await self._fetchall(
            """
            SELECT
                a.*,
                COALESCE(
                    NULLIF(json_extract(a.payload_json, '$.attribute_label'), ''),
                    NULLIF(json_extract(c.payload_json, '$.attribute_label'), ''),
                    NULLIF(json_extract(c.payload_json, '$.label'), ''),
                    a.attribute_type
                ) AS attribute_label,
                c.raw_value AS claim_raw_value,
                c.normalized_value AS claim_normalized_value,
                c.source_kind AS claim_source_kind,
                c.tense AS claim_tense,
                c.polarity AS claim_polarity,
                c.confidence AS claim_confidence,
                c.status AS claim_status,
                c.resolver_note AS claim_resolver_note,
                c.first_seen_at AS claim_first_seen_at,
                c.last_seen_at AS claim_last_seen_at,
                c.updated_at AS claim_updated_at,
                c.payload_json AS claim_payload_json
            FROM profile_attributes a
            LEFT JOIN profile_claims c
                ON c.id = a.current_claim_id
            WHERE a.platform_id = ? AND a.group_id = ? AND a.subject_user_id = ?
            ORDER BY a.updated_at DESC, a.attribute_type ASC
            """,
            (platform_id, group_id, user_id),
        )
        claim_rows = await self._fetchall(
            """
            SELECT
                c.*,
                COALESCE(
                    NULLIF(json_extract(c.payload_json, '$.attribute_label'), ''),
                    NULLIF(json_extract(c.payload_json, '$.label'), ''),
                    c.attribute_type
                ) AS attribute_label,
                COUNT(e.id) AS evidence_count
            FROM profile_claims c
            LEFT JOIN profile_claim_evidence e
                ON e.claim_id = c.id
            WHERE c.platform_id = ? AND c.group_id = ? AND c.subject_user_id = ?
            GROUP BY c.id
            ORDER BY c.updated_at DESC, c.id DESC
            LIMIT 40
            """,
            (platform_id, group_id, user_id),
        )
        claim_ids = [int(row["id"]) for row in claim_rows]
        evidence_by_claim: dict[int, list[dict[str, object]]] = defaultdict(list)
        if claim_ids:
            placeholders = ", ".join("?" for _ in claim_ids)
            evidence_rows = await self._fetchall(
                f"""
                SELECT
                    e.claim_id,
                    e.message_row_id,
                    e.excerpt,
                    e.evidence_kind,
                    e.created_at,
                    m.event_time,
                    m.sender_id,
                    m.sender_name,
                    m.sender_card,
                    m.direction,
                    m.plain_text,
                    m.outline
                FROM profile_claim_evidence e
                JOIN archived_messages m
                    ON m.id = e.message_row_id
                WHERE e.claim_id IN ({placeholders})
                ORDER BY m.event_time DESC, e.id DESC
                """,
                tuple(claim_ids),
            )
            for row in evidence_rows:
                payload = dict(row)
                payload["sender_label"] = (
                    str(payload.get("sender_card") or "").strip()
                    or str(payload.get("sender_name") or "").strip()
                    or str(payload.get("sender_id") or "").strip()
                )
                evidence_by_claim[int(row["claim_id"])].append(payload)

        history_rows = await self._fetchall(
            """
            SELECT
                h.*,
                COALESCE(
                    NULLIF(json_extract(c.payload_json, '$.attribute_label'), ''),
                    NULLIF(json_extract(c.payload_json, '$.label'), ''),
                    h.attribute_type
                ) AS attribute_label,
                c.normalized_value AS claim_normalized_value,
                c.status AS claim_status,
                p.normalized_value AS previous_claim_normalized_value,
                p.status AS previous_claim_status
            FROM profile_attribute_history h
            LEFT JOIN profile_claims c
                ON c.id = h.claim_id
            LEFT JOIN profile_claims p
                ON p.id = h.previous_claim_id
            WHERE h.platform_id = ? AND h.group_id = ? AND h.subject_user_id = ?
            ORDER BY h.created_at DESC, h.id DESC
            LIMIT 40
            """,
            (platform_id, group_id, user_id),
        )

        return {
            "summary": dict(summary),
            "global_summary": dict(global_summary) if global_summary is not None else None,
            "daily_stats": [dict(row) for row in reversed(daily_rows)],
            "outgoing_interactions": [dict(row) for row in outgoing_rows],
            "incoming_interactions": [dict(row) for row in incoming_rows],
            "attributes": [
                self._attribute_row_to_dict(row)
                for row in attribute_rows
            ],
            "claims": [
                {
                    **self._claim_row_to_dict(row),
                    "evidence_count": int(row["evidence_count"] or 0),
                    "evidence": evidence_by_claim.get(int(row["id"]), []),
                }
                for row in claim_rows
            ],
            "attribute_history": [
                self._attribute_history_row_to_dict(row)
                for row in history_rows
            ],
        }

    async def create_profile_message_blocks(
        self,
        *,
        batch_message_limit: int,
        min_batch_messages: int,
        batch_overlap: int,
        max_blocks: int = 8,
    ) -> int:
        await self.initialize()
        assert self._conn is not None

        limit = max(int(batch_message_limit or 40), 8)
        min_count = min(max(int(min_batch_messages or 12), 4), limit)
        overlap = min(max(int(batch_overlap or 0), 0), limit - 1)
        advance = max(limit - overlap, 1)
        max_blocks = max(int(max_blocks or 8), 1)
        now = int(time.time())
        created = 0

        groups = await self._fetchall(
            """
            SELECT
                platform_id,
                group_id,
                MAX(COALESCE(group_name, '')) AS group_name
            FROM archived_messages
            WHERE direction = 'incoming'
            GROUP BY platform_id, group_id
            ORDER BY platform_id ASC, group_id ASC
            """,
            (),
        )

        for group in groups:
            if created >= max_blocks:
                break
            platform_id = str(group["platform_id"] or "")
            group_id = str(group["group_id"] or "")
            group_name = str(group["group_name"] or "")
            cursor_key = f"profile_pipeline_cursor:{platform_id}:{group_id}"
            cursor_value = await self._get_pipeline_state_value(cursor_key)
            cursor_id = int(cursor_value or 0)

            while created < max_blocks:
                rows = await self._fetchall(
                    """
                    SELECT
                        id,
                        event_time,
                        group_name,
                        plain_text
                    FROM archived_messages
                    WHERE direction = 'incoming'
                      AND platform_id = ?
                      AND group_id = ?
                      AND id > ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (platform_id, group_id, cursor_id, limit),
                )
                if len(rows) < min_count:
                    break

                first_row = rows[0]
                last_row = rows[-1]
                first_id = int(first_row["id"])
                last_id = int(last_row["id"])
                message_row_ids = [int(row["id"]) for row in rows]
                approx_chars = sum(len(str(row["plain_text"] or "")) for row in rows)
                block_key = f"{platform_id}:{group_id}:{first_id}:{last_id}"

                existing = await self._fetchone(
                    """
                    SELECT id
                    FROM profile_message_blocks
                    WHERE block_key = ?
                    """,
                    (block_key,),
                )
                block_id: int
                if existing is None:
                    cursor = await self._conn.execute(
                        """
                        INSERT INTO profile_message_blocks (
                            block_key,
                            platform_id,
                            group_id,
                            group_name,
                            start_message_row_id,
                            end_message_row_id,
                            message_row_ids_json,
                            context_message_row_ids_json,
                            message_count,
                            approx_text_chars,
                            first_event_at,
                            last_event_at,
                            status,
                            created_at,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            block_key,
                            platform_id,
                            group_id,
                            str(last_row["group_name"] or group_name or ""),
                            first_id,
                            last_id,
                            self._to_json(message_row_ids),
                            self._to_json(message_row_ids),
                            len(message_row_ids),
                            approx_chars,
                            int(first_row["event_time"] or now),
                            int(last_row["event_time"] or now),
                            "pending",
                            now,
                            now,
                        ),
                    )
                    block_id = int(cursor.lastrowid)
                    await self._conn.execute(
                        """
                        INSERT INTO profile_extraction_jobs (
                            block_id,
                            status,
                            priority,
                            scheduled_at,
                            updated_at
                        ) VALUES (?, 'pending', 100, ?, ?)
                        """,
                        (block_id, now, now),
                    )
                    created += 1
                else:
                    block_id = int(existing["id"])
                    cursor = await self._conn.execute(
                        """
                        INSERT OR IGNORE INTO profile_extraction_jobs (
                            block_id,
                            status,
                            priority,
                            scheduled_at,
                            updated_at
                        ) VALUES (?, 'pending', 100, ?, ?)
                        """,
                        (block_id, now, now),
                    )
                    created += max(int(cursor.rowcount or 0), 0)

                cursor_index = min(advance, len(rows)) - 1
                cursor_id = int(rows[cursor_index]["id"])
                await self._set_pipeline_state_value(cursor_key, str(cursor_id), now=now)

                if len(rows) < limit:
                    break

        if created:
            await self._conn.commit()
        return created

    async def claim_next_profile_job(self) -> dict[str, object] | None:
        await self.initialize()
        assert self._conn is not None

        row = await self._fetchone(
            """
            SELECT
                j.*,
                b.block_key,
                b.platform_id,
                b.group_id,
                b.group_name
            FROM profile_extraction_jobs j
            JOIN profile_message_blocks b ON b.id = j.block_id
            WHERE j.status IN ('pending', 'failed')
            ORDER BY j.priority ASC, j.scheduled_at ASC, j.id ASC
            LIMIT 1
            """,
            (),
        )
        if row is None:
            return None

        now = int(time.time())
        await self._conn.execute(
            """
            UPDATE profile_extraction_jobs
            SET status = 'running',
                attempt_count = attempt_count + 1,
                started_at = ?,
                updated_at = ?,
                last_error = ''
            WHERE id = ?
            """,
            (now, now, int(row["id"])),
        )
        await self._conn.execute(
            """
            UPDATE profile_message_blocks
            SET status = 'running',
                updated_at = ?
            WHERE id = ?
            """,
            (now, int(row["block_id"])),
        )
        await self._conn.commit()
        payload = dict(row)
        payload["workflow_state"] = self._from_json(payload.pop("workflow_state_json", None)) or {}
        payload["result_summary"] = self._from_json(payload.pop("result_summary_json", None)) or {}
        return payload

    async def update_profile_job_progress(
        self,
        *,
        job_id: int,
        stage: str,
        stage_detail: str = "",
        workflow_state: dict[str, object] | None = None,
    ):
        await self.initialize()
        assert self._conn is not None

        now = int(time.time())
        progress_payload = dict(workflow_state or {})
        progress_payload["current_stage"] = str(stage or "").strip()
        progress_payload["stage_detail"] = str(stage_detail or "").strip()
        progress_payload["progress_updated_at"] = now
        await self._conn.execute(
            """
            UPDATE profile_extraction_jobs
            SET updated_at = ?,
                workflow_state_json = ?
            WHERE id = ?
            """,
            (now, self._to_json(progress_payload), int(job_id)),
        )
        await self._conn.execute(
            """
            UPDATE profile_message_blocks
            SET updated_at = ?
            WHERE id = (
                SELECT block_id
                FROM profile_extraction_jobs
                WHERE id = ?
            )
            """,
            (now, int(job_id)),
        )
        await self._conn.commit()

    async def recover_stale_profile_jobs(
        self,
        *,
        timeout_sec: int,
        force: bool = False,
    ) -> int:
        await self.initialize()
        assert self._conn is not None

        now = int(time.time())
        cutoff = now - max(int(timeout_sec or 0), 1)
        if force:
            rows = await self._fetchall(
                """
                SELECT id, block_id
                FROM profile_extraction_jobs
                WHERE status = 'running'
                """,
                (),
            )
            reason = "recovered running profile job after plugin startup"
        else:
            rows = await self._fetchall(
                """
                SELECT id, block_id
                FROM profile_extraction_jobs
                WHERE status = 'running'
                  AND COALESCE(started_at, updated_at, 0) <= ?
                """,
                (cutoff,),
            )
            reason = f"recovered stale running profile job after {timeout_sec}s"

        if not rows:
            return 0

        job_ids = [int(row["id"]) for row in rows]
        block_ids = [int(row["block_id"]) for row in rows]
        job_placeholders = ", ".join("?" for _ in job_ids)
        block_placeholders = ", ".join("?" for _ in block_ids)

        await self._conn.execute(
            f"""
            UPDATE profile_extraction_jobs
            SET status = 'failed',
                finished_at = ?,
                updated_at = ?,
                last_error = ?
            WHERE id IN ({job_placeholders})
            """,
            tuple([now, now, reason, *job_ids]),
        )
        await self._conn.execute(
            f"""
            UPDATE profile_message_blocks
            SET status = 'failed',
                updated_at = ?
            WHERE id IN ({block_placeholders})
            """,
            tuple([now, *block_ids]),
        )
        await self._conn.commit()
        return len(job_ids)

    async def get_profile_job_context(self, job_id: int) -> dict[str, object] | None:
        await self.initialize()
        assert self._conn is not None

        row = await self._fetchone(
            """
            SELECT
                j.*,
                b.block_key,
                b.platform_id,
                b.group_id,
                b.group_name,
                b.message_row_ids_json,
                b.context_message_row_ids_json,
                b.message_count,
                b.approx_text_chars,
                b.first_event_at,
                b.last_event_at
            FROM profile_extraction_jobs j
            JOIN profile_message_blocks b ON b.id = j.block_id
            WHERE j.id = ?
            """,
            (int(job_id),),
        )
        if row is None:
            return None

        payload = dict(row)
        message_row_ids = [
            int(value)
            for value in self._from_json(payload.get("message_row_ids_json")) or []
            if str(value).strip()
        ]
        messages = await self._fetch_messages_by_ids(message_row_ids)

        payload["workflow_state"] = self._from_json(payload.pop("workflow_state_json", None)) or {}
        payload["result_summary"] = self._from_json(payload.pop("result_summary_json", None)) or {}
        payload["message_row_ids"] = message_row_ids
        payload["messages"] = messages
        payload.pop("message_row_ids_json", None)
        payload.pop("context_message_row_ids_json", None)
        return payload

    async def get_profile_resolution_context(
        self,
        *,
        platform_id: str,
        group_id: str,
        subject_user_ids: list[str],
        attribute_types: list[str],
        limit_per_attribute: int = 8,
    ) -> dict[str, object]:
        await self.initialize()
        assert self._conn is not None

        users = [str(value).strip() for value in subject_user_ids if str(value).strip()]
        attrs = [str(value).strip() for value in attribute_types if str(value).strip()]
        if not users:
            return {"attributes": [], "recent_claims": []}

        user_placeholders = ", ".join("?" for _ in users)
        context_limit = max(limit_per_attribute * len(users) * max(len(attrs), 4), 24)

        attribute_rows = await self._fetchall(
            f"""
            SELECT *
            FROM profile_attributes
            WHERE platform_id = ?
              AND group_id = ?
              AND subject_user_id IN ({user_placeholders})
            ORDER BY updated_at DESC, attribute_type ASC
            LIMIT ?
            """,
            tuple([platform_id, group_id, *users, context_limit]),
        )
        claim_rows = await self._fetchall(
            f"""
            SELECT *
            FROM profile_claims
            WHERE platform_id = ?
              AND group_id = ?
              AND subject_user_id IN ({user_placeholders})
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            tuple([platform_id, group_id, *users, context_limit]),
        )
        return {
            "attributes": [
                self._attribute_context_row_to_dict(row)
                for row in attribute_rows
            ],
            "recent_claims": [self._claim_row_to_dict(row) for row in claim_rows],
        }

    async def apply_profile_resolution(
        self,
        *,
        job_id: int,
        resolved_claims: list[dict[str, object]],
        resolution_actions: list[dict[str, object]] | None = None,
        summary: dict[str, object] | None = None,
        workflow_state: dict[str, object] | None = None,
        block_messages: list[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        await self.initialize()
        assert self._conn is not None

        job_context = await self.get_profile_job_context(job_id)
        if job_context is None:
            return {"inserted_claims": 0, "updated_attributes": 0}

        now = int(time.time())
        platform_id = str(job_context.get("platform_id") or "")
        group_id = str(job_context.get("group_id") or "")
        group_name = str(job_context.get("group_name") or "")
        messages = list(block_messages or job_context.get("messages") or [])
        message_time_map = {
            int(message["id"]): int(message.get("event_time") or now)
            for message in messages
            if str(message.get("id", "")).strip()
        }

        inserted_claims = 0
        updated_attributes = 0
        action_result = {"actions_applied": 0, "claims_rewritten": 0, "attributes_rewritten": 0}

        for payload in resolved_claims:
            subject_user_id = str(payload.get("subject_user_id") or "").strip()
            attribute_type = str(payload.get("attribute_type") or "").strip()
            normalized_value = str(payload.get("normalized_value") or "").strip()
            if not subject_user_id or not attribute_type or not normalized_value:
                continue

            evidence_ids = sorted(
                {
                    int(value)
                    for value in payload.get("evidence_message_row_ids", []) or []
                    if str(value).strip()
                }
            )
            evidence_times = [
                message_time_map.get(message_id, now) for message_id in evidence_ids
            ]
            first_seen_at = min(evidence_times or [now])
            last_seen_at = max(evidence_times or [now])
            status = str(payload.get("status") or "candidate")
            current_value = bool(payload.get("current_value", False))
            attribute_label = self._attribute_label_from_payload(payload)
            payload_json = dict(payload.get("payload") or {})
            if attribute_label:
                payload_json["attribute_label"] = attribute_label

            cursor = await self._conn.execute(
                """
                INSERT INTO profile_claims (
                    platform_id,
                    group_id,
                    group_name,
                    subject_user_id,
                    source_message_row_id,
                    attribute_type,
                    raw_value,
                    normalized_value,
                    source_kind,
                    tense,
                    polarity,
                    confidence,
                    status,
                    resolver_note,
                    first_seen_at,
                    last_seen_at,
                    created_at,
                    updated_at,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    platform_id,
                    group_id,
                    group_name,
                    subject_user_id,
                    evidence_ids[0] if evidence_ids else None,
                    attribute_type,
                    str(payload.get("raw_value") or ""),
                    normalized_value,
                    str(payload.get("source_kind") or "unknown"),
                    str(payload.get("tense") or "unknown"),
                    str(payload.get("polarity") or "affirmed"),
                    float(payload.get("confidence", 0.0) or 0.0),
                    status,
                    str(payload.get("note") or ""),
                    first_seen_at,
                    last_seen_at,
                    now,
                    now,
                    self._to_json(payload_json),
                ),
            )
            claim_id = int(cursor.lastrowid)
            inserted_claims += 1

            for superseded_id in payload.get("supersedes_claim_ids", []) or []:
                if not str(superseded_id).strip():
                    continue
                await self._conn.execute(
                    """
                    UPDATE profile_claims
                    SET status = 'outdated',
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, int(superseded_id)),
                )

            for message_id in evidence_ids:
                await self._conn.execute(
                    """
                    INSERT OR IGNORE INTO profile_claim_evidence (
                        claim_id,
                        message_row_id,
                        excerpt,
                        evidence_kind,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        claim_id,
                        message_id,
                        str(payload.get("evidence_excerpt") or ""),
                        "message",
                        now,
                    ),
                )

            if not current_value:
                continue

            previous = await self._fetchone(
                """
                SELECT current_claim_id
                FROM profile_attributes
                WHERE platform_id = ?
                  AND group_id = ?
                  AND subject_user_id = ?
                  AND attribute_type = ?
                """,
                (platform_id, group_id, subject_user_id, attribute_type),
            )
            previous_claim_id = (
                int(previous["current_claim_id"])
                if previous is not None and previous["current_claim_id"] is not None
                else None
            )
            await self._conn.execute(
                """
                INSERT INTO profile_attributes (
                    platform_id,
                    group_id,
                    group_name,
                    subject_user_id,
                    attribute_type,
                    current_claim_id,
                    current_value,
                    normalized_value,
                    confidence,
                    source_kind,
                    first_seen_at,
                    last_seen_at,
                    evidence_count,
                    updated_at,
                    status,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform_id, group_id, subject_user_id, attribute_type)
                DO UPDATE SET
                    group_name = excluded.group_name,
                    current_claim_id = excluded.current_claim_id,
                    current_value = excluded.current_value,
                    normalized_value = excluded.normalized_value,
                    confidence = excluded.confidence,
                    source_kind = excluded.source_kind,
                    first_seen_at = MIN(profile_attributes.first_seen_at, excluded.first_seen_at),
                    last_seen_at = MAX(profile_attributes.last_seen_at, excluded.last_seen_at),
                    evidence_count = excluded.evidence_count,
                    updated_at = excluded.updated_at,
                    status = excluded.status,
                    payload_json = excluded.payload_json
                """,
                (
                    platform_id,
                    group_id,
                    group_name,
                    subject_user_id,
                    attribute_type,
                    claim_id,
                    str(payload.get("raw_value") or ""),
                    normalized_value,
                    float(payload.get("confidence", 0.0) or 0.0),
                    str(payload.get("source_kind") or "unknown"),
                    first_seen_at,
                    last_seen_at,
                    len(evidence_ids),
                    now,
                    status,
                    self._to_json({"attribute_label": attribute_label} if attribute_label else {}),
                ),
            )
            await self._conn.execute(
                """
                INSERT INTO profile_attribute_history (
                    platform_id,
                    group_id,
                    subject_user_id,
                    attribute_type,
                    claim_id,
                    previous_claim_id,
                    action,
                    created_at,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    platform_id,
                    group_id,
                    subject_user_id,
                    attribute_type,
                    claim_id,
                    previous_claim_id,
                    "set_current",
                    now,
                    self._to_json(
                        {
                            "status": status,
                            "note": str(payload.get("note") or ""),
                        }
                    ),
                ),
            )
            updated_attributes += 1

        action_result = await self._apply_profile_resolution_actions(
            platform_id=platform_id,
            group_id=group_id,
            group_name=group_name,
            actions=list(resolution_actions or []),
            now=now,
        )

        await self._conn.execute(
            """
            UPDATE profile_extraction_jobs
            SET status = 'completed',
                finished_at = ?,
                updated_at = ?,
                result_summary_json = ?,
                workflow_state_json = ?
            WHERE id = ?
            """,
            (
                now,
                now,
                self._to_json(
                    {
                        **dict(summary or {}),
                        "inserted_claims": inserted_claims,
                        "updated_attributes": updated_attributes,
                        **action_result,
                    }
                ),
                self._to_json(workflow_state),
                int(job_id),
            ),
        )
        await self._conn.execute(
            """
            UPDATE profile_message_blocks
            SET status = 'completed',
                updated_at = ?
            WHERE id = (
                SELECT block_id
                FROM profile_extraction_jobs
                WHERE id = ?
            )
            """,
            (now, int(job_id)),
        )
        await self._conn.commit()
        return {
            "inserted_claims": inserted_claims,
            "updated_attributes": updated_attributes,
            **action_result,
        }

    async def _apply_profile_resolution_actions(
        self,
        *,
        platform_id: str,
        group_id: str,
        group_name: str,
        actions: list[dict[str, object]],
        now: int,
    ) -> dict[str, int]:
        result = {
            "actions_applied": 0,
            "claims_rewritten": 0,
            "attributes_rewritten": 0,
        }
        for action in actions:
            action_type = str(
                action.get("type")
                or action.get("action_type")
                or ""
            ).strip()
            if not action_type:
                continue

            if action_type in {"set_claim_status", "mark_claim_status", "update_claim_status"}:
                affected = await self._apply_claim_status_action(
                    platform_id=platform_id,
                    group_id=group_id,
                    group_name=group_name,
                    action=action,
                    now=now,
                )
                if affected:
                    result["actions_applied"] += 1
                    result["claims_rewritten"] += affected
                continue

            if action_type in {"rename_attribute", "canonicalize_attribute", "merge_attribute"}:
                affected = await self._apply_attribute_rewrite_action(
                    platform_id=platform_id,
                    group_id=group_id,
                    group_name=group_name,
                    action=action,
                    now=now,
                )
                if affected["attributes"] or affected["claims"]:
                    result["actions_applied"] += 1
                    result["claims_rewritten"] += affected["claims"]
                    result["attributes_rewritten"] += affected["attributes"]
                if action.get("claim_ids") and str(action.get("status") or "").strip():
                    status_affected = await self._apply_claim_status_action(
                        platform_id=platform_id,
                        group_id=group_id,
                        group_name=group_name,
                        action=action,
                        now=now,
                    )
                    result["claims_rewritten"] += status_affected
        return result

    async def _apply_claim_status_action(
        self,
        *,
        platform_id: str,
        group_id: str,
        group_name: str,
        action: dict[str, object],
        now: int,
    ) -> int:
        raw_claim_ids = action.get("claim_ids", []) or []
        if isinstance(raw_claim_ids, (str, int)):
            raw_claim_ids = [raw_claim_ids]
        claim_ids = [
            int(value)
            for value in raw_claim_ids
            if str(value).strip()
        ]
        status = str(action.get("status") or "").strip()
        if not claim_ids or not status:
            return 0

        placeholders = ", ".join("?" for _ in claim_ids)
        rows = await self._fetchall(
            f"""
            SELECT id, subject_user_id, attribute_type, resolver_note, payload_json
            FROM profile_claims
            WHERE platform_id = ?
              AND group_id = ?
              AND id IN ({placeholders})
            """,
            tuple([platform_id, group_id, *claim_ids]),
        )
        if not rows:
            return 0

        reason = str(action.get("reason") or "").strip()
        for row in rows:
            payload = self._json_dict(row["payload_json"])
            payload["last_resolution_action"] = {
                "type": str(action.get("type") or action.get("action_type") or ""),
                "status": status,
                "reason": reason,
                "updated_at": now,
            }
            resolver_note = reason or str(row["resolver_note"] or "")
            await self._conn.execute(
                """
                UPDATE profile_claims
                SET status = ?,
                    resolver_note = ?,
                    updated_at = ?,
                    payload_json = ?
                WHERE id = ?
                """,
                (status, resolver_note, now, self._to_json(payload), int(row["id"])),
            )
            await self._conn.execute(
                """
                INSERT INTO profile_attribute_history (
                    platform_id,
                    group_id,
                    subject_user_id,
                    attribute_type,
                    claim_id,
                    previous_claim_id,
                    action,
                    created_at,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    platform_id,
                    group_id,
                    str(row["subject_user_id"] or ""),
                    str(row["attribute_type"] or ""),
                    int(row["id"]),
                    None,
                    f"set_claim_status:{status}",
                    now,
                    self._to_json(
                        {
                            "status": status,
                            "reason": reason,
                            "group_name": group_name,
                        }
                    ),
                ),
            )

        await self._conn.execute(
            f"""
            UPDATE profile_attributes
            SET status = ?,
                updated_at = ?
            WHERE platform_id = ?
              AND group_id = ?
              AND current_claim_id IN ({placeholders})
            """,
            tuple([status, now, platform_id, group_id, *claim_ids]),
        )
        return len(rows)

    async def _apply_attribute_rewrite_action(
        self,
        *,
        platform_id: str,
        group_id: str,
        group_name: str,
        action: dict[str, object],
        now: int,
    ) -> dict[str, int]:
        subject_user_id = str(action.get("subject_user_id") or "").strip()
        from_attribute_type = str(
            action.get("from_attribute_type")
            or action.get("source_attribute_type")
            or action.get("attribute_type")
            or ""
        ).strip()
        to_attribute_type = str(
            action.get("to_attribute_type")
            or action.get("target_attribute_type")
            or ""
        ).strip()
        if not subject_user_id or not from_attribute_type or not to_attribute_type:
            return {"claims": 0, "attributes": 0}
        if from_attribute_type == to_attribute_type:
            return {"claims": 0, "attributes": 0}

        action_type = str(action.get("type") or action.get("action_type") or "").strip()
        attribute_label = str(action.get("attribute_label") or "").strip()
        reason = str(action.get("reason") or "").strip()

        claim_rows = await self._fetchall(
            """
            SELECT id, payload_json
            FROM profile_claims
            WHERE platform_id = ?
              AND group_id = ?
              AND subject_user_id = ?
              AND attribute_type = ?
            """,
            (platform_id, group_id, subject_user_id, from_attribute_type),
        )
        for row in claim_rows:
            payload = self._json_dict(row["payload_json"])
            if attribute_label:
                payload["attribute_label"] = attribute_label
            payload["last_resolution_action"] = {
                "type": action_type,
                "from_attribute_type": from_attribute_type,
                "to_attribute_type": to_attribute_type,
                "reason": reason,
                "updated_at": now,
            }
            await self._conn.execute(
                """
                UPDATE profile_claims
                SET attribute_type = ?,
                    updated_at = ?,
                    payload_json = ?
                WHERE id = ?
                """,
                (to_attribute_type, now, self._to_json(payload), int(row["id"])),
            )

        source = await self._fetchone(
            """
            SELECT *
            FROM profile_attributes
            WHERE platform_id = ?
              AND group_id = ?
              AND subject_user_id = ?
              AND attribute_type = ?
            """,
            (platform_id, group_id, subject_user_id, from_attribute_type),
        )
        target = await self._fetchone(
            """
            SELECT *
            FROM profile_attributes
            WHERE platform_id = ?
              AND group_id = ?
              AND subject_user_id = ?
              AND attribute_type = ?
            """,
            (platform_id, group_id, subject_user_id, to_attribute_type),
        )

        attributes_rewritten = 0
        claim_id_for_history: int | None = None
        previous_claim_id: int | None = None
        if source is None and not claim_rows:
            return {"claims": 0, "attributes": 0}
        if source is not None:
            source_payload = self._json_dict(source["payload_json"])
            if attribute_label:
                source_payload["attribute_label"] = attribute_label
            source_payload["last_resolution_action"] = {
                "type": action_type,
                "from_attribute_type": from_attribute_type,
                "to_attribute_type": to_attribute_type,
                "reason": reason,
                "updated_at": now,
            }
            claim_id_for_history = (
                int(source["current_claim_id"])
                if source["current_claim_id"] is not None
                else None
            )
            if target is None:
                await self._conn.execute(
                    """
                    UPDATE profile_attributes
                    SET attribute_type = ?,
                        updated_at = ?,
                        payload_json = ?
                    WHERE platform_id = ?
                      AND group_id = ?
                      AND subject_user_id = ?
                      AND attribute_type = ?
                    """,
                    (
                        to_attribute_type,
                        now,
                        self._to_json(source_payload),
                        platform_id,
                        group_id,
                        subject_user_id,
                        from_attribute_type,
                    ),
                )
            else:
                previous_claim_id = (
                    int(target["current_claim_id"])
                    if target["current_claim_id"] is not None
                    else None
                )
                use_source = int(source["updated_at"] or 0) >= int(target["updated_at"] or 0)
                if use_source:
                    await self._conn.execute(
                        """
                        UPDATE profile_attributes
                        SET group_name = ?,
                            current_claim_id = ?,
                            current_value = ?,
                            normalized_value = ?,
                            confidence = ?,
                            source_kind = ?,
                            first_seen_at = MIN(first_seen_at, ?),
                            last_seen_at = MAX(last_seen_at, ?),
                            evidence_count = ?,
                            updated_at = ?,
                            status = ?,
                            payload_json = ?
                        WHERE platform_id = ?
                          AND group_id = ?
                          AND subject_user_id = ?
                          AND attribute_type = ?
                        """,
                        (
                            str(source["group_name"] or group_name or ""),
                            source["current_claim_id"],
                            str(source["current_value"] or ""),
                            str(source["normalized_value"] or ""),
                            float(source["confidence"] or 0.0),
                            str(source["source_kind"] or "unknown"),
                            int(source["first_seen_at"] or now),
                            int(source["last_seen_at"] or now),
                            int(source["evidence_count"] or 0),
                            now,
                            str(source["status"] or "candidate"),
                            self._to_json(source_payload),
                            platform_id,
                            group_id,
                            subject_user_id,
                            to_attribute_type,
                        ),
                    )
                else:
                    target_payload = self._json_dict(target["payload_json"])
                    if attribute_label:
                        target_payload["attribute_label"] = attribute_label
                    target_payload["last_resolution_action"] = {
                        "type": action_type,
                        "from_attribute_type": from_attribute_type,
                        "to_attribute_type": to_attribute_type,
                        "reason": reason,
                        "updated_at": now,
                    }
                    await self._conn.execute(
                        """
                        UPDATE profile_attributes
                        SET updated_at = ?,
                            payload_json = ?
                        WHERE platform_id = ?
                          AND group_id = ?
                          AND subject_user_id = ?
                          AND attribute_type = ?
                        """,
                        (
                            now,
                            self._to_json(target_payload),
                            platform_id,
                            group_id,
                            subject_user_id,
                            to_attribute_type,
                        ),
                    )
                await self._conn.execute(
                    """
                    DELETE FROM profile_attributes
                    WHERE platform_id = ?
                      AND group_id = ?
                      AND subject_user_id = ?
                      AND attribute_type = ?
                    """,
                    (platform_id, group_id, subject_user_id, from_attribute_type),
                )
            attributes_rewritten = 1

        await self._conn.execute(
            """
            INSERT INTO profile_attribute_history (
                platform_id,
                group_id,
                subject_user_id,
                attribute_type,
                claim_id,
                previous_claim_id,
                action,
                created_at,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                platform_id,
                group_id,
                subject_user_id,
                to_attribute_type,
                claim_id_for_history,
                previous_claim_id,
                action_type,
                now,
                self._to_json(
                    {
                        "from_attribute_type": from_attribute_type,
                        "to_attribute_type": to_attribute_type,
                        "attribute_label": attribute_label,
                        "reason": reason,
                        "rewritten_claim_count": len(claim_rows),
                    }
                ),
            ),
        )
        return {"claims": len(claim_rows), "attributes": attributes_rewritten}

    async def complete_profile_job(
        self,
        *,
        job_id: int,
        summary: dict[str, object] | None = None,
        workflow_state: dict[str, object] | None = None,
        block_status: str = "completed",
    ):
        await self.initialize()
        assert self._conn is not None

        now = int(time.time())
        await self._conn.execute(
            """
            UPDATE profile_extraction_jobs
            SET status = 'completed',
                finished_at = ?,
                updated_at = ?,
                result_summary_json = ?,
                workflow_state_json = ?
            WHERE id = ?
            """,
            (
                now,
                now,
                self._to_json(summary),
                self._to_json(workflow_state),
                int(job_id),
            ),
        )
        await self._conn.execute(
            """
            UPDATE profile_message_blocks
            SET status = ?,
                updated_at = ?
            WHERE id = (
                SELECT block_id
                FROM profile_extraction_jobs
                WHERE id = ?
            )
            """,
            (block_status, now, int(job_id)),
        )
        await self._conn.commit()

    async def fail_profile_job(
        self,
        *,
        job_id: int,
        error_text: str,
        workflow_state: dict[str, object] | None = None,
    ):
        await self.initialize()
        assert self._conn is not None

        now = int(time.time())
        existing = await self._fetchone(
            """
            SELECT workflow_state_json
            FROM profile_extraction_jobs
            WHERE id = ?
            """,
            (int(job_id),),
        )
        existing_state = (
            self._from_json(existing["workflow_state_json"])
            if existing is not None
            else None
        )
        if not isinstance(existing_state, dict):
            existing_state = {}
        merged_state = {
            **existing_state,
            **dict(workflow_state or {}),
            "current_stage": "failed",
            "stage_detail": error_text[:300],
            "progress_updated_at": now,
        }
        await self._conn.execute(
            """
            UPDATE profile_extraction_jobs
            SET status = 'failed',
                finished_at = ?,
                updated_at = ?,
                last_error = ?,
                workflow_state_json = ?
            WHERE id = ?
            """,
            (now, now, error_text[:1000], self._to_json(merged_state), int(job_id)),
        )
        await self._conn.execute(
            """
            UPDATE profile_message_blocks
            SET status = 'failed',
                updated_at = ?
            WHERE id = (
                SELECT block_id
                FROM profile_extraction_jobs
                WHERE id = ?
            )
            """,
            (now, int(job_id)),
        )
        await self._conn.commit()

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

            CREATE TABLE IF NOT EXISTS profile_pipeline_state (
                state_key TEXT PRIMARY KEY,
                state_value TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS profile_message_blocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_key TEXT NOT NULL UNIQUE,
                platform_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                group_name TEXT NOT NULL DEFAULT '',
                start_message_row_id INTEGER NOT NULL,
                end_message_row_id INTEGER NOT NULL,
                message_row_ids_json TEXT NOT NULL DEFAULT '[]',
                context_message_row_ids_json TEXT NOT NULL DEFAULT '[]',
                message_count INTEGER NOT NULL DEFAULT 0,
                approx_text_chars INTEGER NOT NULL DEFAULT 0,
                first_event_at INTEGER NOT NULL,
                last_event_at INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_profile_message_blocks_scope
            ON profile_message_blocks (platform_id, group_id, status, last_event_at DESC);

            CREATE TABLE IF NOT EXISTS profile_extraction_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_id INTEGER NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'pending',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                priority INTEGER NOT NULL DEFAULT 100,
                judge_model TEXT NOT NULL DEFAULT '',
                extract_model TEXT NOT NULL DEFAULT '',
                resolve_model TEXT NOT NULL DEFAULT '',
                scheduled_at INTEGER NOT NULL,
                started_at INTEGER,
                finished_at INTEGER,
                updated_at INTEGER NOT NULL,
                last_error TEXT NOT NULL DEFAULT '',
                workflow_state_json TEXT NOT NULL DEFAULT '{}',
                result_summary_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY(block_id) REFERENCES profile_message_blocks(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_profile_extraction_jobs_status
            ON profile_extraction_jobs (status, priority, scheduled_at);

            CREATE TABLE IF NOT EXISTS profile_claims (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                group_name TEXT NOT NULL DEFAULT '',
                subject_user_id TEXT NOT NULL,
                source_message_row_id INTEGER,
                attribute_type TEXT NOT NULL,
                raw_value TEXT NOT NULL DEFAULT '',
                normalized_value TEXT NOT NULL DEFAULT '',
                source_kind TEXT NOT NULL DEFAULT 'unknown',
                tense TEXT NOT NULL DEFAULT 'unknown',
                polarity TEXT NOT NULL DEFAULT 'affirmed',
                confidence REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'candidate',
                resolver_note TEXT NOT NULL DEFAULT '',
                first_seen_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY(source_message_row_id) REFERENCES archived_messages(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_profile_claims_lookup
            ON profile_claims (platform_id, group_id, subject_user_id, attribute_type, status, updated_at DESC);

            CREATE TABLE IF NOT EXISTS profile_claim_evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                claim_id INTEGER NOT NULL,
                message_row_id INTEGER NOT NULL,
                excerpt TEXT NOT NULL DEFAULT '',
                evidence_kind TEXT NOT NULL DEFAULT 'message',
                created_at INTEGER NOT NULL,
                FOREIGN KEY(claim_id) REFERENCES profile_claims(id) ON DELETE CASCADE,
                FOREIGN KEY(message_row_id) REFERENCES archived_messages(id) ON DELETE CASCADE,
                UNIQUE(claim_id, message_row_id, evidence_kind, excerpt)
            );

            CREATE INDEX IF NOT EXISTS idx_profile_claim_evidence_claim
            ON profile_claim_evidence (claim_id, message_row_id);

            CREATE TABLE IF NOT EXISTS profile_attributes (
                platform_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                group_name TEXT NOT NULL DEFAULT '',
                subject_user_id TEXT NOT NULL,
                attribute_type TEXT NOT NULL,
                current_claim_id INTEGER,
                current_value TEXT NOT NULL DEFAULT '',
                normalized_value TEXT NOT NULL DEFAULT '',
                confidence REAL NOT NULL DEFAULT 0,
                source_kind TEXT NOT NULL DEFAULT 'unknown',
                first_seen_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL,
                evidence_count INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'candidate',
                payload_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY (platform_id, group_id, subject_user_id, attribute_type),
                FOREIGN KEY(current_claim_id) REFERENCES profile_claims(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_profile_attributes_subject
            ON profile_attributes (platform_id, group_id, subject_user_id, updated_at DESC);

            CREATE TABLE IF NOT EXISTS profile_attribute_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                subject_user_id TEXT NOT NULL,
                attribute_type TEXT NOT NULL,
                claim_id INTEGER,
                previous_claim_id INTEGER,
                action TEXT NOT NULL DEFAULT 'set_current',
                created_at INTEGER NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY(claim_id) REFERENCES profile_claims(id) ON DELETE SET NULL,
                FOREIGN KEY(previous_claim_id) REFERENCES profile_claims(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_profile_attribute_history_subject
            ON profile_attribute_history (platform_id, group_id, subject_user_id, attribute_type, created_at DESC);
            """
        )
        await self._ensure_column(
            "profile_attributes",
            "payload_json",
            "TEXT NOT NULL DEFAULT '{}'",
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

    async def _get_pipeline_state_value(self, state_key: str) -> str | None:
        row = await self._fetchone(
            """
            SELECT state_value
            FROM profile_pipeline_state
            WHERE state_key = ?
            """,
            (state_key,),
        )
        if row is None:
            return None
        return str(row["state_value"] or "")

    async def _set_pipeline_state_value(
        self,
        state_key: str,
        state_value: str,
        *,
        now: int | None = None,
    ):
        assert self._conn is not None
        stamp = int(now or time.time())
        await self._conn.execute(
            """
            INSERT INTO profile_pipeline_state (
                state_key,
                state_value,
                updated_at
            ) VALUES (?, ?, ?)
            ON CONFLICT(state_key) DO UPDATE SET
                state_value = excluded.state_value,
                updated_at = excluded.updated_at
            """,
            (state_key, state_value, stamp),
        )

    async def _fetch_messages_by_ids(
        self,
        message_row_ids: list[int],
    ) -> list[dict[str, object]]:
        if not message_row_ids:
            return []
        placeholders = ", ".join("?" for _ in message_row_ids)
        message_rows = await self._fetchall(
            f"""
            SELECT *
            FROM archived_messages
            WHERE id IN ({placeholders})
            """,
            tuple(message_row_ids),
        )
        if not message_rows:
            return []

        segment_rows = await self._fetchall(
            f"""
            SELECT *
            FROM archived_segments
            WHERE message_row_id IN ({placeholders})
            ORDER BY message_row_id ASC, seg_index ASC, id ASC
            """,
            tuple(message_row_ids),
        )
        segment_map: dict[int, list[dict[str, object]]] = defaultdict(list)
        for row in segment_rows:
            segment_map[int(row["message_row_id"])].append(self._segment_row_to_dict(row))

        message_map: dict[int, dict[str, object]] = {}
        for row in message_rows:
            payload = dict(row)
            payload["raw_event"] = self._from_json(payload.pop("raw_event_json", None))
            payload["segments"] = segment_map.get(int(payload["id"]), [])
            message_map[int(payload["id"])] = payload

        return [message_map[message_id] for message_id in message_row_ids if message_id in message_map]

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

    async def _ensure_column(self, table: str, column: str, definition: str):
        assert self._conn is not None
        async with self._conn.execute(f"PRAGMA table_info({table})") as cursor:
            rows = await cursor.fetchall()
        if any(str(row["name"]) == column for row in rows):
            return
        await self._conn.execute(
            f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
        )

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

    def _json_dict(self, value) -> dict[str, object]:
        payload = self._from_json(value)
        if isinstance(payload, dict):
            return dict(payload)
        return {}

    def _segment_row_to_dict(self, row) -> dict[str, object]:
        payload = dict(row)
        payload["seg_data"] = self._from_json(payload.pop("seg_data_json", None))
        return payload

    def _forward_node_row_to_dict(self, row) -> dict[str, object]:
        payload = dict(row)
        payload["content"] = self._from_json(payload.pop("content_json", None))
        return payload

    def _claim_row_to_dict(self, row) -> dict[str, object]:
        payload = dict(row)
        payload.setdefault("attribute_label", "")
        payload["payload"] = self._from_json(payload.pop("payload_json", None))
        if not payload["attribute_label"] and isinstance(payload["payload"], dict):
            payload["attribute_label"] = str(
                payload["payload"].get("attribute_label")
                or payload["payload"].get("label")
                or ""
            )
        return payload

    def _attribute_row_to_dict(self, row) -> dict[str, object]:
        payload = dict(row)
        payload.setdefault("attribute_label", "")
        claim_payload = self._from_json(payload.pop("claim_payload_json", None))
        current_claim_id = payload.get("current_claim_id")
        if current_claim_id is not None:
            payload["current_claim"] = {
                "id": int(current_claim_id),
                "raw_value": payload.pop("claim_raw_value", ""),
                "normalized_value": payload.pop("claim_normalized_value", ""),
                "source_kind": payload.pop("claim_source_kind", ""),
                "tense": payload.pop("claim_tense", ""),
                "polarity": payload.pop("claim_polarity", ""),
                "confidence": payload.pop("claim_confidence", 0.0),
                "status": payload.pop("claim_status", ""),
                "resolver_note": payload.pop("claim_resolver_note", ""),
                "first_seen_at": payload.pop("claim_first_seen_at", 0),
                "last_seen_at": payload.pop("claim_last_seen_at", 0),
                "updated_at": payload.pop("claim_updated_at", 0),
                "payload": claim_payload,
            }
        else:
            payload["current_claim"] = None
            payload.pop("claim_raw_value", None)
            payload.pop("claim_normalized_value", None)
            payload.pop("claim_source_kind", None)
            payload.pop("claim_tense", None)
            payload.pop("claim_polarity", None)
            payload.pop("claim_confidence", None)
            payload.pop("claim_status", None)
            payload.pop("claim_resolver_note", None)
            payload.pop("claim_first_seen_at", None)
            payload.pop("claim_last_seen_at", None)
            payload.pop("claim_updated_at", None)
        return payload

    def _attribute_context_row_to_dict(self, row) -> dict[str, object]:
        payload = dict(row)
        payload_json = self._from_json(payload.pop("payload_json", None))
        payload["payload"] = payload_json if isinstance(payload_json, dict) else {}
        payload["attribute_label"] = str(
            payload["payload"].get("attribute_label")
            or payload["payload"].get("label")
            or payload.get("attribute_type")
            or ""
        )
        return payload

    def _attribute_history_row_to_dict(self, row) -> dict[str, object]:
        payload = dict(row)
        payload.setdefault("attribute_label", "")
        payload["payload"] = self._from_json(payload.pop("payload_json", None))
        if not payload["attribute_label"] and isinstance(payload["payload"], dict):
            payload["attribute_label"] = str(
                payload["payload"].get("attribute_label")
                or payload["payload"].get("label")
                or ""
            )
        return payload

    @staticmethod
    def _attribute_label_from_payload(payload: dict[str, object]) -> str:
        label = str(payload.get("attribute_label") or "").strip()
        if label:
            return label
        nested = payload.get("payload")
        if isinstance(nested, dict):
            return str(
                nested.get("attribute_label")
                or nested.get("label")
                or ""
            ).strip()
        return ""
