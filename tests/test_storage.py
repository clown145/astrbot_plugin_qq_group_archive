import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.models import ArchivedMessage, ArchivedNoticeEvent, ArchivedSegment, ForwardNodeRecord
from src.storage import ArchiveDatabase


class ArchiveDatabaseQueryTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self._temp_dir.name) / "archive.db"
        self.db = ArchiveDatabase(db_path)
        await self.db.initialize()

        message = ArchivedMessage(
            platform_id="onebot",
            bot_self_id="bot-1",
            group_id="123456",
            session_id="onebot:GroupMessage:123456",
            group_name="测试群",
            message_id="1001",
            sender_id="2002",
            sender_name="Alice",
            sender_card="AliceCard",
            direction="incoming",
            post_type="message",
            message_sub_type="normal",
            plain_text="hello[image]",
            outline="hello[image]",
            event_time=1710000000,
            archived_at=1710000001,
            raw_event={"post_type": "message", "message_id": 1001},
            segments=[
                ArchivedSegment(
                    index=0,
                    segment_type="text",
                    raw_type="text",
                    text="hello",
                    data={"text": "hello"},
                ),
                ArchivedSegment(
                    index=1,
                    segment_type="image",
                    raw_type="image",
                    data={"url": "https://example.com/image.jpg"},
                    source_url="https://example.com/image.jpg",
                    local_path="media/image/20260417/abc.jpg",
                    media_status="stored",
                    file_size=1234,
                ),
            ],
        )
        self.message_row_id, _ = await self.db.insert_message(message)
        await self.db.insert_forward_nodes(
            message_row_id=self.message_row_id,
            nodes=[
                ForwardNodeRecord(
                    forward_id="forward-1",
                    node_index=0,
                    sender_id="3003",
                    sender_name="Bob",
                    sent_time=1710000002,
                    content_text="forward hello",
                    content_json={"content": [{"type": "text", "data": {"text": "forward hello"}}]},
                )
            ],
        )

        notice = ArchivedNoticeEvent(
            event_key="onebot|123456|group_recall|1710000003",
            platform_id="onebot",
            bot_self_id="bot-1",
            group_id="123456",
            session_id="onebot:GroupMessage:123456",
            group_name="测试群",
            notice_type="group_recall",
            sub_type=None,
            actor_user_id="2002",
            operator_id="2002",
            target_id=None,
            message_id="1001",
            reaction_code=None,
            reaction_count=None,
            event_time=1710000003,
            archived_at=1710000004,
            raw_event={"post_type": "notice", "notice_type": "group_recall"},
        )
        self.notice_row_id, _ = await self.db.insert_notice(notice)
        await self.db.mark_message_recalled(
            platform_id="onebot",
            group_id="123456",
            message_id="1001",
            operator_id="2002",
            recalled_at=1710000003,
        )

    async def asyncTearDown(self):
        await self.db.close()
        self._temp_dir.cleanup()

    async def test_overview_and_group_queries(self):
        overview = await self.db.get_overview()
        self.assertEqual(overview["total_groups"], 1)
        self.assertEqual(overview["incoming_messages"], 1)
        self.assertEqual(overview["notice_events"], 1)
        self.assertEqual(overview["forward_nodes"], 1)

        groups = await self.db.list_groups(search="测试")
        self.assertEqual(groups["total"], 1)
        self.assertEqual(groups["items"][0]["group_id"], "123456")

    async def test_message_and_notice_detail_queries(self):
        messages = await self.db.list_messages(
            platform_id="onebot",
            group_id="123456",
            search="hello",
        )
        self.assertEqual(messages["total"], 1)
        self.assertEqual(messages["items"][0]["forward_node_count"], 1)
        self.assertEqual(messages["items"][0]["is_recalled"], 1)

        message_detail = await self.db.get_message_detail(self.message_row_id)
        self.assertIsNotNone(message_detail)
        self.assertEqual(len(message_detail["segments"]), 2)
        self.assertEqual(message_detail["forward_nodes"][0]["sender_name"], "Bob")

        notices = await self.db.list_notices(
            platform_id="onebot",
            group_id="123456",
            notice_type="group_recall",
        )
        self.assertEqual(notices["total"], 1)

        notice_detail = await self.db.get_notice_detail(self.notice_row_id)
        self.assertIsNotNone(notice_detail)
        self.assertEqual(notice_detail["notice_type"], "group_recall")
        self.assertEqual(notice_detail["raw_event"]["notice_type"], "group_recall")


if __name__ == "__main__":
    unittest.main()
