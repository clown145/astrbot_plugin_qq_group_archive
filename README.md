# astrbot_plugin_qq_group_archive

QQ-only AstrBot plugin that archives selected group traffic into a plugin-local
SQLite database.

## Features

- Archive incoming QQ group messages into `archive.db`
- Mark recalled messages when `group_recall` notice events arrive
- Record message emoji reactions from `group_msg_emoji_like`
- Provide a built-in WebUI for browsing groups, messages, notices, attachments,
  forward nodes, and raw payloads
- Maintain a profile layer for user, group-user, daily, and interaction summaries
- Persist media files into the plugin data directory
- Keep raw OneBot payloads for unsupported segments such as `mface`
- Resolve forward message nodes with `get_forward_msg` when available
- Optionally archive outgoing bot replies via `after_message_sent`

## What gets stored

- Message metadata: group id, sender id, sender nickname/card, message id, time
- Normalized message segments: text, at, face, reply, image, record, video, file, forward
- Notice events: recall, emoji reaction, poke, and optionally every group notice
- Forwarded node details when expansion succeeds
- Raw OneBot payload JSON for later inspection

## Data location

At runtime, AstrBot stores plugin data under:

`data/plugin_data/astrbot_plugin_qq_group_archive/`

Important files:

- `archive.db`: main SQLite database
- `media/`: persisted media files grouped by type and date

## WebUI

When `webui_enabled` is on, the plugin starts a small local HTTP server.

Default address:

`http://127.0.0.1:18766`

Recommended config:

- Keep `webui_host` as `127.0.0.1`
- Set `webui_auth_token` before exposing the port beyond localhost

The WebUI can:

- Search and select archived groups
- Browse paged message and notice timelines
- Browse per-group member profiles and interaction summaries
- Inspect full message segment details
- View recall / emoji reaction raw notice payloads
- Open locally persisted images and files
- Inspect raw OneBot event JSON

Useful API routes:

- `GET /api/overview`
- `GET /api/groups`
- `GET /api/profiles/group`
- `GET /api/profiles/users`
- `GET /api/profiles/users/{user_id}`
- `GET /api/messages`
- `GET /api/messages/{id}`
- `GET /api/notices`
- `GET /api/notices/{id}`
- `GET /api/media/{relative_path}`

## Configuration

Recommended minimal setup:

1. Enable the plugin.
2. Set `group_list_mode` to `whitelist`.
3. Fill `group_list` with full unified session ids or plain QQ group ids.

Examples:

- `123456789`
- `onebot:GroupMessage:123456789`
- `napcat_main:GroupMessage:123456789`

WebUI-specific fields:

- `webui_enabled`: start the built-in WebUI
- `webui_host`: bind host, default `127.0.0.1`
- `webui_port`: bind port, default `18766`
- `webui_auth_token`: optional token checked by the WebUI API

## Commands

- `/归档状态`
  Show whether the current group is inside the archive scope and where data is being written.
- `/归档统计 [days]`
  Show message and notice counts for the current group within the last N days.

## Notes

- QQ small yellow face segments are archived as normal `face` segments.
- QQ message emoji reactions are notice events, not message segments.
- Forward messages are first stored as outer `forward` segments. The plugin then
  tries `get_forward_msg` to archive forwarded node content as a second step.
- Outgoing bot messages can be archived, but they do not currently include the
  final QQ message id because AstrBot does not expose it at `after_message_sent`.

## Database Shape

Main tables:

- `archived_messages`
  One row per archived message. Includes `platform_id`, `group_id`, `session_id`,
  `message_id`, sender fields, `direction`, `plain_text`, `outline`, `event_time`,
  `raw_event_json`, and recall markers.
- `archived_segments`
  One row per message segment. Includes `seg_index`, `seg_type`, `raw_type`,
  text, normalized JSON payload, source URL, local file path, hash, MIME type,
  and file size.
- `archived_notice_events`
  One row per QQ notice event. Includes `notice_type`, `sub_type`, actor/operator
  ids, target ids, related `message_id`, reaction fields, `event_time`, and
  `raw_event_json`.
- `archived_forward_nodes`
  Expanded forward-message nodes fetched from `get_forward_msg`.
- `archived_groups`
  Last seen group name cache for each `(platform_id, group_id)`.

Profile tables:

- `profile_user_summary`
  Cross-group summary for each `(platform_id, user_id)`.
- `profile_user_group_summary`
  Group-scoped summary for each `(platform_id, group_id, user_id)`.
- `profile_user_daily_stats`
  Per-day counters for each `(platform_id, group_id, user_id, stat_date)`.
- `profile_interactions`
  Aggregated interaction edges such as `at`, `reply`, and best-effort
  `emoji_reaction`.

The profile layer is derived from the archive layer, not a replacement for it:

- `archived_*` keeps raw facts and original payloads
- `profile_*` keeps fast aggregated counters for portrait / persona analysis

Typical `archived_messages` row:

```json
{
  "id": 42,
  "platform_id": "napcat_main",
  "group_id": "123456789",
  "message_id": "987654321",
  "sender_id": "10001",
  "sender_name": "Alice",
  "sender_card": "项目组",
  "direction": "incoming",
  "plain_text": "你好[image]",
  "outline": "你好[image]",
  "event_time": 1710000000,
  "is_recalled": 0,
  "raw_event_json": "{...}"
}
```

Typical `archived_segments` row:

```json
{
  "message_row_id": 42,
  "seg_index": 1,
  "seg_type": "image",
  "raw_type": "image",
  "source_url": "https://gchat.qpic.cn/...",
  "local_path": "media/image/20260417/abcdef.jpg",
  "media_status": "stored",
  "file_size": 24567
}
```

Typical `archived_notice_events` row:

```json
{
  "id": 9,
  "notice_type": "group_msg_emoji_like",
  "group_id": "123456789",
  "actor_user_id": "10001",
  "operator_id": "10002",
  "message_id": "987654321",
  "reaction_code": "128077",
  "reaction_count": 3,
  "event_time": 1710001234,
  "raw_event_json": "{...}"
}
```

## Development

Basic validation commands:

```bash
python -m compileall .
python -m unittest discover -s tests
```
