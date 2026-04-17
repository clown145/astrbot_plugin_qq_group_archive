# astrbot_plugin_qq_group_archive

QQ-only AstrBot plugin that archives selected group traffic into a plugin-local
SQLite database.

## Features

- Archive incoming QQ group messages into `archive.db`
- Mark recalled messages when `group_recall` notice events arrive
- Record message emoji reactions from `group_msg_emoji_like`
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

## Configuration

Recommended minimal setup:

1. Enable the plugin.
2. Set `group_list_mode` to `whitelist`.
3. Fill `group_list` with full unified session ids or plain QQ group ids.

Examples:

- `123456789`
- `onebot:GroupMessage:123456789`
- `napcat_main:GroupMessage:123456789`

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

## Development

Basic validation commands:

```bash
python -m compileall .
python -m unittest discover -s tests
```

