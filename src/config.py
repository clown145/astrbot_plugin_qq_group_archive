from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class PluginSettings:
    enabled: bool = True
    group_list_mode: str = "whitelist"
    group_list: list[str] = field(default_factory=list)
    save_raw_event: bool = True
    save_media_files: bool = True
    max_media_size_mb: int = 30
    expand_forward_messages: bool = True
    record_all_notice_events: bool = True
    capture_outgoing_messages: bool = True
    webui_enabled: bool = True
    webui_host: str = "127.0.0.1"
    webui_port: int = 18766
    webui_auth_token: str = ""

    @classmethod
    def from_mapping(cls, mapping: Any) -> "PluginSettings":
        values = dict(mapping or {})
        return cls(
            enabled=bool(values.get("enabled", True)),
            group_list_mode=str(values.get("group_list_mode", "whitelist")).lower(),
            group_list=[
                str(item).strip()
                for item in values.get("group_list", [])
                if str(item).strip()
            ],
            save_raw_event=bool(values.get("save_raw_event", True)),
            save_media_files=bool(values.get("save_media_files", True)),
            max_media_size_mb=max(int(values.get("max_media_size_mb", 30) or 30), 1),
            expand_forward_messages=bool(values.get("expand_forward_messages", True)),
            record_all_notice_events=bool(
                values.get("record_all_notice_events", True)
            ),
            capture_outgoing_messages=bool(
                values.get("capture_outgoing_messages", True)
            ),
            webui_enabled=bool(values.get("webui_enabled", True)),
            webui_host=str(values.get("webui_host", "127.0.0.1")).strip()
            or "127.0.0.1",
            webui_port=max(int(values.get("webui_port", 18766) or 18766), 1),
            webui_auth_token=str(values.get("webui_auth_token", "")).strip(),
        )

    @property
    def max_media_size_bytes(self) -> int:
        return self.max_media_size_mb * 1024 * 1024

    def matches_group(
        self,
        *,
        platform_id: str,
        group_id: str,
        unified_msg_origin: str,
        session_id: str,
    ) -> bool:
        if not self.enabled:
            return False

        targets = {self._normalize(value) for value in self.group_list if value.strip()}
        tokens = {
            self._normalize(group_id),
            self._normalize(unified_msg_origin),
            self._normalize(session_id),
            self._normalize(f"{platform_id}:{group_id}"),
            self._normalize(f"{platform_id}:GroupMessage:{group_id}"),
        }

        if self.group_list_mode == "blacklist":
            return not bool(targets & tokens)

        return bool(targets) and bool(targets & tokens)

    @staticmethod
    def _normalize(value: str) -> str:
        return str(value or "").strip().lower()
