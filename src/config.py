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
    profile_pipeline_enabled: bool = False
    profile_pipeline_mode: str = "astrbot_llm"
    profile_pipeline_poll_interval_sec: int = 30
    profile_pipeline_batch_message_limit: int = 40
    profile_pipeline_min_batch_messages: int = 12
    profile_pipeline_batch_overlap: int = 8
    profile_pipeline_max_jobs_per_tick: int = 2
    profile_pipeline_llm_timeout_sec: int = 300
    profile_pipeline_running_job_timeout_sec: int = 1800
    profile_pipeline_provider_id: str = ""
    profile_pipeline_judge_provider_id: str = ""
    profile_pipeline_extract_provider_id: str = ""
    profile_pipeline_resolve_provider_id: str = ""
    profile_pipeline_extract_include_images: bool = True
    profile_pipeline_extract_max_images: int = 4

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
            profile_pipeline_enabled=bool(
                values.get("profile_pipeline_enabled", False)
            ),
            profile_pipeline_mode=str(
                values.get("profile_pipeline_mode", "astrbot_llm")
            ).strip().lower()
            or "astrbot_llm",
            profile_pipeline_poll_interval_sec=max(
                int(values.get("profile_pipeline_poll_interval_sec", 30) or 30), 5
            ),
            profile_pipeline_batch_message_limit=max(
                int(values.get("profile_pipeline_batch_message_limit", 40) or 40), 8
            ),
            profile_pipeline_min_batch_messages=max(
                int(values.get("profile_pipeline_min_batch_messages", 12) or 12), 4
            ),
            profile_pipeline_batch_overlap=max(
                int(values.get("profile_pipeline_batch_overlap", 8) or 8), 0
            ),
            profile_pipeline_max_jobs_per_tick=max(
                int(values.get("profile_pipeline_max_jobs_per_tick", 2) or 2), 1
            ),
            profile_pipeline_llm_timeout_sec=max(
                int(values.get("profile_pipeline_llm_timeout_sec", 300) or 300), 30
            ),
            profile_pipeline_running_job_timeout_sec=max(
                int(
                    values.get("profile_pipeline_running_job_timeout_sec", 1800)
                    or 1800
                ),
                60,
            ),
            profile_pipeline_provider_id=str(
                values.get("profile_pipeline_provider_id", "")
            ).strip(),
            profile_pipeline_judge_provider_id=str(
                values.get("profile_pipeline_judge_provider_id", "")
            ).strip(),
            profile_pipeline_extract_provider_id=str(
                values.get("profile_pipeline_extract_provider_id", "")
            ).strip(),
            profile_pipeline_resolve_provider_id=str(
                values.get("profile_pipeline_resolve_provider_id", "")
            ).strip(),
            profile_pipeline_extract_include_images=bool(
                values.get("profile_pipeline_extract_include_images", True)
            ),
            profile_pipeline_extract_max_images=max(
                int(values.get("profile_pipeline_extract_max_images", 4) or 4), 0
            ),
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

    def get_profile_stage_provider_id(self, stage: str) -> str:
        stage_name = str(stage or "").strip().lower()
        if stage_name == "judge" and self.profile_pipeline_judge_provider_id:
            return self.profile_pipeline_judge_provider_id
        if stage_name == "extract" and self.profile_pipeline_extract_provider_id:
            return self.profile_pipeline_extract_provider_id
        if stage_name == "resolve" and self.profile_pipeline_resolve_provider_id:
            return self.profile_pipeline_resolve_provider_id
        return self.profile_pipeline_provider_id
