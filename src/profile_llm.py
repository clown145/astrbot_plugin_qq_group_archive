from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from astrbot.api.star import Context

from .config import PluginSettings
from .profile_pipeline_models import (
    CandidateSpan,
    ExtractedClaim,
    JudgeResult,
    ResolutionAction,
    ResolutionResult,
    ResolvedClaim,
)


class ProfilePipelineLLM(Protocol):
    async def judge_block(self, block: dict[str, Any]) -> JudgeResult:
        ...

    async def extract_claims(
        self,
        block: dict[str, Any],
        candidate_span: CandidateSpan,
    ) -> list[ExtractedClaim]:
        ...

    async def resolve_claims(
        self,
        block: dict[str, Any],
        extracted_claims: list[ExtractedClaim],
        resolution_context: dict[str, Any],
    ) -> ResolutionResult:
        ...


@dataclass(slots=True)
class HeuristicProfileLLM:
    """Pipeline-only fallback.

    This intentionally does not maintain keyword enums. Open-ended portrait
    extraction requires an LLM because the ontology is unbounded.
    """

    async def judge_block(self, block: dict[str, Any]) -> JudgeResult:
        messages = list(block.get("messages", []) or [])
        message_row_ids = [
            int(message["id"])
            for message in messages
            if str(message.get("id", "")).strip()
        ]
        subject_user_ids = sorted(
            {
                str(message.get("sender_id", "") or "").strip()
                for message in messages
                if str(message.get("sender_id", "") or "").strip()
            }
        )
        if not message_row_ids:
            return JudgeResult(
                summary={
                    "mode": "heuristic",
                    "candidate_count": 0,
                    "message_count": len(messages),
                    "note": "heuristic validates pipeline only; use astrbot_llm for semantic extraction",
                }
            )
        return JudgeResult(
            candidate_spans=[
                CandidateSpan(
                    message_row_ids=message_row_ids,
                    subject_user_ids=subject_user_ids,
                    claim_types=["open_profile_fact"],
                    reason="pipeline_passthrough",
                    need_image_context=False,
                )
            ],
            summary={
                "mode": "heuristic",
                "candidate_count": 1,
                "message_count": len(messages),
                "note": "heuristic does not extract claims; use astrbot_llm for open-ended portraits",
            },
        )

    async def extract_claims(
        self,
        block: dict[str, Any],
        candidate_span: CandidateSpan,
    ) -> list[ExtractedClaim]:
        return []

    async def resolve_claims(
        self,
        block: dict[str, Any],
        extracted_claims: list[ExtractedClaim],
        resolution_context: dict[str, Any],
    ) -> ResolutionResult:
        return ResolutionResult(
            resolved_claims=[],
            actions=[],
            summary={
                "mode": "heuristic",
                "resolved_count": 0,
                "note": "heuristic mode does not resolve semantic claims",
            },
        )


@dataclass(slots=True)
class NoopProfileLLM:
    async def judge_block(self, block: dict[str, Any]) -> JudgeResult:
        return JudgeResult(summary={"mode": "noop", "candidate_count": 0})

    async def extract_claims(
        self,
        block: dict[str, Any],
        candidate_span: CandidateSpan,
    ) -> list[ExtractedClaim]:
        return []

    async def resolve_claims(
        self,
        block: dict[str, Any],
        extracted_claims: list[ExtractedClaim],
        resolution_context: dict[str, Any],
    ) -> ResolutionResult:
        return ResolutionResult(summary={"mode": "noop", "resolved_count": 0})


@dataclass(slots=True)
class AstrBotProfileLLM:
    context: Context
    config: Any
    data_dir: Path
    fallback: HeuristicProfileLLM = field(default_factory=HeuristicProfileLLM)

    async def judge_block(self, block: dict[str, Any]) -> JudgeResult:
        payload = {
            "platform_id": str(block.get("platform_id") or ""),
            "group_id": str(block.get("group_id") or ""),
            "group_name": str(block.get("group_name") or ""),
            "message_count": len(block.get("messages", []) or []),
            "messages": self._serialize_messages(block.get("messages", []) or []),
        }
        prompt = (
            "你是开放本体的人物画像预筛选器。你的目标是高召回。"
            "只要消息里可能包含任何可用于理解某个群成员的事实、偏好、习惯、关系、身份、经历、状态、风格、环境或长期记忆，就选出来。"
            "没有固定类别表，没有白名单，没有预设上限。属性类型应由后续抽取阶段根据证据自由命名。"
            "每个候选类型除了英文 key，也应该能对应一个中文显示名。"
            "不要因为事实很细、很新、很奇怪、无法归到常见类别就跳过。"
            "如果一整段都可能有线索，可以把整段作为候选 span。"
            "如果完全是无意义闲聊、表情、无主体泛泛讨论、纯玩笑且无法作为证据，可以不选。"
            "返回 JSON，不能输出解释性自然语言。格式："
            '{"candidate_spans":[{"message_row_ids":[1,2],"subject_user_ids":["123"],'
            '"claim_types":["open_attribute_name"],"reason":"为什么这段可能包含画像事实",'
            '"need_image_context":false}],"summary":{"candidate_count":1}}。'
            "claim_types 只是开放式提示字段，请用英文 snake_case 自行命名；不能把任何示例当成枚举。"
            "如果没有候选，candidate_spans 返回空数组。"
            "\n\n批次数据：\n"
            f"{json.dumps(payload, ensure_ascii=False)}"
        )
        try:
            data = await self._call_json_stage("judge", prompt=prompt)
        except Exception as exc:
            raise RuntimeError(f"profile judge LLM failed: {exc}") from exc

        spans = [
            CandidateSpan.from_mapping(item)
            for item in data.get("candidate_spans", []) or []
        ]
        return JudgeResult(
            candidate_spans=spans,
            summary=dict(data.get("summary", {}) or {})
            | {"mode": "astrbot_llm", "candidate_count": len(spans)},
        )

    async def extract_claims(
        self,
        block: dict[str, Any],
        candidate_span: CandidateSpan,
    ) -> list[ExtractedClaim]:
        messages = list(block.get("messages", []) or [])
        target_ids = set(candidate_span.message_row_ids)
        selected_messages = [
            message
            for message in messages
            if int(message.get("id") or 0) in target_ids
        ]
        payload = {
            "platform_id": str(block.get("platform_id") or ""),
            "group_id": str(block.get("group_id") or ""),
            "candidate_span": candidate_span.to_dict(),
            "messages": self._serialize_messages(selected_messages),
        }
        prompt = (
            "你是开放本体的人物画像事实抽取器。请从候选片段中尽可能提取所有有证据的画像 claim。"
            "没有固定属性类别，没有白名单。attribute_type 必须由你根据证据自由创建，使用稳定、细粒度的英文 snake_case；attribute_label 必须直接生成简短自然的中文显示名。"
            "attribute_label 不允许照抄英文 key，不允许使用 snake_case；它应该像“电脑型号”“编程习惯”“穿衣风格”“导师关系”这种可读中文短语。"
            "不要因为属性类型没有见过就丢弃；也不要把所有未知信息塞进 generic_profile_fact。"
            "任何能帮助理解这个人的信息都可以成为 claim，例如关系、导师、对象、穿衣风格、设备、软件环境、编程习惯、上课状态、项目经历、偏好、长期计划、临时状态等；这些只是示例，不是枚举。"
            "抽取规则："
            "1. 每条 claim 必须有 subject_user_id、attribute_type、attribute_label、normalized_value、evidence_message_row_ids、evidence_excerpt；attribute_label 必须是中文。"
            "2. 多个事实必须拆成多条 claim。"
            "3. 区分 source_kind：self_report、other_report、direct_observation、inferred、unknown。"
            "4. 区分 tense：current、past、future、habitual、temporary、unknown。"
            "5. 不要把单纯提到某物当成拥有/使用；证据不足时降低 confidence 或标为 inferred。"
            "6. 玩笑、反话、转述、猜测必须降低 confidence，并在 payload.reason 里说明。"
            "7. 原文片段中的图片如果传入，也可以作为证据；图片结论应标 direct_observation 或 inferred。"
            "只返回 JSON，不要输出自然语言。格式："
            '{"claims":[{"subject_user_id":"123","attribute_type":"open_attribute_name",'
            '"attribute_label":"中文属性名","raw_value":"原文值","normalized_value":"标准化值","source_kind":"self_report",'
            '"tense":"current","polarity":"affirmed","confidence":0.82,'
            '"evidence_message_row_ids":[1],"evidence_excerpt":"原文证据片段",'
            '"payload":{"reason":"为什么这是画像事实"}}]}。'
            "如果没有可抽取事实，claims 返回空数组。"
            "\n\n候选片段：\n"
            f"{json.dumps(payload, ensure_ascii=False)}"
        )
        image_urls = self._collect_image_urls(selected_messages)
        try:
            data = await self._call_json_stage(
                "extract",
                prompt=prompt,
                image_urls=image_urls,
            )
        except Exception as exc:
            raise RuntimeError(f"profile extract LLM failed: {exc}") from exc

        return [
            ExtractedClaim.from_mapping(item)
            for item in data.get("claims", []) or []
            if str(item.get("subject_user_id", "")).strip()
            and str(item.get("attribute_type", "")).strip()
            and str(item.get("normalized_value", "")).strip()
        ]

    async def resolve_claims(
        self,
        block: dict[str, Any],
        extracted_claims: list[ExtractedClaim],
        resolution_context: dict[str, Any],
    ) -> ResolutionResult:
        if not extracted_claims:
            return ResolutionResult(
                resolved_claims=[],
                actions=[],
                summary={"mode": "astrbot_llm", "resolved_count": 0},
            )
        payload = {
            "platform_id": str(block.get("platform_id") or ""),
            "group_id": str(block.get("group_id") or ""),
            "new_claims": [item.to_dict() for item in extracted_claims],
            "existing_attributes": list(resolution_context.get("attributes", []) or []),
            "recent_claims": list(resolution_context.get("recent_claims", []) or []),
        }
        prompt = (
            "你是开放本体的人物画像 claim 合并与冲突消解器。"
            "输入包含新 claim、已有当前属性、近期历史 claim。attribute_type 是开放字段，不存在固定枚举。"
            "你必须保留合理的新属性类型，不能因为类型陌生就丢弃。"
            "必须保留或生成 attribute_label，attribute_label 是给 WebUI 展示的中文属性名；如果输入缺失或不是中文，你要根据语义补成中文短标签。"
            "你可以输出 actions 来整理已有画像："
            "1. set_claim_status：把已有 claim 标为 duplicate、outdated、conflicted、rejected、low_confidence、accepted 或 candidate。"
            "2. rename_attribute：把同一用户的旧 attribute_type 改成更稳定的 attribute_type，并给出中文 attribute_label。"
            "3. merge_attribute：把同一用户语义相同的旧 attribute_type 合并到目标 attribute_type。"
            "actions 只能引用输入 existing_attributes/recent_claims 里已经存在的 claim_id 或 attribute_type；不能引用新 claim 尚未入库的 id。"
            "严禁要求物理删除原始消息或证据，只能软状态标记、重命名或合并聚合视图。"
            "判断每个 claim 应该 accepted、candidate、conflicted 或 outdated，并决定是否 current_value。"
            "同一用户同一 attribute_type 如果是互斥当前状态，通常只能有一个 current_value=true。"
            "但偏好、风格、习惯、关系、技能等可共存的信息，不要强行互相覆盖；必要时保留多个 claim 或使用更细粒度 attribute_type。"
            "只返回 JSON，不要输出自然语言。格式："
            '{"resolved_claims":[{"subject_user_id":"123","attribute_type":"open_attribute_name",'
            '"attribute_label":"中文属性名","raw_value":"原文值","normalized_value":"标准化值","source_kind":"self_report",'
            '"tense":"current","polarity":"affirmed","confidence":0.86,"status":"accepted",'
            '"current_value":true,"evidence_message_row_ids":[1],"evidence_excerpt":"原文证据片段",'
            '"supersedes_claim_ids":[],"merged_claim_ids":[],"note":"reason","payload":{}}],'
            '"actions":[{"type":"merge_attribute","subject_user_id":"123",'
            '"from_attribute_type":"computer_model","to_attribute_type":"device_computer_model",'
            '"attribute_label":"电脑型号","claim_ids":[12,15],"status":"duplicate",'
            '"reason":"语义相同，统一到更稳定的属性名","payload":{}}],'
            '"summary":{"resolved_count":1,"action_count":1}}。'
            "\n\n输入：\n"
            f"{json.dumps(payload, ensure_ascii=False)}"
        )
        try:
            data = await self._call_json_stage("resolve", prompt=prompt)
        except Exception as exc:
            raise RuntimeError(f"profile resolve LLM failed: {exc}") from exc

        resolved = [
            ResolvedClaim.from_mapping(item)
            for item in data.get("resolved_claims", []) or []
            if str(item.get("subject_user_id", "")).strip()
            and str(item.get("attribute_type", "")).strip()
            and str(item.get("normalized_value", "")).strip()
        ]
        actions = [
            ResolutionAction.from_mapping(item)
            for item in data.get("actions", []) or []
            if str(item.get("type") or item.get("action_type") or "").strip()
        ]
        return ResolutionResult(
            resolved_claims=resolved,
            actions=actions,
            summary=dict(data.get("summary", {}) or {})
            | {
                "mode": "astrbot_llm",
                "resolved_count": len(resolved),
                "action_count": len(actions),
            },
        )

    async def _call_json_stage(
        self,
        stage: str,
        *,
        prompt: str,
        image_urls: list[str] | None = None,
    ) -> dict[str, Any]:
        settings = PluginSettings.from_mapping(self.config)
        provider_id = settings.get_profile_stage_provider_id(stage)
        if not provider_id:
            provider = self.context.get_using_provider()
            if provider is None:
                raise RuntimeError(
                    f"profile pipeline {stage} stage has no provider configured"
                )
            provider_id = provider.meta().id
        try:
            response = await asyncio.wait_for(
                self.context.llm_generate(
                    chat_provider_id=provider_id,
                    prompt=prompt,
                    system_prompt=(
                        "You are an open-ontology structured extraction engine. "
                        "Do not use fixed category whitelists. "
                        "Every attribute_label must be a concise Chinese display label, not snake_case. "
                        "Return valid JSON only."
                    ),
                    image_urls=image_urls or None,
                ),
                timeout=settings.profile_pipeline_llm_timeout_sec,
            )
        except asyncio.TimeoutError as exc:
            raise TimeoutError(
                f"profile pipeline {stage} LLM call timed out after "
                f"{settings.profile_pipeline_llm_timeout_sec}s"
            ) from exc
        return self._parse_json_payload(response.completion_text)

    def _collect_image_urls(self, messages: list[dict[str, Any]]) -> list[str]:
        settings = PluginSettings.from_mapping(self.config)
        if not settings.profile_pipeline_extract_include_images:
            return []
        image_urls: list[str] = []
        seen: set[str] = set()
        for message in messages:
            for segment in message.get("segments", []) or []:
                if str(segment.get("seg_type") or "") != "image":
                    continue
                local_path = str(segment.get("local_path") or "").strip()
                source_url = str(segment.get("source_url") or "").strip()
                candidate = ""
                if local_path:
                    absolute = self.data_dir / local_path
                    if absolute.exists():
                        candidate = str(absolute)
                if not candidate and source_url:
                    candidate = source_url
                if not candidate or candidate in seen:
                    continue
                seen.add(candidate)
                image_urls.append(candidate)
                if len(image_urls) >= settings.profile_pipeline_extract_max_images:
                    return image_urls
        return image_urls

    @staticmethod
    def _serialize_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        payload: list[dict[str, Any]] = []
        for message in messages:
            segments = list(message.get("segments", []) or [])
            payload.append(
                {
                    "id": int(message.get("id") or 0),
                    "message_id": str(message.get("message_id") or ""),
                    "sender_id": str(message.get("sender_id") or ""),
                    "sender_name": str(message.get("sender_name") or ""),
                    "sender_card": str(message.get("sender_card") or ""),
                    "event_time": int(message.get("event_time") or 0),
                    "plain_text": str(message.get("plain_text") or ""),
                    "outline": str(message.get("outline") or ""),
                    "segment_types": [
                        str(segment.get("seg_type") or "")
                        for segment in segments
                    ],
                    "image_count": sum(
                        1 for segment in segments if str(segment.get("seg_type") or "") == "image"
                    ),
                }
            )
        return payload

    @staticmethod
    def _parse_json_payload(text: str) -> dict[str, Any]:
        content = str(text or "").strip()
        if not content:
            raise ValueError("empty llm response")
        try:
            parsed = json.loads(content)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

        fenced_match = re.search(r"```(?:json)?\s*(\{.*\}|\[.*\])\s*```", content, re.S)
        if fenced_match:
            parsed = json.loads(fenced_match.group(1))
            if isinstance(parsed, dict):
                return parsed

        object_match = re.search(r"(\{.*\})", content, re.S)
        if object_match:
            parsed = json.loads(object_match.group(1))
            if isinstance(parsed, dict):
                return parsed
        raise ValueError("llm response is not valid json")


def build_profile_llm(
    mode: str,
    *,
    context: Context | None = None,
    config: Any = None,
    data_dir: Path | None = None,
) -> ProfilePipelineLLM:
    selected = str(mode or "").strip().lower()
    if selected in {"heuristic", "rules", ""}:
        return HeuristicProfileLLM()
    if selected in {"astrbot_llm", "llm"}:
        if context is None or data_dir is None:
            raise ValueError("astrbot_llm mode requires plugin context and data_dir")
        return AstrBotProfileLLM(
            context=context,
            config=config,
            data_dir=data_dir,
        )
    return NoopProfileLLM()
