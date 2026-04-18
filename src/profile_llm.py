from __future__ import annotations

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
    ResolutionResult,
    ResolvedClaim,
)

ATTRIBUTE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "education_university": (
        "大学",
        "学院",
        "学校",
        "校区",
        "宿舍",
        "图书馆",
    ),
    "education_major": ("专业", "学院", "系里"),
    "device_phone": (
        "iphone",
        "苹果",
        "安卓",
        "华为",
        "小米",
        "vivo",
        "oppo",
        "三星",
        "一加",
        "荣耀",
        "redmi",
        "realme",
        "手机",
    ),
    "appearance_hair": ("长发", "短发", "头发", "卷发", "黑长直", "寸头"),
    "schedule_status": (
        "上课",
        "下课",
        "考试",
        "期末",
        "复习",
        "实习",
        "放假",
        "军训",
        "图书馆",
    ),
    "location_hint": ("寝室", "宿舍", "图书馆", "教室", "校区"),
}

PHONE_PATTERN = re.compile(
    r"(iphone\s?\d{1,2}(?:\s?(?:pro\s?max|pro|max|plus|mini))?"
    r"|苹果\s?\d{1,2}(?:\s?(?:promax|pro|max|plus|mini))?"
    r"|华为[\w\- ]{0,16}"
    r"|小米[\w\- ]{0,16}"
    r"|redmi[\w\- ]{0,16}"
    r"|荣耀[\w\- ]{0,16}"
    r"|vivo[\w\- ]{0,16}"
    r"|oppo[\w\- ]{0,16}"
    r"|一加[\w\- ]{0,16}"
    r"|三星[\w\- ]{0,16}"
    r"|15pm|16pm|15pro|16pro|安卓机?)",
    re.IGNORECASE,
)
UNIVERSITY_PATTERN = re.compile(r"([\u4e00-\u9fa5]{2,24}(?:大学|学院|学校))")
MAJOR_PATTERN = re.compile(r"([\u4e00-\u9fa5]{2,18}(?:专业|学院|系))")
HAIR_PATTERN = re.compile(r"(长头发|长发|短发|卷发|黑长直|寸头)")


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
    max_neighbor_messages: int = 1

    async def judge_block(self, block: dict[str, Any]) -> JudgeResult:
        messages = list(block.get("messages", []) or [])
        hit_indices: list[int] = []
        claim_types_by_index: dict[int, set[str]] = {}

        for index, message in enumerate(messages):
            combined = self._combined_text(message)
            if not combined:
                continue
            claim_types = self._guess_claim_types(combined)
            if not claim_types:
                continue
            hit_indices.append(index)
            claim_types_by_index[index] = claim_types

        if not hit_indices:
            return JudgeResult(
                summary={
                    "mode": "heuristic",
                    "candidate_count": 0,
                    "message_count": len(messages),
                }
            )

        spans: list[CandidateSpan] = []
        cluster: list[int] = []
        for index in hit_indices:
            if not cluster or index - cluster[-1] <= 2:
                cluster.append(index)
                continue
            spans.append(self._cluster_to_span(messages, cluster, claim_types_by_index))
            cluster = [index]
        if cluster:
            spans.append(self._cluster_to_span(messages, cluster, claim_types_by_index))

        return JudgeResult(
            candidate_spans=spans,
            summary={
                "mode": "heuristic",
                "candidate_count": len(spans),
                "message_count": len(messages),
            },
        )

    async def extract_claims(
        self,
        block: dict[str, Any],
        candidate_span: CandidateSpan,
    ) -> list[ExtractedClaim]:
        message_map = {
            int(message["id"]): message
            for message in block.get("messages", [])
            if str(message.get("id", "")).strip()
        }
        claims: list[ExtractedClaim] = []

        for message_id in candidate_span.message_row_ids:
            message = message_map.get(int(message_id))
            if message is None:
                continue
            text = self._combined_text(message)
            sender_id = str(message.get("sender_id", "") or "").strip()
            if not text or not sender_id:
                continue
            evidence_ids = [int(message_id)]
            claims.extend(
                [
                    *self._extract_device_claims(sender_id, text, evidence_ids),
                    *self._extract_university_claims(sender_id, text, evidence_ids),
                    *self._extract_major_claims(sender_id, text, evidence_ids),
                    *self._extract_hair_claims(sender_id, text, evidence_ids),
                    *self._extract_schedule_claims(sender_id, text, evidence_ids),
                    *self._extract_location_claims(sender_id, text, evidence_ids),
                ]
            )

        deduped: dict[tuple[str, str, str, str], ExtractedClaim] = {}
        for claim in claims:
            key = (
                claim.subject_user_id,
                claim.attribute_type,
                claim.normalized_value,
                claim.evidence_excerpt,
            )
            existing = deduped.get(key)
            if existing is None:
                deduped[key] = claim
                continue
            merged_ids = sorted(
                {
                    *existing.evidence_message_row_ids,
                    *claim.evidence_message_row_ids,
                }
            )
            existing.evidence_message_row_ids = merged_ids
            existing.confidence = max(existing.confidence, claim.confidence)

        return list(deduped.values())

    async def resolve_claims(
        self,
        block: dict[str, Any],
        extracted_claims: list[ExtractedClaim],
        resolution_context: dict[str, Any],
    ) -> ResolutionResult:
        merged_claims: dict[tuple[str, str, str], ExtractedClaim] = {}
        for claim in extracted_claims:
            key = (
                claim.subject_user_id,
                claim.attribute_type,
                claim.normalized_value.strip().lower(),
            )
            existing = merged_claims.get(key)
            if existing is None:
                merged_claims[key] = claim
                continue
            existing.confidence = max(existing.confidence, claim.confidence)
            existing.evidence_message_row_ids = sorted(
                {
                    *existing.evidence_message_row_ids,
                    *claim.evidence_message_row_ids,
                }
            )

        attributes = {
            (
                str(item.get("subject_user_id", "")).strip(),
                str(item.get("attribute_type", "")).strip(),
            ): item
            for item in resolution_context.get("attributes", [])
        }
        resolved: list[ResolvedClaim] = []
        winners: dict[tuple[str, str], ResolvedClaim] = {}

        for claim in sorted(
            merged_claims.values(),
            key=lambda item: (
                item.subject_user_id,
                item.attribute_type,
                -item.confidence,
                -max(item.evidence_message_row_ids or [0]),
            ),
        ):
            attr_key = (claim.subject_user_id, claim.attribute_type)
            existing_attr = attributes.get(attr_key)
            current = winners.get(attr_key)
            if current is None:
                current = self._resolve_against_existing(claim, existing_attr)
                winners[attr_key] = current
            else:
                current = self._resolve_against_current(claim, current)
                if current.current_value:
                    winners[attr_key] = current
            resolved.append(current)

        return ResolutionResult(
            resolved_claims=resolved,
            summary={
                "mode": "heuristic",
                "resolved_count": len(resolved),
                "attribute_count": len(
                    {(item.subject_user_id, item.attribute_type) for item in resolved}
                ),
            },
        )

    def _resolve_against_existing(
        self,
        claim: ExtractedClaim,
        existing_attr: dict[str, Any] | None,
    ) -> ResolvedClaim:
        note = ""
        supersedes: list[int] = []
        current_value = True
        status = "accepted"

        if existing_attr:
            existing_value = str(existing_attr.get("normalized_value", "") or "")
            existing_confidence = float(existing_attr.get("confidence", 0.0) or 0.0)
            existing_claim_id = int(existing_attr.get("current_claim_id") or 0)
            if existing_value and existing_value.strip().lower() != claim.normalized_value.strip().lower():
                if claim.confidence + 0.08 < existing_confidence:
                    current_value = False
                    status = "candidate"
                    note = "existing_attribute_kept"
                elif existing_claim_id:
                    supersedes.append(existing_claim_id)
                    note = "superseded_existing_attribute"
        return ResolvedClaim(
            subject_user_id=claim.subject_user_id,
            attribute_type=claim.attribute_type,
            raw_value=claim.raw_value,
            normalized_value=claim.normalized_value,
            source_kind=claim.source_kind,
            tense=claim.tense,
            polarity=claim.polarity,
            confidence=claim.confidence,
            status=status,
            current_value=current_value,
            evidence_message_row_ids=list(claim.evidence_message_row_ids),
            evidence_excerpt=claim.evidence_excerpt,
            supersedes_claim_ids=supersedes,
            note=note,
            payload=dict(claim.payload),
        )

    def _resolve_against_current(
        self,
        claim: ExtractedClaim,
        current: ResolvedClaim,
    ) -> ResolvedClaim:
        same_value = (
            current.normalized_value.strip().lower()
            == claim.normalized_value.strip().lower()
        )
        if same_value:
            merged_ids = sorted(
                {
                    *current.evidence_message_row_ids,
                    *claim.evidence_message_row_ids,
                }
            )
            return ResolvedClaim(
                subject_user_id=current.subject_user_id,
                attribute_type=current.attribute_type,
                raw_value=current.raw_value or claim.raw_value,
                normalized_value=current.normalized_value,
                source_kind=current.source_kind,
                tense=current.tense,
                polarity=current.polarity,
                confidence=max(current.confidence, claim.confidence),
                status=current.status,
                current_value=current.current_value,
                evidence_message_row_ids=merged_ids,
                evidence_excerpt=current.evidence_excerpt or claim.evidence_excerpt,
                supersedes_claim_ids=list(current.supersedes_claim_ids),
                merged_claim_ids=list(current.merged_claim_ids),
                note=current.note,
                payload=dict(current.payload),
            )

        keep_new = claim.confidence >= current.confidence + 0.1
        if keep_new:
            supersedes = list(current.supersedes_claim_ids)
            return ResolvedClaim(
                subject_user_id=claim.subject_user_id,
                attribute_type=claim.attribute_type,
                raw_value=claim.raw_value,
                normalized_value=claim.normalized_value,
                source_kind=claim.source_kind,
                tense=claim.tense,
                polarity=claim.polarity,
                confidence=claim.confidence,
                status="accepted",
                current_value=True,
                evidence_message_row_ids=list(claim.evidence_message_row_ids),
                evidence_excerpt=claim.evidence_excerpt,
                supersedes_claim_ids=supersedes,
                note="replaced_batch_claim",
                payload=dict(claim.payload),
            )

        return ResolvedClaim(
            subject_user_id=claim.subject_user_id,
            attribute_type=claim.attribute_type,
            raw_value=claim.raw_value,
            normalized_value=claim.normalized_value,
            source_kind=claim.source_kind,
            tense=claim.tense,
            polarity=claim.polarity,
            confidence=claim.confidence,
            status="candidate",
            current_value=False,
            evidence_message_row_ids=list(claim.evidence_message_row_ids),
            evidence_excerpt=claim.evidence_excerpt,
            note="weaker_than_batch_claim",
            payload=dict(claim.payload),
        )

    def _cluster_to_span(
        self,
        messages: list[dict[str, Any]],
        cluster: list[int],
        claim_types_by_index: dict[int, set[str]],
    ) -> CandidateSpan:
        start = max(cluster[0] - self.max_neighbor_messages, 0)
        end = min(cluster[-1] + self.max_neighbor_messages, len(messages) - 1)
        message_row_ids = [
            int(messages[index]["id"])
            for index in range(start, end + 1)
            if str(messages[index].get("id", "")).strip()
        ]
        subject_user_ids = sorted(
            {
                str(messages[index].get("sender_id", "") or "").strip()
                for index in range(start, end + 1)
                if str(messages[index].get("sender_id", "")).strip()
            }
        )
        claim_types = sorted(
            {
                claim_type
                for index in cluster
                for claim_type in claim_types_by_index.get(index, set())
            }
        )
        return CandidateSpan(
            message_row_ids=message_row_ids,
            subject_user_ids=subject_user_ids,
            claim_types=claim_types,
            reason="keyword_hit",
            need_image_context=False,
        )

    def _guess_claim_types(self, text: str) -> set[str]:
        lowered = text.lower()
        claim_types: set[str] = set()
        for attribute_type, keywords in ATTRIBUTE_KEYWORDS.items():
            if any(keyword.lower() in lowered for keyword in keywords):
                claim_types.add(attribute_type)
        return claim_types

    @staticmethod
    def _combined_text(message: dict[str, Any]) -> str:
        plain_text = str(message.get("plain_text", "") or "")
        outline = str(message.get("outline", "") or "")
        return f"{plain_text}\n{outline}".strip()

    def _extract_device_claims(
        self,
        sender_id: str,
        text: str,
        evidence_ids: list[int],
    ) -> list[ExtractedClaim]:
        claims: list[ExtractedClaim] = []
        for matched in PHONE_PATTERN.findall(text):
            value = matched.strip()
            if not value:
                continue
            normalized = self._normalize_phone_value(value)
            claims.append(
                ExtractedClaim(
                    subject_user_id=sender_id,
                    attribute_type="device_phone",
                    raw_value=value,
                    normalized_value=normalized,
                    source_kind="self_report",
                    tense="current",
                    confidence=0.62,
                    evidence_message_row_ids=list(evidence_ids),
                    evidence_excerpt=text[:240],
                )
            )
        return claims

    def _extract_university_claims(
        self,
        sender_id: str,
        text: str,
        evidence_ids: list[int],
    ) -> list[ExtractedClaim]:
        claims: list[ExtractedClaim] = []
        for matched in UNIVERSITY_PATTERN.findall(text):
            value = matched.strip()
            if len(value) < 3:
                continue
            claims.append(
                ExtractedClaim(
                    subject_user_id=sender_id,
                    attribute_type="education_university",
                    raw_value=value,
                    normalized_value=value,
                    source_kind="self_report",
                    tense="current",
                    confidence=0.64,
                    evidence_message_row_ids=list(evidence_ids),
                    evidence_excerpt=text[:240],
                )
            )
        return claims

    def _extract_major_claims(
        self,
        sender_id: str,
        text: str,
        evidence_ids: list[int],
    ) -> list[ExtractedClaim]:
        claims: list[ExtractedClaim] = []
        for matched in MAJOR_PATTERN.findall(text):
            value = matched.strip()
            if len(value) < 2:
                continue
            claims.append(
                ExtractedClaim(
                    subject_user_id=sender_id,
                    attribute_type="education_major",
                    raw_value=value,
                    normalized_value=value,
                    source_kind="self_report",
                    tense="current",
                    confidence=0.58,
                    evidence_message_row_ids=list(evidence_ids),
                    evidence_excerpt=text[:240],
                )
            )
        return claims

    def _extract_hair_claims(
        self,
        sender_id: str,
        text: str,
        evidence_ids: list[int],
    ) -> list[ExtractedClaim]:
        claims: list[ExtractedClaim] = []
        for matched in HAIR_PATTERN.findall(text):
            value = matched.strip()
            if not value:
                continue
            claims.append(
                ExtractedClaim(
                    subject_user_id=sender_id,
                    attribute_type="appearance_hair",
                    raw_value=value,
                    normalized_value=value,
                    source_kind="self_report",
                    tense="current",
                    confidence=0.6,
                    evidence_message_row_ids=list(evidence_ids),
                    evidence_excerpt=text[:240],
                )
            )
        return claims

    def _extract_schedule_claims(
        self,
        sender_id: str,
        text: str,
        evidence_ids: list[int],
    ) -> list[ExtractedClaim]:
        lowered = text.lower()
        mapping = {
            "上课": "in_class",
            "下课": "after_class",
            "考试": "exam_period",
            "期末": "finals",
            "复习": "studying",
            "实习": "internship",
            "放假": "vacation",
            "军训": "military_training",
        }
        claims: list[ExtractedClaim] = []
        for keyword, normalized in mapping.items():
            if keyword not in text:
                continue
            claims.append(
                ExtractedClaim(
                    subject_user_id=sender_id,
                    attribute_type="schedule_status",
                    raw_value=keyword,
                    normalized_value=normalized,
                    source_kind="self_report",
                    tense="current",
                    confidence=0.56 if keyword in {"上课", "下课"} else 0.52,
                    evidence_message_row_ids=list(evidence_ids),
                    evidence_excerpt=text[:240],
                )
            )
        if "图书馆" in lowered:
            claims.append(
                ExtractedClaim(
                    subject_user_id=sender_id,
                    attribute_type="schedule_status",
                    raw_value="图书馆",
                    normalized_value="studying_in_library",
                    source_kind="self_report",
                    tense="current",
                    confidence=0.55,
                    evidence_message_row_ids=list(evidence_ids),
                    evidence_excerpt=text[:240],
                )
            )
        return claims

    def _extract_location_claims(
        self,
        sender_id: str,
        text: str,
        evidence_ids: list[int],
    ) -> list[ExtractedClaim]:
        hints = ("宿舍", "寝室", "图书馆", "教室", "学校", "校区")
        claims: list[ExtractedClaim] = []
        for hint in hints:
            if hint not in text:
                continue
            claims.append(
                ExtractedClaim(
                    subject_user_id=sender_id,
                    attribute_type="location_hint",
                    raw_value=hint,
                    normalized_value=hint,
                    source_kind="self_report",
                    tense="current",
                    confidence=0.45,
                    evidence_message_row_ids=list(evidence_ids),
                    evidence_excerpt=text[:240],
                )
            )
        return claims

    @staticmethod
    def _normalize_phone_value(value: str) -> str:
        cleaned = re.sub(r"\s+", " ", value.strip()).lower()
        aliases = {
            "15pm": "Apple iPhone 15 Pro Max",
            "16pm": "Apple iPhone 16 Pro Max",
            "15pro": "Apple iPhone 15 Pro",
            "16pro": "Apple iPhone 16 Pro",
            "安卓机": "Android Phone",
            "安卓": "Android Phone",
        }
        if cleaned in aliases:
            return aliases[cleaned]
        brand_aliases = {
            "iphone": "Apple ",
            "苹果": "Apple iPhone ",
            "华为": "Huawei ",
            "小米": "Xiaomi ",
            "redmi": "Redmi ",
            "荣耀": "Honor ",
            "vivo": "vivo ",
            "oppo": "OPPO ",
            "一加": "OnePlus ",
            "三星": "Samsung ",
        }
        for prefix, normalized_prefix in brand_aliases.items():
            if cleaned.startswith(prefix):
                suffix = value[len(prefix) :].strip()
                return f"{normalized_prefix}{suffix}".strip()
        return value.strip()


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
            "你在做群聊人物画像预筛选。请只找出“值得进一步提取画像事实”的消息片段。"
            "不要总结人物，不要脑补，不要输出解释性自然语言。"
            "返回 JSON，格式为 "
            '{"candidate_spans":[{"message_row_ids":[1,2],"subject_user_ids":["123"],'
            '"claim_types":["education_university"],"reason":"...","need_image_context":false}],'
            '"summary":{"candidate_count":1}}。'
            "claim_types 只能从这些枚举中选择："
            '["education_university","education_major","device_phone","appearance_hair","schedule_status","location_hint"]。'
            "如果没有值得提取的信息，candidate_spans 返回空数组。"
            "\n\n批次数据如下：\n"
            f"{json.dumps(payload, ensure_ascii=False)}"
        )
        try:
            data = await self._call_json_stage("judge", prompt=prompt)
        except Exception:
            return await self.fallback.judge_block(block)

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
            "你在做群聊人物画像事实抽取。请从给定消息中提取“可以入画像的事实 claim”。"
            "不要自由总结，不要输出多余文字，只返回 JSON。"
            "返回格式为 "
            '{"claims":[{"subject_user_id":"123","attribute_type":"device_phone","raw_value":"15pm",'
            '"normalized_value":"Apple iPhone 15 Pro Max","source_kind":"self_report",'
            '"tense":"current","polarity":"affirmed","confidence":0.82,'
            '"evidence_message_row_ids":[1],"evidence_excerpt":"我这15pm又发烫了","payload":{}}]}。'
            "attribute_type 只能从这些枚举中选择："
            '["education_university","education_major","device_phone","appearance_hair","schedule_status","location_hint"]。'
            "如果没有可抽取事实，claims 返回空数组。"
            "\n\n候选片段如下：\n"
            f"{json.dumps(payload, ensure_ascii=False)}"
        )
        image_urls = self._collect_image_urls(selected_messages)
        try:
            data = await self._call_json_stage(
                "extract",
                prompt=prompt,
                image_urls=image_urls,
            )
        except Exception:
            return await self.fallback.extract_claims(block, candidate_span)

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
            "你在做群聊人物画像 claim 合并与冲突消解。"
            "输入里有新 claim、已有当前属性、历史 claim。"
            "你需要判断哪些 claim 应该接受、哪些只是候选、哪些会覆盖当前值。"
            "不要输出自然语言说明，只返回 JSON。"
            "返回格式为 "
            '{"resolved_claims":[{"subject_user_id":"123","attribute_type":"device_phone",'
            '"raw_value":"15pm","normalized_value":"Apple iPhone 15 Pro Max","source_kind":"self_report",'
            '"tense":"current","polarity":"affirmed","confidence":0.86,"status":"accepted","current_value":true,'
            '"evidence_message_row_ids":[1],"evidence_excerpt":"我这15pm又发烫了","supersedes_claim_ids":[8],'
            '"merged_claim_ids":[],"note":"newer_stronger_claim","payload":{}}],'
            '"summary":{"resolved_count":1}}。'
            "status 只能是 candidate、accepted、outdated、conflicted 之一。"
            "如果同一用户同一 attribute_type 出现多个不同值，只能有一个 current_value=true，除非所有新 claim 都不应成为当前值。"
            "\n\n输入如下：\n"
            f"{json.dumps(payload, ensure_ascii=False)}"
        )
        try:
            data = await self._call_json_stage("resolve", prompt=prompt)
        except Exception:
            return await self.fallback.resolve_claims(
                block,
                extracted_claims,
                resolution_context,
            )

        resolved = [
            ResolvedClaim.from_mapping(item)
            for item in data.get("resolved_claims", []) or []
            if str(item.get("subject_user_id", "")).strip()
            and str(item.get("attribute_type", "")).strip()
            and str(item.get("normalized_value", "")).strip()
        ]
        return ResolutionResult(
            resolved_claims=resolved,
            summary=dict(data.get("summary", {}) or {})
            | {"mode": "astrbot_llm", "resolved_count": len(resolved)},
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
        kwargs: dict[str, Any] = {}
        model_name = settings.get_profile_stage_model(stage)
        if model_name:
            kwargs["model"] = model_name
        response = await self.context.llm_generate(
            chat_provider_id=provider_id,
            prompt=prompt,
            system_prompt=(
                "You are a structured information extraction engine. "
                "Return valid JSON only."
            ),
            image_urls=image_urls or None,
            **kwargs,
        )
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
