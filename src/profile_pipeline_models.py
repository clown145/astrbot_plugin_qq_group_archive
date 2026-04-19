from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class CandidateSpan:
    message_row_ids: list[int] = field(default_factory=list)
    subject_user_ids: list[str] = field(default_factory=list)
    claim_types: list[str] = field(default_factory=list)
    reason: str = ""
    need_image_context: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_row_ids": [int(value) for value in self.message_row_ids],
            "subject_user_ids": [str(value) for value in self.subject_user_ids if str(value).strip()],
            "claim_types": [str(value) for value in self.claim_types if str(value).strip()],
            "reason": self.reason,
            "need_image_context": bool(self.need_image_context),
        }

    @classmethod
    def from_mapping(cls, payload: dict[str, Any] | None) -> "CandidateSpan":
        values = dict(payload or {})
        return cls(
            message_row_ids=[
                int(value)
                for value in values.get("message_row_ids", [])
                if str(value).strip()
            ],
            subject_user_ids=[
                str(value).strip()
                for value in values.get("subject_user_ids", [])
                if str(value).strip()
            ],
            claim_types=[
                str(value).strip()
                for value in values.get("claim_types", [])
                if str(value).strip()
            ],
            reason=str(values.get("reason", "") or ""),
            need_image_context=bool(values.get("need_image_context", False)),
        )


@dataclass(slots=True)
class ExtractedClaim:
    subject_user_id: str
    attribute_type: str
    attribute_label: str
    raw_value: str
    normalized_value: str
    source_kind: str = "unknown"
    tense: str = "unknown"
    polarity: str = "affirmed"
    confidence: float = 0.0
    evidence_message_row_ids: list[int] = field(default_factory=list)
    evidence_excerpt: str = ""
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "subject_user_id": self.subject_user_id,
            "attribute_type": self.attribute_type,
            "attribute_label": self.attribute_label,
            "raw_value": self.raw_value,
            "normalized_value": self.normalized_value,
            "source_kind": self.source_kind,
            "tense": self.tense,
            "polarity": self.polarity,
            "confidence": float(self.confidence),
            "evidence_message_row_ids": [
                int(value) for value in self.evidence_message_row_ids
            ],
            "evidence_excerpt": self.evidence_excerpt,
            "payload": dict(self.payload),
        }

    @classmethod
    def from_mapping(cls, payload: dict[str, Any] | None) -> "ExtractedClaim":
        values = dict(payload or {})
        return cls(
            subject_user_id=str(values.get("subject_user_id", "") or "").strip(),
            attribute_type=str(values.get("attribute_type", "") or "").strip(),
            attribute_label=str(values.get("attribute_label", "") or "").strip(),
            raw_value=str(values.get("raw_value", "") or ""),
            normalized_value=str(values.get("normalized_value", "") or ""),
            source_kind=str(values.get("source_kind", "unknown") or "unknown"),
            tense=str(values.get("tense", "unknown") or "unknown"),
            polarity=str(values.get("polarity", "affirmed") or "affirmed"),
            confidence=float(values.get("confidence", 0.0) or 0.0),
            evidence_message_row_ids=[
                int(value)
                for value in values.get("evidence_message_row_ids", [])
                if str(value).strip()
            ],
            evidence_excerpt=str(values.get("evidence_excerpt", "") or ""),
            payload=dict(values.get("payload", {}) or {}),
        )


@dataclass(slots=True)
class ResolvedClaim:
    subject_user_id: str
    attribute_type: str
    attribute_label: str
    raw_value: str
    normalized_value: str
    source_kind: str = "unknown"
    tense: str = "unknown"
    polarity: str = "affirmed"
    confidence: float = 0.0
    status: str = "candidate"
    current_value: bool = False
    evidence_message_row_ids: list[int] = field(default_factory=list)
    evidence_excerpt: str = ""
    supersedes_claim_ids: list[int] = field(default_factory=list)
    merged_claim_ids: list[int] = field(default_factory=list)
    note: str = ""
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "subject_user_id": self.subject_user_id,
            "attribute_type": self.attribute_type,
            "attribute_label": self.attribute_label,
            "raw_value": self.raw_value,
            "normalized_value": self.normalized_value,
            "source_kind": self.source_kind,
            "tense": self.tense,
            "polarity": self.polarity,
            "confidence": float(self.confidence),
            "status": self.status,
            "current_value": bool(self.current_value),
            "evidence_message_row_ids": [
                int(value) for value in self.evidence_message_row_ids
            ],
            "evidence_excerpt": self.evidence_excerpt,
            "supersedes_claim_ids": [
                int(value) for value in self.supersedes_claim_ids
            ],
            "merged_claim_ids": [int(value) for value in self.merged_claim_ids],
            "note": self.note,
            "payload": dict(self.payload),
        }

    @classmethod
    def from_mapping(cls, payload: dict[str, Any] | None) -> "ResolvedClaim":
        values = dict(payload or {})
        return cls(
            subject_user_id=str(values.get("subject_user_id", "") or "").strip(),
            attribute_type=str(values.get("attribute_type", "") or "").strip(),
            attribute_label=str(values.get("attribute_label", "") or "").strip(),
            raw_value=str(values.get("raw_value", "") or ""),
            normalized_value=str(values.get("normalized_value", "") or ""),
            source_kind=str(values.get("source_kind", "unknown") or "unknown"),
            tense=str(values.get("tense", "unknown") or "unknown"),
            polarity=str(values.get("polarity", "affirmed") or "affirmed"),
            confidence=float(values.get("confidence", 0.0) or 0.0),
            status=str(values.get("status", "candidate") or "candidate"),
            current_value=bool(values.get("current_value", False)),
            evidence_message_row_ids=[
                int(value)
                for value in values.get("evidence_message_row_ids", [])
                if str(value).strip()
            ],
            evidence_excerpt=str(values.get("evidence_excerpt", "") or ""),
            supersedes_claim_ids=[
                int(value)
                for value in values.get("supersedes_claim_ids", [])
                if str(value).strip()
            ],
            merged_claim_ids=[
                int(value)
                for value in values.get("merged_claim_ids", [])
                if str(value).strip()
            ],
            note=str(values.get("note", "") or ""),
            payload=dict(values.get("payload", {}) or {}),
        )


@dataclass(slots=True)
class ResolutionAction:
    action_type: str
    subject_user_id: str = ""
    from_attribute_type: str = ""
    to_attribute_type: str = ""
    attribute_label: str = ""
    claim_ids: list[int] = field(default_factory=list)
    status: str = ""
    reason: str = ""
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.action_type,
            "subject_user_id": self.subject_user_id,
            "from_attribute_type": self.from_attribute_type,
            "to_attribute_type": self.to_attribute_type,
            "attribute_label": self.attribute_label,
            "claim_ids": [int(value) for value in self.claim_ids],
            "status": self.status,
            "reason": self.reason,
            "payload": dict(self.payload),
        }

    @classmethod
    def from_mapping(cls, payload: dict[str, Any] | None) -> "ResolutionAction":
        values = dict(payload or {})
        raw_claim_ids = values.get("claim_ids", []) or []
        if isinstance(raw_claim_ids, (str, int)):
            raw_claim_ids = [raw_claim_ids]
        return cls(
            action_type=str(
                values.get("type")
                or values.get("action_type")
                or ""
            ).strip(),
            subject_user_id=str(values.get("subject_user_id", "") or "").strip(),
            from_attribute_type=str(
                values.get("from_attribute_type")
                or values.get("source_attribute_type")
                or values.get("attribute_type")
                or ""
            ).strip(),
            to_attribute_type=str(
                values.get("to_attribute_type")
                or values.get("target_attribute_type")
                or ""
            ).strip(),
            attribute_label=str(values.get("attribute_label", "") or "").strip(),
            claim_ids=[
                int(value)
                for value in raw_claim_ids
                if str(value).strip()
            ],
            status=str(values.get("status", "") or "").strip(),
            reason=str(values.get("reason", "") or "").strip(),
            payload=dict(values.get("payload", {}) or {}),
        )


@dataclass(slots=True)
class JudgeResult:
    candidate_spans: list[CandidateSpan] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_spans": [item.to_dict() for item in self.candidate_spans],
            "summary": dict(self.summary),
        }


@dataclass(slots=True)
class ResolutionResult:
    resolved_claims: list[ResolvedClaim] = field(default_factory=list)
    actions: list[ResolutionAction] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "resolved_claims": [item.to_dict() for item in self.resolved_claims],
            "actions": [item.to_dict() for item in self.actions],
            "summary": dict(self.summary),
        }
