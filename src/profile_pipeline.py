from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypedDict

from astrbot.api import logger
from astrbot.api.star import Context

from .config import PluginSettings
from .profile_llm import ProfilePipelineLLM, build_profile_llm
from .profile_pipeline_models import CandidateSpan, ExtractedClaim, JudgeResult
from .storage import ArchiveDatabase

try:
    from langgraph.graph import END, START, StateGraph

    LANGGRAPH_AVAILABLE = True
except ImportError:
    END = START = StateGraph = None
    LANGGRAPH_AVAILABLE = False


class ProfileWorkflowState(TypedDict, total=False):
    job_id: int
    job_context: dict[str, Any]
    block: dict[str, Any]
    messages: list[dict[str, Any]]
    candidate_spans: list[dict[str, Any]]
    extracted_claims: list[dict[str, Any]]
    resolution_context: dict[str, Any]
    resolved_claims: list[dict[str, Any]]
    resolution_actions: list[dict[str, Any]]
    summary: dict[str, Any]


@dataclass(slots=True)
class ProfilePipelineService:
    db: ArchiveDatabase
    config: Any
    context: Context
    data_dir: Path
    llm_client: ProfilePipelineLLM | None = None
    _runner_task: asyncio.Task | None = None
    _wake_event: asyncio.Event = field(default_factory=asyncio.Event)
    _graph: Any = None
    _llm_mode: str = ""

    def __post_init__(self):
        if LANGGRAPH_AVAILABLE:
            self._graph = self._build_graph()

    @property
    def is_supported(self) -> bool:
        return bool(LANGGRAPH_AVAILABLE and self._graph is not None)

    def get_runtime_status(self) -> dict[str, Any]:
        settings = PluginSettings.from_mapping(self.config)
        return {
            "enabled": settings.profile_pipeline_enabled,
            "mode": settings.profile_pipeline_mode,
            "langgraph_available": LANGGRAPH_AVAILABLE,
            "supported": self.is_supported,
            "runner_running": self._runner_task is not None
            and not self._runner_task.done(),
            "llm_mode": self._llm_mode,
            "poll_interval_sec": settings.profile_pipeline_poll_interval_sec,
            "batch_message_limit": settings.profile_pipeline_batch_message_limit,
            "min_batch_messages": settings.profile_pipeline_min_batch_messages,
            "batch_overlap": settings.profile_pipeline_batch_overlap,
            "max_jobs_per_tick": settings.profile_pipeline_max_jobs_per_tick,
            "llm_timeout_sec": settings.profile_pipeline_llm_timeout_sec,
            "running_job_timeout_sec": settings.profile_pipeline_running_job_timeout_sec,
        }

    async def start(self):
        settings = PluginSettings.from_mapping(self.config)
        if not settings.profile_pipeline_enabled:
            logger.info("qq_group_archive profile pipeline disabled in config")
            return
        if not self.is_supported:
            logger.warning(
                "qq_group_archive profile pipeline requested but langgraph is unavailable"
            )
            return
        self._ensure_llm(settings)
        if self._runner_task is None or self._runner_task.done():
            recovered = await self.db.recover_stale_profile_jobs(
                timeout_sec=settings.profile_pipeline_running_job_timeout_sec,
                force=True,
            )
            if recovered:
                logger.warning(
                    "qq_group_archive recovered %s running profile jobs on startup",
                    recovered,
                )
            self._runner_task = asyncio.create_task(self._run_loop())
            logger.info(
                "qq_group_archive profile pipeline started in %s mode",
                self._llm_mode,
            )

    async def stop(self):
        if self._runner_task is None:
            return
        self._runner_task.cancel()
        try:
            await self._runner_task
        except asyncio.CancelledError:
            pass
        self._runner_task = None

    async def wake(self):
        self._wake_event.set()

    async def trigger_once(self) -> dict[str, Any]:
        await self.start()
        await self.wake()
        return self.get_runtime_status()

    async def _run_loop(self):
        while True:
            settings = PluginSettings.from_mapping(self.config)
            self._ensure_llm(settings)
            try:
                await self._tick(settings)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("qq_group_archive profile pipeline tick failed: %s", exc)

            try:
                await asyncio.wait_for(
                    self._wake_event.wait(),
                    timeout=settings.profile_pipeline_poll_interval_sec,
                )
            except asyncio.TimeoutError:
                pass
            self._wake_event.clear()

    async def _tick(self, settings: PluginSettings):
        recovered = await self.db.recover_stale_profile_jobs(
            timeout_sec=settings.profile_pipeline_running_job_timeout_sec,
            force=False,
        )
        if recovered:
            logger.warning(
                "qq_group_archive recovered %s stale running profile jobs",
                recovered,
            )

        await self.db.create_profile_message_blocks(
            batch_message_limit=settings.profile_pipeline_batch_message_limit,
            min_batch_messages=min(
                settings.profile_pipeline_min_batch_messages,
                settings.profile_pipeline_batch_message_limit,
            ),
            batch_overlap=min(
                settings.profile_pipeline_batch_overlap,
                max(settings.profile_pipeline_batch_message_limit - 1, 0),
            ),
            max_blocks=settings.profile_pipeline_max_jobs_per_tick,
        )

        processed = 0
        while processed < settings.profile_pipeline_max_jobs_per_tick:
            job = await self.db.claim_next_profile_job()
            if job is None:
                break
            await self._run_job(int(job["id"]))
            processed += 1

    async def _run_job(self, job_id: int):
        if self._graph is None:
            return
        try:
            await self._graph.ainvoke({"job_id": int(job_id)})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self.db.fail_profile_job(
                job_id=int(job_id),
                error_text=str(exc),
                workflow_state={"job_id": int(job_id), "error": str(exc)},
            )
            logger.error("qq_group_archive profile job %s failed: %s", job_id, exc)

    def _ensure_llm(self, settings: PluginSettings):
        mode = settings.profile_pipeline_mode
        if self.llm_client is None or self._llm_mode != mode:
            self.llm_client = build_profile_llm(
                mode,
                context=self.context,
                config=self.config,
                data_dir=self.data_dir,
            )
            self._llm_mode = mode

    def _build_graph(self):
        builder = StateGraph(ProfileWorkflowState)
        builder.add_node("load_job", self._load_job)
        builder.add_node("judge_block", self._judge_block)
        builder.add_node("extract_claims", self._extract_claims)
        builder.add_node("resolve_claims", self._resolve_claims)
        builder.add_node("persist_claims", self._persist_claims)
        builder.add_node("persist_without_claims", self._persist_without_claims)

        builder.add_edge(START, "load_job")
        builder.add_edge("load_job", "judge_block")
        builder.add_conditional_edges(
            "judge_block",
            self._route_after_judge,
            {
                "extract_claims": "extract_claims",
                "persist_without_claims": "persist_without_claims",
            },
        )
        builder.add_edge("extract_claims", "resolve_claims")
        builder.add_edge("resolve_claims", "persist_claims")
        builder.add_edge("persist_claims", END)
        builder.add_edge("persist_without_claims", END)
        return builder.compile()

    async def _load_job(self, state: ProfileWorkflowState) -> ProfileWorkflowState:
        await self._mark_progress(
            int(state["job_id"]),
            "load_job",
            "读取任务上下文",
        )
        context = await self.db.get_profile_job_context(int(state["job_id"]))
        if context is None:
            raise RuntimeError(f"profile job {state['job_id']} not found")
        return {
            "job_context": context,
            "block": {
                "block_id": int(context["block_id"]),
                "block_key": str(context.get("block_key") or ""),
                "platform_id": str(context.get("platform_id") or ""),
                "group_id": str(context.get("group_id") or ""),
                "group_name": str(context.get("group_name") or ""),
                "message_count": int(context.get("message_count") or 0),
                "first_event_at": int(context.get("first_event_at") or 0),
                "last_event_at": int(context.get("last_event_at") or 0),
                "messages": list(context.get("messages") or []),
            },
            "messages": list(context.get("messages") or []),
            "summary": {},
        }

    async def _judge_block(self, state: ProfileWorkflowState) -> ProfileWorkflowState:
        assert self.llm_client is not None
        block = dict(state.get("block") or {})
        block["messages"] = list(state.get("messages") or [])
        await self._mark_progress(
            int(state["job_id"]),
            "judge_block",
            "调用候选判断模型",
            state=state,
        )
        result: JudgeResult = await self.llm_client.judge_block(block)
        summary = dict(state.get("summary") or {})
        summary["judge"] = result.summary
        await self._mark_progress(
            int(state["job_id"]),
            "judge_done",
            f"候选片段 {len(result.candidate_spans)} 个",
            state={
                **state,
                "candidate_spans": [item.to_dict() for item in result.candidate_spans],
                "summary": summary,
            },
        )
        return {
            "candidate_spans": [item.to_dict() for item in result.candidate_spans],
            "summary": summary,
        }

    def _route_after_judge(self, state: ProfileWorkflowState) -> str:
        if state.get("candidate_spans"):
            return "extract_claims"
        return "persist_without_claims"

    async def _extract_claims(self, state: ProfileWorkflowState) -> ProfileWorkflowState:
        assert self.llm_client is not None
        block = dict(state.get("block") or {})
        block["messages"] = list(state.get("messages") or [])
        claims: list[dict[str, Any]] = []
        spans = list(state.get("candidate_spans", []) or [])
        for index, span_payload in enumerate(spans, start=1):
            span = CandidateSpan.from_mapping(span_payload)
            await self._mark_progress(
                int(state["job_id"]),
                "extract_claims",
                f"抽取候选片段 {index}/{len(spans)}",
                state={**state, "extracted_claims": claims},
                extra={"current_span": span.to_dict()},
            )
            extracted = await self.llm_client.extract_claims(block, span)
            claims.extend([item.to_dict() for item in extracted])
            await self._mark_progress(
                int(state["job_id"]),
                "extract_claims",
                f"已抽取 {index}/{len(spans)}，累计 claim {len(claims)} 条",
                state={**state, "extracted_claims": claims},
                extra={"current_span": span.to_dict()},
            )

        summary = dict(state.get("summary") or {})
        summary["extract"] = {
            "candidate_count": len(state.get("candidate_spans") or []),
            "claim_count": len(claims),
        }
        return {"extracted_claims": claims, "summary": summary}

    async def _resolve_claims(self, state: ProfileWorkflowState) -> ProfileWorkflowState:
        assert self.llm_client is not None
        extracted_claims = [
            ExtractedClaim.from_mapping(payload)
            for payload in state.get("extracted_claims", []) or []
        ]
        subject_user_ids = sorted(
            {item.subject_user_id for item in extracted_claims if item.subject_user_id}
        )
        attribute_types = sorted(
            {item.attribute_type for item in extracted_claims if item.attribute_type}
        )
        block = dict(state.get("block") or {})
        block["messages"] = list(state.get("messages") or [])
        await self._mark_progress(
            int(state["job_id"]),
            "resolve_claims",
            f"合并消歧 {len(extracted_claims)} 条 claim",
            state=state,
        )
        resolution_context = await self.db.get_profile_resolution_context(
            platform_id=str(block.get("platform_id") or ""),
            group_id=str(block.get("group_id") or ""),
            subject_user_ids=subject_user_ids,
            attribute_types=attribute_types,
        )
        result = await self.llm_client.resolve_claims(
            block,
            extracted_claims,
            resolution_context,
        )
        summary = dict(state.get("summary") or {})
        summary["resolve"] = result.summary
        await self._mark_progress(
            int(state["job_id"]),
            "resolve_done",
            f"消解结果 {len(result.resolved_claims)} 条，整理动作 {len(result.actions)} 个",
            state={
                **state,
                "resolution_context": resolution_context,
                "resolved_claims": [item.to_dict() for item in result.resolved_claims],
                "resolution_actions": [item.to_dict() for item in result.actions],
                "summary": summary,
            },
        )
        return {
            "resolution_context": resolution_context,
            "resolved_claims": [item.to_dict() for item in result.resolved_claims],
            "resolution_actions": [item.to_dict() for item in result.actions],
            "summary": summary,
        }

    async def _persist_claims(self, state: ProfileWorkflowState) -> ProfileWorkflowState:
        summary = dict(state.get("summary") or {})
        await self._mark_progress(
            int(state["job_id"]),
            "persist_claims",
            f"写入 {len(state.get('resolved_claims') or [])} 条结果",
            state=state,
        )
        persist_result = await self.db.apply_profile_resolution(
            job_id=int(state["job_id"]),
            resolved_claims=list(state.get("resolved_claims") or []),
            resolution_actions=list(state.get("resolution_actions") or []),
            summary=summary,
            workflow_state=self._workflow_state_payload(state),
            block_messages=list(state.get("messages") or []),
        )
        logger.info(
            "qq_group_archive profile job %s completed with %s claims / %s attributes / %s actions",
            state["job_id"],
            persist_result.get("inserted_claims"),
            persist_result.get("updated_attributes"),
            persist_result.get("actions_applied"),
        )
        return {}

    async def _persist_without_claims(
        self,
        state: ProfileWorkflowState,
    ) -> ProfileWorkflowState:
        summary = dict(state.get("summary") or {})
        summary["extract"] = {"candidate_count": 0, "claim_count": 0}
        await self._mark_progress(
            int(state["job_id"]),
            "persist_without_claims",
            "没有候选片段，直接完成",
            state={**state, "summary": summary},
        )
        await self.db.complete_profile_job(
            job_id=int(state["job_id"]),
            summary=summary,
            workflow_state=self._workflow_state_payload(state),
        )
        return {}

    async def _mark_progress(
        self,
        job_id: int,
        stage: str,
        detail: str = "",
        *,
        state: ProfileWorkflowState | dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ):
        payload = self._progress_state_payload(state or {})
        if extra:
            payload.update(extra)
        await self.db.update_profile_job_progress(
            job_id=int(job_id),
            stage=stage,
            stage_detail=detail,
            workflow_state=payload,
        )

    @staticmethod
    def _workflow_state_payload(state: ProfileWorkflowState) -> dict[str, Any]:
        return {
            "candidate_spans": list(state.get("candidate_spans") or []),
            "extracted_claims": list(state.get("extracted_claims") or []),
            "resolved_claims": list(state.get("resolved_claims") or []),
            "resolution_actions": list(state.get("resolution_actions") or []),
            "summary": dict(state.get("summary") or {}),
        }

    @staticmethod
    def _progress_state_payload(state: ProfileWorkflowState | dict[str, Any]) -> dict[str, Any]:
        return {
            "candidate_count": len(state.get("candidate_spans") or []),
            "extracted_claim_count": len(state.get("extracted_claims") or []),
            "resolved_claim_count": len(state.get("resolved_claims") or []),
            "resolution_action_count": len(state.get("resolution_actions") or []),
            "summary": dict(state.get("summary") or {}),
        }
