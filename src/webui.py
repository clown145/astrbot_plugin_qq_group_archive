from __future__ import annotations

import asyncio
import mimetypes
from pathlib import Path

from astrbot.api import logger

from .storage import ArchiveDatabase

try:
    from aiohttp import web
except ImportError:
    web = None


class ArchiveWebUIServer:
    def __init__(
        self,
        *,
        data_dir: Path,
        db: ArchiveDatabase,
        host: str,
        port: int,
        auth_token: str = "",
        profile_pipeline=None,
    ):
        self.data_dir = data_dir
        self.db = db
        self.host = host
        self.port = port
        self.auth_token = auth_token.strip()
        self.profile_pipeline = profile_pipeline
        self.media_dir = self.data_dir / "media"
        self.assets_dir = Path(__file__).resolve().parent / "webui_assets"
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._lock = asyncio.Lock()

    @property
    def is_supported(self) -> bool:
        return web is not None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    async def start(self):
        if not self.is_supported:
            raise RuntimeError("aiohttp is not installed")

        async with self._lock:
            if self._runner is not None:
                return

            app = web.Application(middlewares=self._build_middlewares())
            self._setup_routes(app)
            self._runner = web.AppRunner(app, access_log=None)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self.host, self.port)
            await self._site.start()
            logger.info("qq_group_archive webui started at %s", self.base_url)

    async def stop(self):
        async with self._lock:
            if self._site is not None:
                await self._site.stop()
                self._site = None
            if self._runner is not None:
                await self._runner.cleanup()
                self._runner = None

    def _build_middlewares(self):
        if not self.auth_token:
            return []

        @web.middleware
        async def auth_middleware(request, handler):
            return await self._auth_middleware(request, handler)

        return [auth_middleware]

    async def _auth_middleware(self, request, handler):
        path = request.path or "/"
        if not path.startswith("/api/"):
            return await handler(request)

        token = (
            request.headers.get("X-Auth-Token", "").strip()
            or request.query.get("token", "").strip()
        )
        if token != self.auth_token:
            return self._json_response(
                {"error": "unauthorized", "auth_required": True},
                status=401,
            )
        return await handler(request)

    def _setup_routes(self, app: "web.Application"):
        app.router.add_get("/", self._handle_index)
        app.router.add_get("/api/health", self._handle_health)
        app.router.add_get("/api/overview", self._handle_overview)
        app.router.add_get("/api/profile-pipeline/status", self._handle_profile_pipeline_status)
        app.router.add_post("/api/profile-pipeline/wake", self._handle_profile_pipeline_wake)
        app.router.add_post("/api/profile-pipeline/reset", self._handle_profile_pipeline_reset)
        app.router.add_get("/api/groups", self._handle_groups)
        app.router.add_get("/api/profiles/group", self._handle_group_profile_summary)
        app.router.add_get("/api/profiles/users", self._handle_group_profile_users)
        app.router.add_get(
            "/api/profiles/users/{user_id}",
            self._handle_user_group_profile,
        )
        app.router.add_get("/api/messages", self._handle_messages)
        app.router.add_get("/api/messages/{message_id:\\d+}", self._handle_message_detail)
        app.router.add_get("/api/notices", self._handle_notices)
        app.router.add_get("/api/notices/{notice_id:\\d+}", self._handle_notice_detail)
        app.router.add_get("/api/media/{relative_path:.*}", self._handle_media)
        app.router.add_static("/assets/", path=self.assets_dir, name="assets")

    async def _handle_index(self, _request):
        index_path = self.assets_dir / "index.html"
        if not index_path.is_file():
            return self._json_response({"error": "webui_not_found"}, status=404)
        return web.Response(
            text=index_path.read_text("utf-8"),
            content_type="text/html",
        )

    async def _handle_health(self, _request):
        return self._json_response(
            {
                "status": "ok",
                "db_path": str(self.db.db_path),
                "media_dir": str(self.media_dir),
                "auth_required": bool(self.auth_token),
            }
        )

    async def _handle_overview(self, _request):
        return self._json_response(await self.db.get_overview())

    async def _handle_profile_pipeline_status(self, _request):
        return self._json_response(await self._profile_pipeline_status_payload())

    async def _handle_profile_pipeline_wake(self, _request):
        if self.profile_pipeline is None:
            return self._json_response(
                {
                    "error": "profile_pipeline_unavailable",
                    "status": await self._profile_pipeline_status_payload(),
                },
                status=503,
            )
        runtime = self.profile_pipeline.get_runtime_status()
        if runtime.get("mode") != "astrbot_llm":
            return self._json_response(
                {
                    "error": "profile_pipeline_not_llm_mode",
                    "message": "profile_pipeline_mode must be astrbot_llm to run portrait extraction",
                    "status": await self._profile_pipeline_status_payload(),
                },
                status=409,
            )
        runtime = await self.profile_pipeline.trigger_once()
        return self._json_response(
            {
                "triggered": True,
                "runtime": runtime,
                "storage": await self.db.get_profile_pipeline_status(),
            }
        )

    async def _handle_profile_pipeline_reset(self, request):
        clear_claims = request.query.get("clear_claims", "1").strip() != "0"
        if self.profile_pipeline is not None:
            runtime = self.profile_pipeline.get_runtime_status()
            if runtime.get("mode") != "astrbot_llm":
                return self._json_response(
                    {
                        "error": "profile_pipeline_not_llm_mode",
                        "message": "profile_pipeline_mode must be astrbot_llm before resetting and rerunning portrait extraction",
                        "status": await self._profile_pipeline_status_payload(),
                    },
                    status=409,
                )
        reset_counts = await self.db.reset_profile_pipeline(clear_claims=clear_claims)
        runtime = None
        if self.profile_pipeline is not None:
            runtime = await self.profile_pipeline.trigger_once()
        return self._json_response(
            {
                "reset": True,
                "clear_claims": clear_claims,
                "deleted_counts": reset_counts,
                "runtime": runtime,
                "storage": await self.db.get_profile_pipeline_status(),
            }
        )

    async def _profile_pipeline_status_payload(self):
        runtime = None
        if self.profile_pipeline is not None:
            runtime = self.profile_pipeline.get_runtime_status()
        return {
            "runtime": runtime,
            "storage": await self.db.get_profile_pipeline_status(),
        }

    async def _handle_groups(self, request):
        search = request.query.get("search", "")
        limit = self._query_int(request, "limit", default=200, minimum=1, maximum=500)
        offset = self._query_int(request, "offset", default=0, minimum=0)
        data = await self.db.list_groups(limit=limit, offset=offset, search=search)
        return self._json_response(data)

    async def _handle_group_profile_summary(self, request):
        platform_id = request.query.get("platform_id", "").strip()
        group_id = request.query.get("group_id", "").strip()
        if not platform_id or not group_id:
            return self._json_response(
                {"error": "platform_id and group_id are required"},
                status=400,
            )
        data = await self.db.get_group_profile_summary(
            platform_id=platform_id,
            group_id=group_id,
        )
        return self._json_response(data)

    async def _handle_group_profile_users(self, request):
        platform_id = request.query.get("platform_id", "").strip()
        group_id = request.query.get("group_id", "").strip()
        if not platform_id or not group_id:
            return self._json_response(
                {"error": "platform_id and group_id are required"},
                status=400,
            )

        search = request.query.get("search", "")
        limit = self._query_int(request, "limit", default=50, minimum=1, maximum=200)
        offset = self._query_int(request, "offset", default=0, minimum=0)
        data = await self.db.list_group_profile_users(
            platform_id=platform_id,
            group_id=group_id,
            limit=limit,
            offset=offset,
            search=search,
        )
        return self._json_response(data)

    async def _handle_user_group_profile(self, request):
        platform_id = request.query.get("platform_id", "").strip()
        group_id = request.query.get("group_id", "").strip()
        user_id = request.match_info.get("user_id", "").strip()
        if not platform_id or not group_id or not user_id:
            return self._json_response(
                {"error": "platform_id, group_id, and user_id are required"},
                status=400,
            )

        data = await self.db.get_user_group_profile(
            platform_id=platform_id,
            group_id=group_id,
            user_id=user_id,
        )
        if data is None:
            return self._json_response({"error": "profile_not_found"}, status=404)
        return self._json_response(data)

    async def _handle_messages(self, request):
        platform_id = request.query.get("platform_id", "").strip()
        group_id = request.query.get("group_id", "").strip()
        if not platform_id or not group_id:
            return self._json_response(
                {"error": "platform_id and group_id are required"},
                status=400,
            )

        direction = request.query.get("direction", "")
        search = request.query.get("search", "")
        limit = self._query_int(request, "limit", default=50, minimum=1, maximum=200)
        offset = self._query_int(request, "offset", default=0, minimum=0)
        data = await self.db.list_messages(
            platform_id=platform_id,
            group_id=group_id,
            limit=limit,
            offset=offset,
            direction=direction,
            search=search,
        )
        return self._json_response(data)

    async def _handle_message_detail(self, request):
        message_id = self._path_int(request, "message_id")
        detail = await self.db.get_message_detail(message_id)
        if detail is None:
            return self._json_response({"error": "message_not_found"}, status=404)
        return self._json_response(detail)

    async def _handle_notices(self, request):
        platform_id = request.query.get("platform_id", "").strip()
        group_id = request.query.get("group_id", "").strip()
        if not platform_id or not group_id:
            return self._json_response(
                {"error": "platform_id and group_id are required"},
                status=400,
            )

        notice_type = request.query.get("notice_type", "")
        limit = self._query_int(request, "limit", default=50, minimum=1, maximum=200)
        offset = self._query_int(request, "offset", default=0, minimum=0)
        data = await self.db.list_notices(
            platform_id=platform_id,
            group_id=group_id,
            limit=limit,
            offset=offset,
            notice_type=notice_type,
        )
        return self._json_response(data)

    async def _handle_notice_detail(self, request):
        notice_id = self._path_int(request, "notice_id")
        detail = await self.db.get_notice_detail(notice_id)
        if detail is None:
            return self._json_response({"error": "notice_not_found"}, status=404)
        return self._json_response(detail)

    async def _handle_media(self, request):
        relative_path = request.match_info.get("relative_path", "").strip()
        if not relative_path:
            return self._json_response({"error": "missing_media_path"}, status=400)

        candidate = (self.data_dir / relative_path).resolve()
        media_root = self.media_dir.resolve()
        if media_root not in candidate.parents:
            return self._json_response({"error": "forbidden"}, status=403)
        if not candidate.is_file():
            return self._json_response({"error": "media_not_found"}, status=404)

        content_type = mimetypes.guess_type(str(candidate))[0]
        return web.FileResponse(candidate, headers=self._content_type_header(content_type))

    @staticmethod
    def _path_int(request, key: str) -> int:
        value = request.match_info.get(key, "").strip()
        return int(value)

    @staticmethod
    def _query_int(
        request,
        key: str,
        *,
        default: int,
        minimum: int,
        maximum: int | None = None,
    ) -> int:
        raw_value = request.query.get(key, "").strip()
        if not raw_value:
            value = default
        else:
            value = int(raw_value)
        value = max(value, minimum)
        if maximum is not None:
            value = min(value, maximum)
        return value

    @staticmethod
    def _content_type_header(content_type: str | None) -> dict[str, str]:
        if not content_type:
            return {}
        return {"Content-Type": content_type}

    @staticmethod
    def _json_response(payload, *, status: int = 200):
        return web.json_response(payload, status=status, dumps=_json_dumps)


def _json_dumps(value):
    import json

    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
