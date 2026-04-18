from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import filter
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)

from .src.config import PluginSettings
from .src.service import QQGroupArchiveService
from .src.storage import ArchiveDatabase
from .src.webui import ArchiveWebUIServer


class QQGroupArchivePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.context = context
        self.config = config
        self.data_dir = StarTools.get_data_dir("astrbot_plugin_qq_group_archive")
        self.db = ArchiveDatabase(self.data_dir / "archive.db")
        self.service = QQGroupArchiveService(
            data_dir=self.data_dir,
            db=self.db,
            config=config,
        )
        self.webui: ArchiveWebUIServer | None = None

    @filter.on_platform_loaded()
    async def on_platform_loaded(self):
        await self.service.initialize()
        await self._ensure_webui()

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    async def on_group_event(self, event: AiocqhttpMessageEvent):
        await self.service.archive_event(event)

    @filter.after_message_sent()
    async def on_after_message_sent(self, event: AiocqhttpMessageEvent):
        await self.service.archive_outgoing(event)

    @filter.command("归档状态")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    async def archive_status(self, event: AiocqhttpMessageEvent):
        yield event.plain_result(await self.service.get_group_status_text(event))

    @filter.command("归档统计")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    async def archive_stats(
        self,
        event: AiocqhttpMessageEvent,
        days: int | None = None,
    ):
        yield event.plain_result(
            await self.service.get_group_stats_text(event, days=days or 7)
        )

    async def terminate(self):
        if self.webui is not None:
            await self.webui.stop()
        await self.db.close()
        logger.info("qq_group_archive terminated")

    async def _ensure_webui(self):
        settings = PluginSettings.from_mapping(self.config)
        if not settings.webui_enabled:
            return
        if self.webui is None:
            self.webui = ArchiveWebUIServer(
                data_dir=self.data_dir,
                db=self.db,
                host=settings.webui_host,
                port=settings.webui_port,
                auth_token=settings.webui_auth_token,
            )
        if not self.webui.is_supported:
            logger.warning(
                "qq_group_archive webui requested but aiohttp is unavailable"
            )
            return
        try:
            await self.webui.start()
        except Exception as exc:
            logger.error("failed to start qq_group_archive webui: %s", exc)
