import asyncio
import logging
import json
import signal
import sys
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from config.settings import settings
from utils.db import init_db
from utils.clone_manager import get_clone_manager
import utils.clone_manager as clone_manager_module

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

_main_bot = None
_main_dp = None


async def _restore_active_polls(bot: Bot):
    from utils.db import get_db, is_mongo, get_sqlite_path
    from utils.poll_renderer import render_giveaway_message, build_vote_keyboard
    await asyncio.sleep(3)
    try:
        if is_mongo():
            db = get_db()
            polls = await db.giveaways.find(
                {"is_active": True, "message_id": {"$ne": None}}
            ).to_list(length=None)
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(
                    "SELECT * FROM giveaways WHERE is_active=1 AND message_id IS NOT NULL"
                ) as cur:
                    rows = await cur.fetchall()
            polls = []
            for r in rows:
                d = dict(r)
                d["prizes"]  = json.loads(d["prizes"])
                d["options"] = json.loads(d["options"])
                d["votes"]   = json.loads(d["votes"])
                d["is_active"] = bool(d["is_active"])
                polls.append(d)

        restored = 0
        for poll in polls:
            try:
                votes = {int(k): v for k, v in poll.get("votes", {}).items()}
                text = render_giveaway_message(
                    title=poll["title"], prizes=poll["prizes"],
                    options=poll["options"], votes=votes,
                    total_votes=poll["total_votes"], is_active=True
                )
                keyboard = build_vote_keyboard(
                    poll["giveaway_id"], poll["options"], is_active=True
                )
                await bot.edit_message_text(
                    text, chat_id=poll["channel_id"],
                    message_id=poll["message_id"],
                    reply_markup=keyboard, parse_mode="HTML"
                )
                restored += 1
                await asyncio.sleep(0.3)
            except Exception:
                pass
        if restored:
            logger.info(f"✅ Restored {restored} active poll(s) after restart")
    except Exception as e:
        logger.error(f"Poll restore error: {e}")


async def _graceful_shutdown():
    """
    Stop polling cleanly on SIGTERM so Telegram releases the getUpdates
    session IMMEDIATELY — prevents the 2-5 hour conflict on next deploy.
    """
    global _main_bot, _main_dp
    logger.info("🛑 Shutdown signal — stopping bot gracefully...")

    try:
        if _main_dp is not None:
            await _main_dp.stop_polling()
            logger.info("✅ Polling stopped")
    except Exception as e:
        logger.warning(f"stop_polling error: {e}")

    try:
        if _main_bot is not None:
            await _main_bot.delete_webhook(drop_pending_updates=False)
            await _main_bot.session.close()
            logger.info("✅ Bot session closed — Telegram lock released")
    except Exception as e:
        logger.warning(f"Bot close error: {e}")

    logger.info("👋 Shutdown complete")


def _install_signal_handlers(loop):
    def _handle():
        logger.info("📡 Signal caught — graceful shutdown")
        asyncio.ensure_future(_graceful_shutdown(), loop=loop)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle)
        except (NotImplementedError, RuntimeError):
            pass


async def _start_bot():
    global _main_bot, _main_dp

    try:
        bot = Bot(token=settings.BOT_TOKEN)
        _main_bot = bot

        # Force-release any stale Telegram polling session with retries
        for attempt in range(1, 6):
            try:
                await bot.delete_webhook(drop_pending_updates=True)
                logger.info(f"🔄 Webhook cleared (attempt {attempt})")
                break
            except Exception as e:
                logger.warning(f"delete_webhook attempt {attempt} failed: {e}")
                if attempt < 5:
                    await asyncio.sleep(3)

        # Give old instance time to fully release its session
        logger.info("⏳ Waiting 5s for old instance to release Telegram session...")
        await asyncio.sleep(5)

        storage = MemoryStorage()
        dp = Dispatcher(storage=storage)
        _main_dp = dp

        me = await bot.get_me()
        clone_manager_module.MAIN_BOT_USERNAME = me.username
        logger.info(f"🤖 Main bot: @{me.username}")

        from web.broadcaster import set_main_bot
        set_main_bot(bot)

        from utils.log_utils import set_main_bot as set_log_bot
        set_log_bot(bot)

        from utils.ban_middleware import BanMiddleware
        dp.message.middleware(BanMiddleware())
        dp.callback_query.middleware(BanMiddleware())

        import importlib
        for mod_name in [
            "handlers.start", "handlers.giveaway", "handlers.referral",
            "handlers.admin", "handlers.clone_bot", "handlers.stats",
        ]:
            if mod_name in sys.modules:
                importlib.reload(sys.modules[mod_name])
            else:
                importlib.import_module(mod_name)

        dp.include_router(sys.modules["handlers.start"].router)
        dp.include_router(sys.modules["handlers.admin"].router)
        dp.include_router(sys.modules["handlers.clone_bot"].router)
        dp.include_router(sys.modules["handlers.referral"].router)
        dp.include_router(sys.modules["handlers.stats"].router)
        dp.include_router(sys.modules["handlers.giveaway"].router)

        clone_manager = get_clone_manager()
        asyncio.create_task(clone_manager.start_all_clones())
        asyncio.create_task(_restore_active_polls(bot))

        from utils.snapshot_scheduler import set_bot as set_snap_bot, snapshot_loop
        set_snap_bot(bot)
        asyncio.create_task(snapshot_loop())

        from utils.keep_alive import set_domain, keep_alive_loop
        set_domain(settings.WEB_DOMAIN)
        asyncio.create_task(keep_alive_loop())

        from utils.premium_reminder import set_bot as set_prem_bot, premium_expiry_reminder_loop
        set_prem_bot(bot)
        asyncio.create_task(premium_expiry_reminder_loop())

        logger.info("🚀 Bot polling started!")
        await dp.start_polling(
            bot,
            allowed_updates=["message", "callback_query", "chat_member"],
            drop_pending_updates=True,
        )
    except Exception as e:
        logger.error(f"❌ Bot failed: {e}", exc_info=True)


async def _init_then_start_bot():
    try:
        logger.info("🔄 Initialising database...")
        await init_db()
        logger.info("✅ Database ready — starting bot")
        await _start_bot()
    except Exception as e:
        logger.error(f"❌ Startup failed: {e}", exc_info=True)


async def main():
    import uvicorn
    from web.app import app as fastapi_app

    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop)

    uvi_config = uvicorn.Config(
        fastapi_app,
        host="0.0.0.0",
        port=settings.WEB_PORT,
        log_level="warning",
        loop="none",
    )
    uvi_server = uvicorn.Server(uvi_config)

    asyncio.create_task(_init_then_start_bot())

    logger.info(f"🌐 Web server binding on port {settings.WEB_PORT}...")
    await uvi_server.serve()

    await _graceful_shutdown()


if __name__ == "__main__":
    asyncio.run(main())
