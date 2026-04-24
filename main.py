import asyncio
import logging
import json
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from config.settings import settings
from handlers import start, giveaway, referral, admin, clone_bot
from utils.db import init_db
from utils.clone_manager import get_clone_manager
import utils.clone_manager as clone_manager_module

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ─── Poll restore onrestart ──────────────────────────────────

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


# ─── Main ─────────────────────────────────────────────────────

async def main():
    # Init DB
    await init_db()

    # Bot + dispatcher
    storage = MemoryStorage()
    bot = Bot(token=settings.BOT_TOKEN)
    dp  = Dispatcher(storage=storage)

    # Set main bot username
    me = await bot.get_me()
    clone_manager_module.MAIN_BOT_USERNAME = me.username
    logger.info(f"🤖 Main bot: @{me.username}")

    # Broadcaster reference
    from web.broadcaster import set_main_bot
    set_main_bot(bot)

    # Register all routers
    dp.include_router(start.router)
    dp.include_router(giveaway.router)
    dp.include_router(referral.router)
    dp.include_router(admin.router)
    dp.include_router(clone_bot.router)

    # Start clone bots
    clone_manager = get_clone_manager()
    asyncio.create_task(clone_manager.start_all_clones())

    # Restore active polls after restart
    asyncio.create_task(_restore_active_polls(bot))

    # Channel member snapshot scheduler (every 30 min)
    from utils.snapshot_scheduler import set_bot as set_snap_bot, snapshot_loop
    set_snap_bot(bot)
    asyncio.create_task(snapshot_loop())
    logger.info("📸 Snapshot scheduler started")

    # Keep-alive pinger (Render free tier anti-sleep)
    from utils.keep_alive import set_domain, keep_alive_loop
    set_domain(settings.WEB_DOMAIN)
    asyncio.create_task(keep_alive_loop())

    # ── Web server runs in the SAME event loop as the bot ──
    # Use uvicorn's Config+Server API so it shares asyncio loop
    # This fixes the Internal Server Error caused by Motor MongoDB
    # client being used from a different thread/event loop.
    import uvicorn
    from web.app import app as fastapi_app

    uvi_config = uvicorn.Config(
        fastapi_app,
        host="0.0.0.0",
        port=settings.WEB_PORT,
        log_level="warning",
        loop="none",          # use the already-running loop
    )
    uvi_server = uvicorn.Server(uvi_config)

    logger.info(f"🌐 Web panel starting on port {settings.WEB_PORT}")
    logger.info(f"🔗 Admin: https://{settings.WEB_DOMAIN}/adminpanel/royalisbest/a?b3c")

    # Run bot polling and web server concurrently in the same event loop
    logger.info("🚀 Bot polling started!")
    await asyncio.gather(
        dp.start_polling(
            bot,
            allowed_updates=["message", "callback_query", "chat_member"]
        ),
        uvi_server.serve(),
    )


if __name__ == "__main__":
    asyncio.run(main())
