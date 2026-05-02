"""Global broadcast helper called from the web panel."""
import asyncio
import logging
from aiogram import Bot

logger = logging.getLogger(__name__)
_main_bot: Bot = None

def set_main_bot(bot: Bot):
    global _main_bot
    _main_bot = bot

async def do_global_broadcast(message: str):
    if not _main_bot:
        logger.error("Broadcast: main bot not set")
        return

    from utils.db import get_db, is_mongo, get_sqlite_path
    seen = set()
    sent = failed = 0

    # 1. All main bot users
    try:
        if is_mongo():
            db = get_db()
            async for u in db.main_bot_users.find({"is_banned": {"$ne": True}}, {"user_id": 1}):
                uid = u["user_id"]
                if uid in seen:
                    continue
                seen.add(uid)
                try:
                    await _main_bot.send_message(uid, f"📢 <b>Announcement</b>\n\n{message}", parse_mode="HTML")
                    sent += 1
                except Exception:
                    failed += 1
                await asyncio.sleep(0.05)
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                async with conn.execute(
                    "SELECT user_id FROM main_bot_users WHERE is_banned=0 OR is_banned IS NULL"
                ) as cur:
                    rows = await cur.fetchall()
            for row in rows:
                uid = row[0]
                if uid in seen:
                    continue
                seen.add(uid)
                try:
                    await _main_bot.send_message(uid, f"📢 <b>Announcement</b>\n\n{message}", parse_mode="HTML")
                    sent += 1
                except Exception:
                    failed += 1
                await asyncio.sleep(0.05)
    except Exception as e:
        logger.error(f"Broadcast main_bot_users error: {e}")

    # 2. Clone bot users not already seen
    try:
        from models.referral import get_all_users_for_clone, get_all_clone_bots
        clones = await get_all_clone_bots()
        for clone in clones:
            users = await get_all_users_for_clone(clone["token"])
            for u in users:
                uid = u["user_id"]
                if uid in seen:
                    continue
                seen.add(uid)
                try:
                    await _main_bot.send_message(uid, f"📢 <b>Announcement</b>\n\n{message}", parse_mode="HTML")
                    sent += 1
                except Exception:
                    failed += 1
                await asyncio.sleep(0.05)
    except Exception as e:
        logger.error(f"Broadcast clone users error: {e}")

    logger.info(f"Broadcast done: {sent} sent, {failed} failed, {len(seen)} unique")
