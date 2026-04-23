"""
Periodically snapshots channel member counts for all active panels.
Runs every 30 minutes.
"""
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
_bot = None


def set_bot(bot):
    global _bot
    _bot = bot


async def _fetch_member_count(channel_id: str) -> int:
    if not _bot:
        return 0
    try:
        chat = await _bot.get_chat(channel_id)
        count = getattr(chat, 'member_count', None)
        if count is None:
            count = await _bot.get_chat_member_count(channel_id)
        return count or 0
    except Exception:
        return 0


async def snapshot_loop():
    """Runs forever, snapshotting every 30 minutes."""
    while True:
        try:
            await _do_snapshot_all()
        except Exception as e:
            logger.error(f"Snapshot error: {e}")
        await asyncio.sleep(1800)  # 30 min


async def _do_snapshot_all():
    from utils.db import is_mongo, get_db, get_sqlite_path
    from models.panel import add_snapshot

    if is_mongo():
        db = get_db()
        panels = await db.panels.find({"is_deleted": False}).to_list(length=None)
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT * FROM panels WHERE is_deleted=0"
            ) as cur:
                rows = await cur.fetchall()
        panels = [dict(r) for r in rows]

    for p in panels:
        channel_id = p.get("channel_id")
        if not channel_id:
            continue
        count = await _fetch_member_count(channel_id)
        if count > 0:
            await add_snapshot(p["token"], count)
        await asyncio.sleep(0.5)

    if panels:
        logger.info(f"✅ Snapshots taken for {len(panels)} panel(s)")
