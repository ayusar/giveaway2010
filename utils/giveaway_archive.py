"""
utils/giveaway_archive.py

When a giveaway is closed:
  1. Serialize full data (giveaway + all votes) to JSON
  2. Send the JSON file to the DATABASE_CHANNEL in Telegram
  3. Delete the giveaway + its votes from MongoDB / SQLite
  4. Store enriched metadata (title, creator_id, created_at, end_date) in
     giveaway_archive_refs so the admin panel and /oldgiveaway command work.

Admin can later request old giveaway data — bot sends the stored file back.
Users with a panel link can also view their closed giveaway via /oldgiveaway ID.
"""
from __future__ import annotations

import json
import logging
import io
from datetime import datetime, timezone
from aiogram import Bot
from config.settings import settings

logger = logging.getLogger(__name__)


async def archive_and_purge(bot: Bot, giveaway_id: str) -> bool:
    """
    Fetch full giveaway data, send to DATABASE_CHANNEL as a JSON file,
    store enriched metadata, then delete from the live database.
    Returns True on success.
    """
    db_channel = getattr(settings, "DATABASE_CHANNEL", None)
    if not db_channel:
        logger.warning("archive_and_purge: DATABASE_CHANNEL not set — skipping archive")
        return False

    # Always use the main bot to send to DATABASE_CHANNEL
    # (the bot passed in may be a clone bot that isn't admin in the channel)
    from utils.log_utils import get_main_bot
    send_bot = get_main_bot() or bot

    from utils.db import get_db, is_mongo, get_sqlite_path

    # ── 1. Fetch full data ────────────────────────────────────
    giveaway = None
    all_votes = []

    if is_mongo():
        db = get_db()
        giveaway = await db.giveaways.find_one({"giveaway_id": giveaway_id})
        if giveaway:
            giveaway.pop("_id", None)
            raw_votes = await db.votes.find({"giveaway_id": giveaway_id}).to_list(None)
            for v in raw_votes:
                v.pop("_id", None)
                all_votes.append(v)
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT * FROM giveaways WHERE giveaway_id=?", (giveaway_id,)
            ) as cur:
                row = await cur.fetchone()
            if row:
                giveaway = dict(row)
                giveaway["prizes"]  = json.loads(giveaway["prizes"])
                giveaway["options"] = json.loads(giveaway["options"])
                giveaway["votes"]   = json.loads(giveaway["votes"])
                async with conn.execute(
                    "SELECT * FROM votes WHERE giveaway_id=?", (giveaway_id,)
                ) as cur:
                    all_votes = [dict(r) for r in await cur.fetchall()]

    if not giveaway:
        logger.warning(f"archive_and_purge: giveaway {giveaway_id} not found")
        return False

    # ── 2. Build archive payload ──────────────────────────────
    archive = {
        "archived_at": datetime.now(timezone.utc).isoformat(),
        "giveaway":    giveaway,
        "votes":       all_votes,
    }

    def _default(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    json_bytes = json.dumps(archive, default=_default, ensure_ascii=False, indent=2).encode()
    file_obj   = io.BytesIO(json_bytes)
    file_obj.name = f"giveaway_{giveaway_id}.json"

    title       = giveaway.get("title", giveaway_id)
    total_v     = giveaway.get("total_votes", 0)
    creator_id  = giveaway.get("creator_id", 0)
    created_at  = str(giveaway.get("created_at", ""))[:19]
    end_date    = str(giveaway.get("end_time", ""))[:19] or str(datetime.now(timezone.utc))[:19]
    archived_at = datetime.now(timezone.utc).isoformat()

    caption = (
        f"📦 <b>Giveaway Archived</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 <b>ID:</b> <code>{giveaway_id}</code>\n"
        f"🏷 <b>Title:</b> {title}\n"
        f"👥 <b>Total Votes:</b> {total_v}\n"
        f"📅 <b>Created:</b> {created_at} UTC\n"
        f"⏰ <b>Ended:</b> {end_date} UTC\n"
        f"📦 <b>Archived:</b> {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n\n"
        f"⬇️ Download this file to restore or view old data.\n"
        f"Use <code>/getgiveaway {giveaway_id}</code> in bot to retrieve it."
    )

    # ── 3. Send to DATABASE_CHANNEL ───────────────────────────
    try:
        sent_msg = await send_bot.send_document(
            db_channel,
            document=file_obj,
            caption=caption,
            parse_mode="HTML",
        )
        file_id = sent_msg.document.file_id
        logger.info(f"✅ Giveaway {giveaway_id} archived to DATABASE_CHANNEL")
    except Exception as e:
        logger.error(f"archive_and_purge: failed to send to DATABASE_CHANNEL: {e}")
        return False

    # ── 3b. Store enriched metadata for admin panel + /oldgiveaway ──
    try:
        if is_mongo():
            await get_db().giveaway_archive_refs.update_one(
                {"giveaway_id": giveaway_id},
                {"$set": {
                    "giveaway_id": giveaway_id,
                    "file_id":     file_id,
                    "title":       title,
                    "creator_id":  creator_id,
                    "created_at":  created_at,
                    "end_date":    end_date,
                    "total_votes": total_v,
                    "archived_at": archived_at,
                }},
                upsert=True,
            )
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                await conn.execute(
                    """CREATE TABLE IF NOT EXISTS giveaway_archive_refs (
                        giveaway_id TEXT PRIMARY KEY,
                        file_id     TEXT,
                        title       TEXT,
                        creator_id  INTEGER,
                        created_at  TEXT,
                        end_date    TEXT,
                        total_votes INTEGER DEFAULT 0,
                        archived_at TEXT
                    )"""
                )
                await conn.execute(
                    """INSERT OR REPLACE INTO giveaway_archive_refs
                       (giveaway_id, file_id, title, creator_id, created_at, end_date, total_votes, archived_at)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (giveaway_id, file_id, title, creator_id,
                     created_at, end_date, total_v, archived_at)
                )
                await conn.commit()
    except Exception as e:
        logger.warning(f"archive_and_purge: failed to store metadata: {e}")

    # ── 4. Purge from live database ───────────────────────────
    try:
        if is_mongo():
            db = get_db()
            await db.giveaways.delete_one({"giveaway_id": giveaway_id})
            await db.votes.delete_many({"giveaway_id": giveaway_id})
            await db.panels.delete_many({"ref_id": giveaway_id, "panel_type": "giveaway"})
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                await conn.execute("DELETE FROM giveaways WHERE giveaway_id=?", (giveaway_id,))
                await conn.execute("DELETE FROM votes WHERE giveaway_id=?", (giveaway_id,))
                try:
                    await conn.execute(
                        "DELETE FROM panels WHERE ref_id=? AND panel_type='giveaway'",
                        (giveaway_id,)
                    )
                except Exception:
                    pass
                await conn.commit()
        logger.info(f"🗑 Giveaway {giveaway_id} purged from live DB after archiving")
    except Exception as e:
        logger.error(f"archive_and_purge: purge failed: {e}")

    return True


async def get_old_giveaways(limit: int = 200) -> list[dict]:
    """Return all archived giveaway metadata, newest first."""
    from utils.db import get_db, is_mongo, get_sqlite_path
    try:
        if is_mongo():
            docs = await get_db().giveaway_archive_refs.find(
                {}, {"_id": 0}
            ).sort("archived_at", -1).limit(limit).to_list(None)
            return docs or []
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                try:
                    conn.row_factory = aiosqlite.Row
                    async with conn.execute(
                        """SELECT giveaway_id, file_id, title, creator_id,
                                  created_at, end_date, total_votes, archived_at
                           FROM giveaway_archive_refs
                           ORDER BY archived_at DESC LIMIT ?""", (limit,)
                    ) as cur:
                        return [dict(r) for r in await cur.fetchall()]
                except Exception:
                    return []
    except Exception as e:
        logger.error(f"get_old_giveaways error: {e}")
        return []


async def delete_old_giveaways_before(months: int) -> int:
    """Delete archive metadata older than `months` months. Returns count deleted."""
    from utils.db import get_db, is_mongo, get_sqlite_path
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=months * 30)).isoformat()
    try:
        if is_mongo():
            result = await get_db().giveaway_archive_refs.delete_many(
                {"archived_at": {"$lt": cutoff}}
            )
            return result.deleted_count
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                async with conn.execute(
                    "SELECT COUNT(*) FROM giveaway_archive_refs WHERE archived_at < ?", (cutoff,)
                ) as cur:
                    count = (await cur.fetchone())[0]
                await conn.execute(
                    "DELETE FROM giveaway_archive_refs WHERE archived_at < ?", (cutoff,)
                )
                await conn.commit()
                return count
    except Exception as e:
        logger.error(f"delete_old_giveaways_before error: {e}")
        return 0
