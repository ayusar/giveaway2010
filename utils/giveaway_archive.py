"""
utils/giveaway_archive.py

When a giveaway is closed:
  1. Serialize full data (giveaway + all votes) to JSON
  2. Send the JSON file to the DATABASE_CHANNEL in Telegram
  3. Delete the giveaway + its votes from MongoDB / SQLite
  4. Store enriched metadata in giveaway_archive_refs
"""
from __future__ import annotations

import json
import logging
import io
import asyncio
from aiogram.types import BufferedInputFile
from datetime import datetime, timezone
from aiogram import Bot
from config.settings import settings

logger = logging.getLogger(__name__)

# Timeout in seconds for each DB/Telegram operation
_TIMEOUT = 20


async def archive_and_purge(bot: Bot, giveaway_id: str) -> bool:
    print(f"[ARCHIVE] Starting archive for giveaway {giveaway_id}", flush=True)

    db_channel = getattr(settings, "DATABASE_CHANNEL", None)
    print(f"[ARCHIVE] DATABASE_CHANNEL = {db_channel!r}", flush=True)
    if not db_channel:
        logger.warning("archive_and_purge: DATABASE_CHANNEL not set — skipping archive")
        return False

    try:
        db_channel = int(db_channel)
        print(f"[ARCHIVE] db_channel converted to int: {db_channel}", flush=True)
    except (ValueError, TypeError):
        pass

    from utils.log_utils import get_main_bot
    send_bot = get_main_bot()
    if send_bot is None:
        send_bot = bot
        print(f"[ARCHIVE] WARNING: get_main_bot() is None — using fallback bot.", flush=True)
    else:
        print(f"[ARCHIVE] Using main bot (token prefix: {send_bot.token[:10]}...)", flush=True)

    from utils.db import get_db, is_mongo, get_sqlite_path

    # ── 1. Fetch full data (with timeout) ────────────────────
    print(f"[ARCHIVE] Fetching giveaway data, is_mongo={is_mongo()}", flush=True)
    giveaway = None
    all_votes = []

    try:
        if is_mongo():
            db = get_db()
            print(f"[ARCHIVE] Running find_one...", flush=True)
            giveaway = await asyncio.wait_for(
                db.giveaways.find_one({"giveaway_id": giveaway_id}),
                timeout=_TIMEOUT
            )
            print(f"[ARCHIVE] giveaway found: {giveaway is not None}", flush=True)
            if giveaway:
                giveaway.pop("_id", None)
                print(f"[ARCHIVE] Fetching votes...", flush=True)
                raw_votes = await asyncio.wait_for(
                    db.votes.find({"giveaway_id": giveaway_id}).to_list(None),
                    timeout=_TIMEOUT
                )
                for v in raw_votes:
                    v.pop("_id", None)
                    all_votes.append(v)
                print(f"[ARCHIVE] votes fetched: {len(all_votes)}", flush=True)
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
    except asyncio.TimeoutError:
        print(f"[ARCHIVE] TIMEOUT fetching giveaway data from DB after {_TIMEOUT}s!", flush=True)
        logger.error("archive_and_purge: DB fetch timed out")
        raise RuntimeError(f"MongoDB timed out after {_TIMEOUT}s — check your MONGO_URI connection")

    if not giveaway:
        print(f"[ARCHIVE] ERROR: giveaway {giveaway_id} not found in DB!", flush=True)
        logger.warning(f"archive_and_purge: giveaway {giveaway_id} not found")
        return False

    # ── 2. Build archive payload (enriched with panel/member stats) ─
    print(f"[ARCHIVE] Building JSON payload...", flush=True)

    # Fetch panel data for this giveaway (member stats, snapshots)
    member_stats = {}
    try:
        from utils.db import get_db as _gdb, is_mongo as _im, get_sqlite_path as _gsp
        if _im():
            panel_doc = await _gdb().panels.find_one({"ref_id": giveaway_id, "panel_type": "giveaway"})
        else:
            import aiosqlite as _aio
            async with _aio.connect(_gsp()) as _conn:
                _conn.row_factory = _aio.Row
                async with _conn.execute(
                    "SELECT * FROM panels WHERE ref_id=? AND panel_type='giveaway' LIMIT 1", (giveaway_id,)
                ) as _cur:
                    _row = await _cur.fetchone()
                panel_doc = dict(_row) if _row else None

        if panel_doc:
            snaps_raw = panel_doc.get("member_snapshots", [])
            if isinstance(snaps_raw, str):
                try:
                    snaps_raw = json.loads(snaps_raw) if snaps_raw else []
                except Exception:
                    snaps_raw = []
            member_start   = panel_doc.get("member_start", 0)
            member_final   = snaps_raw[-1]["c"] if snaps_raw else member_start
            member_gained  = member_final - member_start
            member_stats = {
                "channel_title":    panel_doc.get("channel_title", ""),
                "channel_username": panel_doc.get("channel_username", ""),
                "member_start":     member_start,
                "member_final":     member_final,
                "member_gained":    member_gained,
                "snapshots":        snaps_raw,
                "panel_token":      panel_doc.get("token", ""),
            }
    except Exception as _e:
        logger.warning(f"archive_and_purge: could not fetch panel stats: {_e}")

    # Build final votes summary
    raw_votes_dict = giveaway.get("votes", {}) or {}
    if isinstance(raw_votes_dict, str):
        try:
            raw_votes_dict = json.loads(raw_votes_dict)
        except Exception:
            raw_votes_dict = {}
    options = giveaway.get("options", [])
    votes_summary = [
        {"option": options[int(k)] if int(k) < len(options) else k,
         "votes": v,
         "pct": round(v / max(giveaway.get("total_votes", 1), 1) * 100, 1)}
        for k, v in raw_votes_dict.items()
    ]
    votes_summary.sort(key=lambda x: x["votes"], reverse=True)

    archive = {
        "archived_at":    datetime.now(timezone.utc).isoformat(),
        "giveaway":       giveaway,
        "votes":          all_votes,
        "votes_summary":  votes_summary,
        "member_stats":   member_stats,
        "summary": {
            "giveaway_id":   giveaway_id,
            "title":         giveaway.get("title", ""),
            "total_votes":   giveaway.get("total_votes", 0),
            "options_count": len(options),
            "created_at":    str(giveaway.get("created_at", ""))[:19],
            "closed_at":     datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "channel":       member_stats.get("channel_username", ""),
            "member_start":  member_stats.get("member_start", 0),
            "member_final":  member_stats.get("member_final", 0),
            "member_gained": member_stats.get("member_gained", 0),
            "winner":        votes_summary[0]["option"] if votes_summary else "N/A",
        },
    }

    def _default(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        try:
            from bson import ObjectId
            if isinstance(obj, ObjectId):
                return str(obj)
        except ImportError:
            pass
        return str(obj)

    try:
        json_bytes = json.dumps(archive, default=_default, ensure_ascii=False, indent=2).encode()
        print(f"[ARCHIVE] JSON serialized OK, size={len(json_bytes)} bytes", flush=True)
    except Exception as e:
        print(f"[ARCHIVE] JSON serialization FAILED: {e}", flush=True)
        raise

    file_obj = BufferedInputFile(json_bytes, filename=f"giveaway_{giveaway_id}.json")

    title       = giveaway.get("title", giveaway_id)
    total_v     = giveaway.get("total_votes", 0)
    creator_id  = giveaway.get("creator_id", 0)
    created_at  = str(giveaway.get("created_at", ""))[:19]
    end_date    = str(giveaway.get("end_time", ""))[:19] or str(datetime.now(timezone.utc))[:19]
    archived_at = datetime.now(timezone.utc).isoformat()
    winner_line = f"\n🏆 <b>Winner:</b> {votes_summary[0]['option']} ({votes_summary[0]['votes']} votes)" if votes_summary else ""
    m_start  = member_stats.get("member_start", 0)
    m_final  = member_stats.get("member_final", 0)
    m_gained = member_stats.get("member_gained", 0)
    ch_name  = member_stats.get("channel_username", "—")
    gain_str = f"+{m_gained}" if m_gained >= 0 else str(m_gained)

    caption = (
        f"📦 <b>Giveaway Archived</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 <b>ID:</b> <code>{giveaway_id}</code>\n"
        f"🏷 <b>Title:</b> {title}\n"
        f"📢 <b>Channel:</b> {ch_name}\n"
        f"👥 <b>Total Votes:</b> {total_v}{winner_line}\n"
        f"📈 <b>Members:</b> {m_start} → {m_final} ({gain_str} gained)\n"
        f"📅 <b>Created:</b> {created_at} UTC\n"
        f"⏰ <b>Ended:</b> {end_date} UTC\n"
        f"📦 <b>Archived:</b> {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n\n"
        f"⬇️ Download this file to restore or view full data.\n"
        f"Use <code>/getgiveaway {giveaway_id}</code> in bot to retrieve it."
    )

    # ── 3. Send to DATABASE_CHANNEL ───────────────────────────
    print(f"[ARCHIVE] Sending document to db_channel={db_channel}", flush=True)
    try:
        sent_msg = await asyncio.wait_for(
            send_bot.send_document(
                db_channel,
                document=file_obj,
                caption=caption,
                parse_mode="HTML",
            ),
            timeout=_TIMEOUT
        )
        file_id = sent_msg.document.file_id
        print(f"[ARCHIVE] Document sent OK, file_id={file_id}", flush=True)
        logger.info(f"✅ Giveaway {giveaway_id} archived to DATABASE_CHANNEL")
    except asyncio.TimeoutError:
        print(f"[ARCHIVE] TIMEOUT sending document after {_TIMEOUT}s!", flush=True)
        raise RuntimeError(f"Telegram send_document timed out after {_TIMEOUT}s")
    except Exception as e:
        print(f"[ARCHIVE] send_document FAILED: {type(e).__name__}: {e}", flush=True)
        logger.error(f"archive_and_purge: failed to send to DATABASE_CHANNEL {db_channel!r}: {e}")
        raise

    # ── 3b. Store metadata ────────────────────────────────────
    try:
        if is_mongo():
            await asyncio.wait_for(
                get_db().giveaway_archive_refs.update_one(
                    {"giveaway_id": giveaway_id},
                    {"$set": {
                        "giveaway_id":   giveaway_id,
                        "file_id":       file_id,
                        "title":         title,
                        "creator_id":    creator_id,
                        "created_at":    created_at,
                        "end_date":      end_date,
                        "total_votes":   total_v,
                        "archived_at":   archived_at,
                        "channel":       member_stats.get("channel_username",""),
                        "member_start":  member_stats.get("member_start", 0),
                        "member_final":  member_stats.get("member_final", 0),
                        "member_gained": member_stats.get("member_gained", 0),
                        "winner":        votes_summary[0]["option"] if votes_summary else "",
                    }},
                    upsert=True,
                ),
                timeout=_TIMEOUT
            )
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                await conn.execute(
                    """CREATE TABLE IF NOT EXISTS giveaway_archive_refs (
                        giveaway_id  TEXT PRIMARY KEY,
                        file_id      TEXT,
                        title        TEXT,
                        creator_id   INTEGER,
                        created_at   TEXT,
                        end_date     TEXT,
                        total_votes  INTEGER DEFAULT 0,
                        archived_at  TEXT,
                        channel      TEXT,
                        member_start INTEGER DEFAULT 0,
                        member_final INTEGER DEFAULT 0,
                        member_gained INTEGER DEFAULT 0,
                        winner       TEXT
                    )"""
                )
                await conn.execute(
                    """INSERT OR REPLACE INTO giveaway_archive_refs
                       (giveaway_id, file_id, title, creator_id, created_at, end_date,
                        total_votes, archived_at, channel, member_start, member_final,
                        member_gained, winner)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (giveaway_id, file_id, title, creator_id,
                     created_at, end_date, total_v, archived_at,
                     member_stats.get("channel_username",""),
                     member_stats.get("member_start", 0),
                     member_stats.get("member_final", 0),
                     member_stats.get("member_gained", 0),
                     votes_summary[0]["option"] if votes_summary else "")
                )
                await conn.commit()
    except asyncio.TimeoutError:
        print(f"[ARCHIVE] TIMEOUT storing metadata — continuing to purge anyway", flush=True)
    except Exception as e:
        logger.warning(f"archive_and_purge: failed to store metadata: {e}")

    # ── 4. Purge from live database (keep panels so links stay valid) ──
    print(f"[ARCHIVE] Purging giveaway from live DB...", flush=True)
    try:
        if is_mongo():
            db = get_db()
            await asyncio.wait_for(
                db.giveaways.delete_one({"giveaway_id": giveaway_id}),
                timeout=_TIMEOUT
            )
            await asyncio.wait_for(
                db.votes.delete_many({"giveaway_id": giveaway_id}),
                timeout=_TIMEOUT
            )
            # NOTE: panels are NOT deleted — kept so user panel links remain valid
            # Panel will serve archived data from Telegram DB channel
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                await conn.execute("DELETE FROM giveaways WHERE giveaway_id=?", (giveaway_id,))
                await conn.execute("DELETE FROM votes WHERE giveaway_id=?", (giveaway_id,))
                # NOTE: panels are NOT deleted — kept so user panel links remain valid
                await conn.commit()
        print(f"[ARCHIVE] Purge complete (panels preserved).", flush=True)
        logger.info(f"🗑 Giveaway {giveaway_id} purged from live DB after archiving (panels kept)")
    except asyncio.TimeoutError:
        print(f"[ARCHIVE] TIMEOUT during purge — data was archived but not deleted from DB", flush=True)
        logger.error("archive_and_purge: purge timed out")
    except Exception as e:
        logger.error(f"archive_and_purge: purge failed: {e}")

    return True


async def get_old_giveaways(limit: int = 200) -> list[dict]:
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
