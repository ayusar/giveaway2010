"""
Web server — FastAPI
Admin panel: /adminpanel/royalisbest/a?b3c
User panel:  /panel/<token>
"""
import time, secrets, hashlib, json, logging, asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

logger = logging.getLogger(__name__)

# ── Bot task holder ───────────────────────────────────────────
_bot_task: asyncio.Task = None


async def _start_bot():
    """Start the Telegram bot. Called from lifespan — works even when Render
    launches web.app:app directly instead of main.py."""
    try:
        import utils.clone_manager as clone_manager_module
        from aiogram import Bot, Dispatcher
        from aiogram.fsm.storage.memory import MemoryStorage
        from config.settings import settings
        from utils.db import init_db
        from utils.clone_manager import get_clone_manager
        from web.broadcaster import set_main_bot
        from handlers import start, giveaway, referral, admin, clone_bot

        logger.info("🔄 Initialising database...")
        await init_db()
        logger.info("✅ Database ready")

        storage = MemoryStorage()
        bot = Bot(token=settings.BOT_TOKEN)
        dp = Dispatcher(storage=storage)

        me = await bot.get_me()
        clone_manager_module.MAIN_BOT_USERNAME = me.username
        logger.info(f"🤖 Bot: @{me.username}")

        set_main_bot(bot)

        # Register main bot for log_utils
        from utils.log_utils import set_main_bot as set_log_bot
        set_log_bot(bot)

        # ── Ban middleware ────────────────────────────────────
        from utils.ban_middleware import BanMiddleware
        dp.message.middleware(BanMiddleware())
        dp.callback_query.middleware(BanMiddleware())

        dp.include_router(start.router)
        dp.include_router(giveaway.router)
        dp.include_router(referral.router)
        dp.include_router(admin.router)
        dp.include_router(clone_bot.router)

        from handlers import stats as stats_handler
        dp.include_router(stats_handler.router)

        clone_manager = get_clone_manager()
        asyncio.create_task(clone_manager.start_all_clones())

        from utils.snapshot_scheduler import set_bot as set_snap_bot, snapshot_loop
        set_snap_bot(bot)
        asyncio.create_task(snapshot_loop())

        from utils.keep_alive import set_domain, keep_alive_loop
        set_domain(settings.WEB_DOMAIN)
        asyncio.create_task(keep_alive_loop())

        logger.info("🚀 Bot polling started!")
        await dp.start_polling(bot, allowed_updates=["message", "callback_query", "chat_member"])

    except Exception as e:
        logger.error(f"❌ Bot startup failed: {e}", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Starts bot on FastAPI startup regardless of how uvicorn is invoked."""
    global _bot_task
    _bot_task = asyncio.create_task(_start_bot())
    yield
    if _bot_task and not _bot_task.done():
        _bot_task.cancel()


app = FastAPI(docs_url=None, redoc_url=None, lifespan=lifespan)

_sessions: dict = {}
_start_time = time.time()
PANEL_SECRET = "royalisbest"
PANEL_PARAM  = "b3c"


def _hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()
def _get_token(request): return request.cookies.get("panel_session")
def _is_auth(token):
    if not token or token not in _sessions: return False
    if _sessions[token] < time.time():
        del _sessions[token]; return False
    return True

# ══════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════

async def _check_creds(username, password):
    try:
        from utils.db import get_db, is_mongo, get_sqlite_path
        hashed = _hash_pw(password)
        if is_mongo():
            db = get_db()
            if db is None:
                import logging
                logging.getLogger(__name__).error("MongoDB not ready yet")
                return False
            return bool(await db.panel_users.find_one({"username": username, "password": hashed}))
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            # Ensure table exists (first boot safety)
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS panel_users "
                "(id INTEGER PRIMARY KEY, username TEXT UNIQUE, password TEXT)"
            )
            await conn.commit()
            async with conn.execute(
                "SELECT id FROM panel_users WHERE username=? AND password=?", (username, hashed)
            ) as cur:
                return await cur.fetchone() is not None
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"_check_creds error: {e}")
        return False


@app.get(f"/adminpanel/{PANEL_SECRET}/login", response_class=HTMLResponse)
async def login_page(error: str = ""):
    return HTMLResponse(_login_html(error))


@app.post(f"/adminpanel/{PANEL_SECRET}/login")
async def login_post(request: Request):
    try:
        form = await request.form()
        if await _check_creds(form.get("username",""), form.get("password","")):
            tok = secrets.token_hex(32)
            _sessions[tok] = time.time() + 28800
            r = RedirectResponse(url=f"/adminpanel/{PANEL_SECRET}/a?{PANEL_PARAM}", status_code=302)
            r.set_cookie("panel_session", tok, httponly=True, max_age=28800)
            return r
        return HTMLResponse(_login_html("❌ Invalid username or password"))
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"login_post error: {e}")
        return HTMLResponse(_login_html("❌ Server error, please try again"))


@app.get(f"/adminpanel/{PANEL_SECRET}/logout")
async def logout(request: Request):
    tok = _get_token(request)
    if tok and tok in _sessions: del _sessions[tok]
    r = RedirectResponse(url=f"/adminpanel/{PANEL_SECRET}/login", status_code=302)
    r.delete_cookie("panel_session"); return r


# ══════════════════════════════════════════════════════════════
# ADMIN PANEL
# ══════════════════════════════════════════════════════════════

@app.get(f"/adminpanel/{PANEL_SECRET}/a", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    if not _is_auth(_get_token(request)):
        return RedirectResponse(url=f"/adminpanel/{PANEL_SECRET}/login", status_code=302)
    return HTMLResponse(_admin_html())


@app.get(f"/adminpanel/{PANEL_SECRET}/api/stats")
async def api_stats(request: Request):
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    return JSONResponse(await _build_stats())


@app.get(f"/adminpanel/{PANEL_SECRET}/api/clones")
async def api_clones(request: Request):
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    return JSONResponse(await _build_clones())


@app.get(f"/adminpanel/{PANEL_SECRET}/api/clone_users/{{token}}")
async def api_clone_users(token: str, request: Request):
    """
    Live fetch of all users who started a specific clone bot,
    queried directly from the referrals table by clone_token.
    Returns: user_id, user_name, refer_count, joined_at — sorted by refer_count desc.
    """
    if not _is_auth(_get_token(request)):
        raise HTTPException(401)

    from utils.db import get_db, is_mongo, get_sqlite_path

    users = []
    try:
        if is_mongo():
            db = get_db()
            docs = await db.referrals.find(
                {"clone_token": token}
            ).sort("refer_count", -1).to_list(None)
            users = [
                {
                    "user_id":     d.get("user_id"),
                    "user_name":   d.get("user_name") or "—",
                    "refer_count": d.get("refer_count", 0),
                    "joined_at":   str(d.get("joined_at", ""))[:10],
                }
                for d in docs
            ]
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(
                    "SELECT user_id, user_name, refer_count, joined_at "
                    "FROM referrals WHERE clone_token=? ORDER BY refer_count DESC",
                    (token,)
                ) as cur:
                    rows = await cur.fetchall()
            users = [
                {
                    "user_id":     r["user_id"],
                    "user_name":   r["user_name"] or "—",
                    "refer_count": r["refer_count"] or 0,
                    "joined_at":   str(r["joined_at"] or "")[:10],
                }
                for r in rows
            ]
    except Exception as e:
        logger.error(f"clone_users error: {e}")
        raise HTTPException(500, detail=str(e))

    return JSONResponse({
        "token":  token,
        "users":  users,
        "total":  len(users),
    })


@app.get(f"/adminpanel/{PANEL_SECRET}/api/giveaways")
async def api_giveaways(request: Request):
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    return JSONResponse(await _build_giveaways())


@app.get(f"/adminpanel/{PANEL_SECRET}/api/old_giveaways")
async def api_old_giveaways(request: Request):
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    from utils.giveaway_archive import get_old_giveaways
    data = await get_old_giveaways(limit=200)
    return JSONResponse(data)


@app.get(f"/adminpanel/{PANEL_SECRET}/api/old_giveaway_file/{{giveaway_id}}")
async def api_old_giveaway_file(giveaway_id: str, request: Request):
    """Return the Telegram file_id for a specific archived giveaway so the frontend can trigger a bot send."""
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    from utils.db import get_db, is_mongo, get_sqlite_path
    try:
        if is_mongo():
            doc = await get_db().giveaway_archive_refs.find_one({"giveaway_id": giveaway_id.upper()})
            if doc:
                doc.pop("_id", None)
                return JSONResponse(doc)
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(
                    "SELECT * FROM giveaway_archive_refs WHERE giveaway_id=?",
                    (giveaway_id.upper(),)
                ) as cur:
                    row = await cur.fetchone()
                if row:
                    return JSONResponse(dict(row))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    raise HTTPException(404, detail="Not found")


@app.delete(f"/adminpanel/{PANEL_SECRET}/api/old_giveaways")
async def api_deleteold(request: Request):
    """Delete archived giveaway metadata older than N months."""
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    body = await request.json()
    months = int(body.get("months", 0))
    if months < 1:
        return JSONResponse({"ok": False, "error": "months must be >= 1"}, status_code=400)
    from utils.giveaway_archive import delete_old_giveaways_before
    deleted = await delete_old_giveaways_before(months)
    return JSONResponse({"ok": True, "deleted": deleted})


@app.get(f"/adminpanel/{PANEL_SECRET}/api/panels")
async def api_panels(request: Request):
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    return JSONResponse(await _build_panels())


@app.get(f"/adminpanel/{PANEL_SECRET}/api/giveaway_participants/{{giveaway_id}}")
async def api_giveaway_participants(giveaway_id: str, request: Request):
    """
    Live fetch: returns every user who voted in this giveaway, plus their
    referral count from whichever clone bot they used (matched by user_id).
    No user_ids are cached — this queries the votes + referrals tables fresh.
    """
    if not _is_auth(_get_token(request)):
        raise HTTPException(401)

    from utils.db import get_db, is_mongo, get_sqlite_path

    participants = []

    try:
        if is_mongo():
            db = get_db()
            votes = await db.votes.find({"giveaway_id": giveaway_id}).to_list(None)
            for v in votes:
                uid = v.get("user_id")
                uname = v.get("user_name") or "Unknown"
                ref_doc = await db.referrals.find_one({"user_id": uid})
                refer_count = ref_doc.get("refer_count", 0) if ref_doc else 0
                participants.append({
                    "user_id":     uid,
                    "user_name":   uname,
                    "option":      v.get("option_index", 0),
                    "voted_at":    str(v.get("voted_at", ""))[:16],
                    "refer_count": refer_count,
                })
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(
                    "SELECT user_id, user_name, option_index, voted_at FROM votes "
                    "WHERE giveaway_id=? ORDER BY voted_at DESC",
                    (giveaway_id,)
                ) as cur:
                    vote_rows = await cur.fetchall()

                for vr in vote_rows:
                    uid = vr["user_id"]
                    async with conn.execute(
                        "SELECT refer_count FROM referrals WHERE user_id=? "
                        "ORDER BY refer_count DESC LIMIT 1",
                        (uid,)
                    ) as rc:
                        ref_row = await rc.fetchone()
                    refer_count = ref_row[0] if ref_row else 0
                    participants.append({
                        "user_id":     uid,
                        "user_name":   vr["user_name"] or "Unknown",
                        "option":      vr["option_index"],
                        "voted_at":    str(vr["voted_at"] or "")[:16],
                        "refer_count": refer_count,
                    })
    except Exception as e:
        logger.error(f"giveaway_participants error: {e}")
        raise HTTPException(500, detail=str(e))

    participants.sort(key=lambda x: x["refer_count"], reverse=True)
    return JSONResponse({
        "giveaway_id": giveaway_id,
        "participants": participants,
        "total": len(participants),
    })


@app.post(f"/adminpanel/{PANEL_SECRET}/api/ban_clone")
async def api_ban_clone(request: Request):
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    data = await request.json()
    from models.referral import ban_clone_bot
    from utils.clone_manager import get_clone_manager
    await get_clone_manager().stop_clone(data.get("token",""))
    await ban_clone_bot(data.get("token",""))
    return JSONResponse({"ok": True})


@app.post(f"/adminpanel/{PANEL_SECRET}/api/broadcast")
async def api_broadcast(request: Request):
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    data = await request.json()
    msg = data.get("message","")
    if not msg: return JSONResponse({"ok": False, "error": "Empty"})
    import asyncio
    from web.broadcaster import do_global_broadcast
    asyncio.create_task(do_global_broadcast(msg))
    return JSONResponse({"ok": True})


@app.post(f"/adminpanel/{PANEL_SECRET}/api/delete_panel")
async def api_delete_panel(request: Request):
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    data = await request.json()
    from models.panel import soft_delete_panel
    await soft_delete_panel(data.get("token",""))
    return JSONResponse({"ok": True})


# ══════════════════════════════════════════════════════════════
# USER PANEL (public)
# ══════════════════════════════════════════════════════════════

@app.get("/panel/{token}", response_class=HTMLResponse)
async def user_panel(token: str):
    from models.panel import get_panel
    panel = await get_panel(token)
    if not panel:
        return HTMLResponse(_not_found_html(), status_code=404)
    data = await _build_panel_data(panel)
    return HTMLResponse(_user_panel_html(panel, data))


@app.get("/panel/{token}/api/data")
async def user_panel_data(token: str):
    from models.panel import get_panel
    panel = await get_panel(token)
    if not panel: raise HTTPException(404)
    return JSONResponse(await _build_panel_data(panel))


@app.post("/panel/{token}/delete")
async def user_panel_delete(token: str, request: Request):
    """Anyone can delete — we trust the link is private."""
    from models.panel import get_panel, soft_delete_panel
    panel = await get_panel(token)
    if not panel: raise HTTPException(404)
    await soft_delete_panel(token)
    return JSONResponse({"ok": True})


# ══════════════════════════════════════════════════════════════
# DATA BUILDERS
# ══════════════════════════════════════════════════════════════

async def _build_stats():
    from utils.db import get_db, is_mongo, get_sqlite_path
    uptime_s = int(time.time() - _start_time)
    h,r = divmod(uptime_s,3600); m,s = divmod(r,60)
    uptime = f"{h}h {m}m {s}s"

    if is_mongo():
        db = get_db()
        total_clones    = await db.clone_bots.count_documents({"is_active": True})
        total_users     = await db.referrals.count_documents({})
        total_giveaways = await db.giveaways.count_documents({})
        active_polls    = await db.giveaways.count_documents({"is_active": True})
        total_votes     = 0
        async for g in db.giveaways.find({}, {"total_votes": 1}):
            total_votes += g.get("total_votes", 0)
        banned_clones   = await db.clone_bots.count_documents({"is_banned": True})
        total_panels    = await db.panels.count_documents({"is_deleted": False})
        # Daily joins 7 days
        since = datetime.utcnow() - timedelta(days=7)
        pipeline = [
            {"$match": {"joined_at": {"$gte": since}}},
            {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$joined_at"}},
                        "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        daily_raw = await db.referrals.aggregate(pipeline).to_list(length=None)
        # Bot usage by day (giveaways created)
        pipeline2 = [
            {"$match": {"created_at": {"$gte": since}}},
            {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}},
                        "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        usage_raw = await db.giveaways.aggregate(pipeline2).to_list(length=None)
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            async def cnt(q, p=()):
                async with conn.execute(q,p) as c: return (await c.fetchone())[0]
            total_clones    = await cnt("SELECT COUNT(*) FROM clone_bots WHERE is_active=1")
            total_users     = await cnt("SELECT COUNT(*) FROM referrals")
            total_giveaways = await cnt("SELECT COUNT(*) FROM giveaways")
            active_polls    = await cnt("SELECT COUNT(*) FROM giveaways WHERE is_active=1")
            total_votes_row = await cnt("SELECT COALESCE(SUM(total_votes),0) FROM giveaways")
            total_votes     = total_votes_row
            banned_clones   = await cnt("SELECT COUNT(*) FROM clone_bots WHERE is_banned=1")
            try:
                total_panels = await cnt("SELECT COUNT(*) FROM panels WHERE is_deleted=0")
            except Exception:
                total_panels = 0
            since_str = (datetime.utcnow()-timedelta(days=7)).isoformat()
            async with conn.execute(
                "SELECT substr(joined_at,1,10) d, COUNT(*) c FROM referrals "
                "WHERE joined_at>=? GROUP BY d ORDER BY d", (since_str,)
            ) as cur:
                daily_raw = [{"_id":r[0],"count":r[1]} for r in await cur.fetchall()]
            async with conn.execute(
                "SELECT substr(created_at,1,10) d, COUNT(*) c FROM giveaways "
                "WHERE created_at>=? GROUP BY d ORDER BY d", (since_str,)
            ) as cur:
                usage_raw = [{"_id":r[0],"count":r[1]} for r in await cur.fetchall()]

    daily_labels, daily_counts, usage_counts = [], [], []
    day_map = {d["_id"]:d["count"] for d in daily_raw}
    usage_map = {d["_id"]:d["count"] for d in usage_raw}
    for i in range(6,-1,-1):
        day = (datetime.utcnow()-timedelta(days=i)).strftime("%Y-%m-%d")
        daily_labels.append(day[5:])
        daily_counts.append(day_map.get(day,0))
        usage_counts.append(usage_map.get(day,0))

    return {
        "uptime": uptime, "total_clones": total_clones, "total_users": total_users,
        "total_giveaways": total_giveaways, "active_polls": active_polls,
        "total_votes": total_votes, "banned_clones": banned_clones,
        "total_panels": total_panels,
        "daily_labels": daily_labels, "daily_counts": daily_counts,
        "usage_counts": usage_counts
    }


async def _build_clones():
    from utils.db import get_db, is_mongo, get_sqlite_path
    if is_mongo():
        db = get_db()
        clones = await db.clone_bots.find({}).sort("created_at",-1).limit(100).to_list(None)
        result = []
        for c in clones:
            uc = await db.referrals.count_documents({"clone_token": c["token"]})
            result.append({
                "bot_username": c.get("bot_username","?"),
                "owner_id": c.get("owner_id"),
                "is_active": c.get("is_active",False),
                "is_banned": c.get("is_banned",False),
                "user_count": uc,
                "channel": c.get("channel_link","—"),
                "token": c["token"],
                "token_preview": c["token"][:12]+"...",
                "created_at": str(c.get("created_at",""))[:10],
            })
        return result
    import aiosqlite
    async with aiosqlite.connect(get_sqlite_path()) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM clone_bots ORDER BY created_at DESC LIMIT 100") as cur:
            rows = await cur.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            async with conn.execute(
                "SELECT COUNT(*) FROM referrals WHERE clone_token=?", (d["token"],)
            ) as c2:
                uc = (await c2.fetchone())[0]
            result.append({
                "bot_username": d.get("bot_username","?"),
                "owner_id": d.get("owner_id"),
                "is_active": bool(d.get("is_active")),
                "is_banned": bool(d.get("is_banned")),
                "user_count": uc,
                "channel": d.get("channel_link","—"),
                "token": d["token"],
                "token_preview": d["token"][:12]+"...",
                "created_at": str(d.get("created_at",""))[:10],
            })
    return result


async def _build_giveaways():
    from utils.db import get_db, is_mongo, get_sqlite_path
    import json as _json
    if is_mongo():
        giveaways = await get_db().giveaways.find({}).sort("created_at",-1).limit(100).to_list(None)
        return [{"giveaway_id":g["giveaway_id"],"title":g["title"],
                 "channel_id":g.get("channel_id",""),"total_votes":g.get("total_votes",0),
                 "is_active":g.get("is_active",False),"created_at":str(g.get("created_at",""))[:10]} for g in giveaways]
    import aiosqlite
    async with aiosqlite.connect(get_sqlite_path()) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM giveaways ORDER BY created_at DESC LIMIT 100") as cur:
            rows = await cur.fetchall()
    return [{"giveaway_id":d["giveaway_id"],"title":d["title"],
             "channel_id":d.get("channel_id",""),"total_votes":d["total_votes"],
             "is_active":bool(d["is_active"]),"created_at":str(d.get("created_at",""))[:10]}
            for d in [dict(r) for r in rows]]


async def _build_panels():
    from utils.db import get_db, is_mongo, get_sqlite_path
    if is_mongo():
        panels = await get_db().panels.find({"is_deleted":False}).sort("created_at",-1).limit(100).to_list(None)
        return [{"token":p["token"],"panel_type":p["panel_type"],"channel_title":p.get("channel_title",""),
                 "channel_username":p.get("channel_username",""),"created_at":str(p.get("created_at",""))[:10]} for p in panels]
    import aiosqlite
    async with aiosqlite.connect(get_sqlite_path()) as conn:
        conn.row_factory = aiosqlite.Row
        try:
            async with conn.execute("SELECT * FROM panels WHERE is_deleted=0 ORDER BY created_at DESC LIMIT 100") as cur:
                rows = await cur.fetchall()
            return [{"token":d["token"],"panel_type":d["panel_type"],"channel_title":d.get("channel_title",""),
                     "channel_username":d.get("channel_username",""),"created_at":str(d.get("created_at",""))[:10]}
                    for d in [dict(r) for r in rows]]
        except Exception:
            return []



async def _build_users(page: int = 1, search: str = "") -> dict:
    """Paginated user list from main_bot_users."""
    from utils.db import get_db, is_mongo, get_sqlite_path
    per_page = 50
    offset   = (page - 1) * per_page

    if is_mongo():
        db    = get_db()
        query = {}
        if search:
            query = {"$or": [
                {"first_name": {"$regex": search, "$options": "i"}},
                {"username":   {"$regex": search, "$options": "i"}},
                {"user_id":    int(search) if search.isdigit() else -1},
            ]}
        total = await db.main_bot_users.count_documents(query)
        users = await db.main_bot_users.find(query).sort("joined_at", -1).skip(offset).limit(per_page).to_list(None)
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            conn.row_factory = aiosqlite.Row
            if search:
                like = f"%{search}%"
                q_count = "SELECT COUNT(*) FROM main_bot_users WHERE first_name LIKE ? OR username LIKE ? OR CAST(user_id AS TEXT) LIKE ?"
                q_rows  = "SELECT * FROM main_bot_users WHERE first_name LIKE ? OR username LIKE ? OR CAST(user_id AS TEXT) LIKE ? ORDER BY joined_at DESC LIMIT ? OFFSET ?"
                async with conn.execute(q_count, (like, like, like)) as c:
                    total = (await c.fetchone())[0]
                async with conn.execute(q_rows, (like, like, like, per_page, offset)) as c:
                    rows = await c.fetchall()
            else:
                async with conn.execute("SELECT COUNT(*) FROM main_bot_users") as c:
                    total = (await c.fetchone())[0]
                async with conn.execute(
                    "SELECT * FROM main_bot_users ORDER BY joined_at DESC LIMIT ? OFFSET ?",
                    (per_page, offset)
                ) as c:
                    rows = await c.fetchall()
            users = [dict(r) for r in rows]

    return {
        "users":    [
            {
                "user_id":    u["user_id"],
                "first_name": u.get("first_name") or "",
                "last_name":  u.get("last_name")  or "",
                "username":   u.get("username")   or "",
                "is_banned":  bool(u.get("is_banned", False)),
                "joined_at":  str(u.get("joined_at", ""))[:10],
            }
            for u in users
        ],
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "pages":    max(1, (total + per_page - 1) // per_page),
    }

async def _build_panel_data(panel: dict) -> dict:
    """Build full data for the user panel page."""
    from utils.db import get_db, is_mongo, get_sqlite_path
    import json as _json
    panel_type = panel.get("panel_type")
    ref_id = panel.get("ref_id")
    votes_data, options, prizes, total_votes = [], [], [], 0
    refer_data, top_referrers = [], []
    total_refs = 0

    if panel_type == "giveaway":
        if is_mongo():
            g = await get_db().giveaways.find_one({"giveaway_id": ref_id})
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute("SELECT * FROM giveaways WHERE giveaway_id=?", (ref_id,)) as cur:
                    row = await cur.fetchone()
            g = None
            if row:
                g = dict(row)
                g["prizes"] = _json.loads(g["prizes"])
                g["options"] = _json.loads(g["options"])
                g["votes"] = _json.loads(g["votes"])
        if g:
            options = g["options"]
            prizes = g["prizes"]
            total_votes = g.get("total_votes", 0)
            raw_votes = {int(k): v for k, v in g.get("votes", {}).items()}
            votes_data = [{"name": options[i], "votes": raw_votes.get(i, 0)} for i in range(len(options))]
            votes_data.sort(key=lambda x: x["votes"], reverse=True)

    elif panel_type == "refer":
        if is_mongo():
            users = await get_db().referrals.find({"clone_token": ref_id}).sort("refer_count", -1).to_list(None)
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(
                    "SELECT * FROM referrals WHERE clone_token=? ORDER BY refer_count DESC", (ref_id,)
                ) as cur:
                    rows = await cur.fetchall()
            users = [dict(r) for r in rows]
        total_refs = len(users)
        top_referrers = [{"name": u["user_name"], "refs": u.get("refer_count", 0)} for u in users[:20]]
        refer_data = [{"name": u["user_name"], "refs": u.get("refer_count", 0),
                       "joined": str(u.get("joined_at",""))[:10]} for u in users[:50]]

    # Channel growth snapshots
    snapshots = panel.get("member_snapshots", [])
    member_start = panel.get("member_start", 0)
    member_current = snapshots[-1]["c"] if snapshots else member_start
    member_gain = member_current - member_start

    snap_labels = [s["t"][5:16].replace("T"," ") for s in snapshots[-24:]]
    snap_counts = [s["c"] for s in snapshots[-24:]]

    return {
        "panel_type": panel_type,
        "channel_title": panel.get("channel_title",""),
        "channel_username": panel.get("channel_username",""),
        "member_start": member_start,
        "member_current": member_current,
        "member_gain": member_gain,
        "snap_labels": snap_labels,
        "snap_counts": snap_counts,
        "votes_data": votes_data,
        "prizes": prizes,
        "total_votes": total_votes,
        "top_referrers": top_referrers,
        "refer_data": refer_data,
        "total_refs": total_refs,
        "created_at": panel.get("created_at","")[:10],
    }




@app.get(f"/adminpanel/{PANEL_SECRET}/api/users")
async def api_users(request: Request, page: int = 1, search: str = ""):
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    return JSONResponse(await _build_users(page=page, search=search))


@app.get(f"/adminpanel/{PANEL_SECRET}/api/users/export")
async def api_users_export(request: Request):
    """Export all main_bot_users as a CSV download."""
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    from fastapi.responses import StreamingResponse
    import csv, io
    from utils.db import get_db, is_mongo, get_sqlite_path

    rows = []
    if is_mongo():
        db = get_db()
        async for u in db.main_bot_users.find({}).sort("joined_at", -1):
            rows.append({
                "user_id":    u.get("user_id", ""),
                "first_name": u.get("first_name", ""),
                "last_name":  u.get("last_name", ""),
                "username":   u.get("username", ""),
                "is_banned":  1 if u.get("is_banned") else 0,
                "joined_at":  str(u.get("joined_at", ""))[:19],
            })
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT user_id,first_name,last_name,username,is_banned,joined_at "
                "FROM main_bot_users ORDER BY joined_at DESC"
            ) as cur:
                for r in await cur.fetchall():
                    rows.append(dict(r))

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["user_id","first_name","last_name","username","is_banned","joined_at"])
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)

    from datetime import datetime
    fname = f"users_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get(f"/adminpanel/{PANEL_SECRET}/api/live_users")
async def api_live_users(request: Request):
    """Live total user count — polled every few seconds by the dashboard widget."""
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    from utils.db import get_db, is_mongo, get_sqlite_path
    try:
        if is_mongo():
            count = await get_db().main_bot_users.count_documents({})
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                async with conn.execute("SELECT COUNT(*) FROM main_bot_users") as cur:
                    count = (await cur.fetchone())[0]
        return JSONResponse({"count": count})
    except Exception as e:
        return JSONResponse({"count": 0, "error": str(e)})


@app.get(f"/adminpanel/{PANEL_SECRET}/api/bot_stats")
async def api_bot_stats(request: Request):
    """Combined stats panel — mirrors /stats bot command."""
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    from utils.db import get_db, is_mongo, get_sqlite_path
    try:
        if is_mongo():
            db = get_db()
            total_users      = await db.main_bot_users.count_documents({})
            total_giveaways  = await db.giveaways.count_documents({})
            live_giveaways   = await db.giveaways.count_documents({"is_active": True})
            closed_giveaways = await db.giveaways.count_documents({"is_active": False})
            total_votes = 0
            async for g in db.giveaways.find({}, {"total_votes": 1}):
                total_votes += g.get("total_votes", 0)
            total_clones  = await db.clone_bots.count_documents({"is_active": True})
            banned_users  = await db.main_bot_users.count_documents({"is_banned": True})
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                async def cnt(q, p=()):
                    async with conn.execute(q, p) as c:
                        return (await c.fetchone())[0]
                total_users      = await cnt("SELECT COUNT(*) FROM main_bot_users")
                total_giveaways  = await cnt("SELECT COUNT(*) FROM giveaways")
                live_giveaways   = await cnt("SELECT COUNT(*) FROM giveaways WHERE is_active=1")
                closed_giveaways = await cnt("SELECT COUNT(*) FROM giveaways WHERE is_active=0")
                total_votes      = await cnt("SELECT COALESCE(SUM(total_votes),0) FROM giveaways")
                total_clones     = await cnt("SELECT COUNT(*) FROM clone_bots WHERE is_active=1")
                banned_users     = await cnt("SELECT COUNT(*) FROM main_bot_users WHERE is_banned=1")

        return JSONResponse({
            "total_users":      total_users,
            "total_giveaways":  total_giveaways,
            "live_giveaways":   live_giveaways,
            "closed_giveaways": closed_giveaways,
            "total_votes":      total_votes,
            "total_clones":     total_clones,
            "banned_users":     banned_users,
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post(f"/adminpanel/{PANEL_SECRET}/api/ban_user")
async def api_ban_user(request: Request):
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    data = await request.json()
    user_id = int(data.get("user_id", 0))
    if not user_id:
        return JSONResponse({"ok": False, "error": "Missing user_id"})
    from utils.db import get_db, is_mongo, get_sqlite_path
    if is_mongo():
        await get_db().main_bot_users.update_one(
            {"user_id": user_id}, {"$set": {"is_banned": True}}, upsert=True
        )
        await get_db().banned_users.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "banned": True}},
            upsert=True,
        )
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            await conn.execute(
                "UPDATE main_bot_users SET is_banned=1 WHERE user_id=?", (user_id,)
            )
            await conn.execute(
                "INSERT OR REPLACE INTO banned_users (user_id, banned) VALUES (?,1)", (user_id,)
            )
            await conn.commit()
    return JSONResponse({"ok": True})


@app.post(f"/adminpanel/{PANEL_SECRET}/api/unban_user")
async def api_unban_user(request: Request):
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    data = await request.json()
    user_id = int(data.get("user_id", 0))
    if not user_id:
        return JSONResponse({"ok": False, "error": "Missing user_id"})
    from utils.db import get_db, is_mongo, get_sqlite_path
    if is_mongo():
        await get_db().main_bot_users.update_one(
            {"user_id": user_id}, {"$set": {"is_banned": False}}
        )
        await get_db().banned_users.delete_one({"user_id": user_id})
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            await conn.execute(
                "UPDATE main_bot_users SET is_banned=0 WHERE user_id=?", (user_id,)
            )
            await conn.execute(
                "DELETE FROM banned_users WHERE user_id=?", (user_id,)
            )
            await conn.commit()
    return JSONResponse({"ok": True})

@app.get("/health")
async def health():
    """Render health check endpoint."""
    return JSONResponse({"status": "ok", "uptime": int(time.time() - _start_time)})


@app.get("/")
async def root():
    """Root redirect to admin panel."""
    return RedirectResponse(url=f"/adminpanel/{PANEL_SECRET}/login", status_code=302)




# ══════════════════════════════════════════════════════════════
# HTML TEMPLATES
# ══════════════════════════════════════════════════════════════

def _login_html(error=""):
    err = f'<div class="error">{error}</div>' if error else ""
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RoyalityBots Admin</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#07070e;color:#f1f0ff;font-family:'Syne',sans-serif;min-height:100vh;
  display:flex;align-items:center;justify-content:center;
  background-image:radial-gradient(ellipse 60% 40% at 50% 0%,rgba(109,40,217,.15),transparent)}}
.card{{background:#0e0e1a;border:1px solid #1f1f35;border-radius:20px;padding:48px 40px;
  width:100%;max-width:380px;box-shadow:0 0 80px rgba(109,40,217,.15)}}
.logo{{text-align:center;margin-bottom:32px}}
.logo h1{{font-size:24px;font-weight:800;background:linear-gradient(135deg,#c084fc,#a78bfa);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.logo p{{color:#6b7280;font-size:12px;margin-top:5px}}
label{{display:block;font-size:12px;color:#6b7280;margin-bottom:5px;font-weight:600;text-transform:uppercase;letter-spacing:.05em}}
input{{width:100%;background:#0d0d16;border:1px solid #1f1f35;border-radius:10px;
  padding:12px 16px;color:#f1f0ff;font-family:'Syne',sans-serif;font-size:14px;
  outline:none;transition:border-color .2s;margin-bottom:16px}}
input:focus{{border-color:#7c3aed}}
button{{width:100%;background:linear-gradient(135deg,#6d28d9,#8b5cf6);border:none;
  border-radius:10px;padding:14px;color:#fff;font-family:'Syne',sans-serif;
  font-size:15px;font-weight:700;cursor:pointer;transition:opacity .2s}}
button:hover{{opacity:.85}}
.error{{background:rgba(244,63,94,.1);border:1px solid rgba(244,63,94,.25);
  color:#f43f5e;border-radius:8px;padding:10px 14px;font-size:13px;text-align:center;margin-bottom:14px}}
hr{{border:none;border-top:1px solid #1f1f35;margin:24px 0}}
.hint{{text-align:center;font-size:11px;color:#4a4a6a}}
</style></head><body>
<div class="card">
  <div class="logo"><h1>👑 RoyalityBots</h1><p>Superadmin Access Only</p></div>
  {err}
  <form method="POST">
    <label>Username</label><input type="text" name="username" required autocomplete="off">
    <label>Password</label><input type="password" name="password" required>
    <button type="submit">Sign In →</button>
  </form>
  <hr><p class="hint">Use /addadmin in bot to create accounts</p>
</div></body></html>"""


def _admin_html():
    """Returns the full admin dashboard HTML."""
    with open(__file__.replace("app.py","admin_dashboard.html")) as f:
        return f.read()


def _user_panel_html(panel, data):
    """Returns the user panel HTML with injected data."""
    panel_type = data["panel_type"]
    with open(__file__.replace("app.py","user_panel.html")) as f:
        html = f.read()
    # Inject JSON data
    html = html.replace("__PANEL_DATA__", json.dumps(data))
    html = html.replace("__PANEL_TOKEN__", panel["token"])
    html = html.replace("__PANEL_TYPE__", panel_type)
    return html


def _not_found_html():
    return """<!DOCTYPE html><html><head><title>Not Found</title>
<style>body{background:#07070e;color:#f1f0ff;font-family:sans-serif;
display:flex;align-items:center;justify-content:center;min-height:100vh;text-align:center}
h1{font-size:48px;margin-bottom:12px}p{color:#6b7280}</style></head>
<body><div><h1>🔍</h1><h2>Panel Not Found</h2>
<p>This panel link is invalid or has been deleted.</p></div></body></html>"""
