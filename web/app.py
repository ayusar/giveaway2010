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

        # ── Premium expiry reminder loop ──────────────────────
        from utils.premium_reminder import set_bot as set_prem_bot, premium_expiry_reminder_loop
        set_prem_bot(bot)
        asyncio.create_task(premium_expiry_reminder_loop())

        logger.info("🚀 Bot polling started!")
        await dp.start_polling(bot, allowed_updates=["message", "callback_query", "chat_member"])

    except Exception as e:
        logger.error(f"❌ Bot startup failed: {e}", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Bot is started by main.py — do NOT start here to avoid conflict."""
    yield


app = FastAPI(docs_url=None, redoc_url=None, lifespan=lifespan)

_sessions: dict = {}
_start_time = time.time()
PANEL_SECRET = "royalisbest"
PANEL_PARAM  = "b3c"

# ── Module-level settings import ──────────────────────────────
from config.settings import settings as settings

# ── Rate limiting: {ip: [timestamp, ...]} ─────────────────────
_rate_store: dict = {}
_RATE_LIMIT  = 60   # max requests
_RATE_WINDOW = 60   # per N seconds

def _check_rate_limit(ip: str) -> bool:
    """Return True if request is allowed, False if rate limited."""
    now = time.time()
    hits = _rate_store.get(ip, [])
    hits = [t for t in hits if now - t < _RATE_WINDOW]
    if len(hits) >= _RATE_LIMIT:
        _rate_store[ip] = hits
        return False
    hits.append(now)
    _rate_store[ip] = hits
    return True

def _get_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

def _rate_guard(request: Request):
    """Call at start of any public endpoint to enforce rate limit."""
    if not _check_rate_limit(_get_ip(request)):
        raise HTTPException(status_code=429, detail="Too many requests — slow down.")


def _hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()
def _get_token(request): return request.cookies.get("panel_session")

# ── Signed-token auth (survives server restarts) ──────────────
import hmac as _hmac, hashlib as _hashlib

def _sign_token(raw: str) -> str:
    """Return raw + '.' + HMAC signature."""
    _key = getattr(settings, "SECRET_KEY", None) or "fallback-secret-key-change-me"
    sig = _hmac.new(
        _key.encode(),
        raw.encode(),
        _hashlib.sha256
    ).hexdigest()[:16]
    return f"{raw}.{sig}"

def _verify_token(token: str) -> bool:
    """Return True if token has a valid signature and is not expired."""
    if not token or "." not in token:
        return False
    try:
        parts = token.rsplit(".", 1)
        if len(parts) != 2:
            return False
        raw, sig = parts
        _key = getattr(settings, "SECRET_KEY", None) or "fallback-secret-key-change-me"
        expected = _hmac.new(
            _key.encode(),
            raw.encode(),
            _hashlib.sha256
        ).hexdigest()[:16]
        if not _hmac.compare_digest(sig, expected):
            return False
        _, expiry_str = raw.split(":", 1)
        return float(expiry_str) > time.time()
    except Exception:
        return False

def _make_session_token() -> str:
    expiry = time.time() + 28800  # 8 hours
    return _sign_token(f"admin:{expiry}")

def _is_auth(token):
    # Support both old in-memory sessions (backward compat) and new signed tokens
    if not token:
        return False
    # New signed token
    if _verify_token(token):
        return True
    # Legacy in-memory session (still works until restart)
    if token in _sessions:
        if _sessions[token] < time.time():
            del _sessions[token]
            return False
        return True
    return False

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
            tok = _make_session_token()
            r = RedirectResponse(url=f"/adminpanel/{PANEL_SECRET}/a?{PANEL_PARAM}", status_code=302)
            r.set_cookie("panel_session", tok, httponly=True, max_age=28800, path="/", samesite="lax")
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
    r.delete_cookie("panel_session", path="/"); return r


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
    _rate_guard(request)
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    return JSONResponse(await _build_stats())


@app.get(f"/adminpanel/{PANEL_SECRET}/api/clones")
async def api_clones(request: Request):
    _rate_guard(request)
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
    _rate_guard(request)
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    from utils.giveaway_archive import get_old_giveaways
    from utils.db import get_db, is_mongo, get_sqlite_path
    data = await get_old_giveaways(limit=200)
    # Enrich each row with panel token if one exists
    try:
        gid_list = [g["giveaway_id"] for g in data if g.get("giveaway_id")]
        token_map = {}
        if is_mongo():
            db = get_db()
            panels = await db.panels.find(
                {"ref_id": {"$in": gid_list}, "panel_type": "giveaway", "is_deleted": False},
                {"ref_id": 1, "token": 1}
            ).to_list(None)
            token_map = {p["ref_id"]: p["token"] for p in panels}
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                placeholders = ",".join("?" * len(gid_list))
                async with conn.execute(
                    f"SELECT ref_id, token FROM panels WHERE ref_id IN ({placeholders}) AND panel_type='giveaway' AND is_deleted=0",
                    gid_list
                ) as cur:
                    rows = await cur.fetchall()
                token_map = {r[0]: r[1] for r in rows}
        for g in data:
            g["panel_token"] = token_map.get(g.get("giveaway_id"), None)
    except Exception as e:
        logger.warning(f"api_old_giveaways panel enrich error: {e}")
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
    _rate_guard(request)
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
async def user_panel(token: str, request: Request):
    _rate_guard(request)
    from models.panel import get_panel
    from utils.db import get_db, is_mongo, get_sqlite_path
    panel = await get_panel(token)
    if not panel:
        return HTMLResponse(_not_found_html(), status_code=404)
    # If giveaway panel — check if giveaway still exists in live DB
    if panel.get("panel_type") == "giveaway":
        ref_id = panel.get("ref_id")
        giveaway_exists = False
        if is_mongo():
            g = await get_db().giveaways.find_one({"giveaway_id": ref_id}, {"_id": 1})
            giveaway_exists = g is not None
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                async with conn.execute(
                    "SELECT giveaway_id FROM giveaways WHERE giveaway_id=?", (ref_id,)
                ) as cur:
                    giveaway_exists = (await cur.fetchone()) is not None
        if not giveaway_exists:
            # Serve page with loading screen — JS will fetch archived data
            return HTMLResponse(_archived_panel_html(panel), status_code=200)
    try:
        data = await _build_panel_data(panel)
    except Exception as e:
        logger.error(f"_build_panel_data failed for token={token}: {e}", exc_info=True)
        # Return safe fallback data so the page at least loads
        data = {
            "panel_type": panel.get("panel_type", "refer"),
            "channel_title": panel.get("channel_title", ""),
            "channel_username": panel.get("channel_username", ""),
            "member_start": 0, "member_current": 0, "member_gain": 0,
            "snap_labels": [], "snap_counts": [],
            "votes_data": [], "prizes": [], "total_votes": 0,
            "top_referrers": [], "refer_data": [], "total_refs": 0,
            "created_at": "", "is_active": False, "title": "",
        }
    return HTMLResponse(_user_panel_html(panel, data))


def _user_panel_html(panel, data):
    """Returns the user panel HTML with injected data. Guaranteed — never returns raw template."""
    import json as _json, re as _re
    panel_type = data.get("panel_type", panel.get("panel_type", "refer"))
    token = panel.get("token", "")

    with open(__file__.replace("app.py", "user_panel.html")) as f:
        html = f.read()

    # Serialize — default=str handles datetime/ObjectId/Decimal safely
    try:
        json_data = _json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        json_data = "{}"
    # Escape </script> inside JSON so it doesn't break the <script> block
    json_data = json_data.replace("</", "<\\/")

    # Replace placeholder — use regex so whitespace variations don't matter
    html = _re.sub(r"__PANEL_DATA__", json_data, html)
    html = _re.sub(r"'__PANEL_TOKEN__'", _json.dumps(token), html)
    html = _re.sub(r"__PANEL_TOKEN__", token, html)
    html = _re.sub(r"__PANEL_TYPE__", panel_type, html)

    # Final guarantee — if still present, the HTML template has unexpected content
    if "__PANEL_DATA__" in html:
        logger.error("__PANEL_DATA__ placeholder NOT replaced — injecting fallback")
        html = html.replace("__PANEL_DATA__", json_data)
    if "__PANEL_TOKEN__" in html:
        html = html.replace("__PANEL_TOKEN__", token)

    return html


@app.get("/panel/{token}/api/data")
async def user_panel_data(token: str, request: Request):
    _rate_guard(request)
    # Token is in the URL path itself — that IS the auth. No referer check needed.
    from models.panel import get_panel
    panel = await get_panel(token)
    if not panel: raise HTTPException(404)
    # Check if giveaway archived — fetch from Telegram if needed
    if panel.get("panel_type") == "giveaway":
        from utils.db import get_db, is_mongo, get_sqlite_path
        ref_id = panel.get("ref_id")
        giveaway_exists = False
        if is_mongo():
            g = await get_db().giveaways.find_one({"giveaway_id": ref_id}, {"_id": 1})
            giveaway_exists = g is not None
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                async with conn.execute("SELECT giveaway_id FROM giveaways WHERE giveaway_id=?", (ref_id,)) as cur:
                    giveaway_exists = (await cur.fetchone()) is not None
        if not giveaway_exists:
            archived = await _fetch_archived_giveaway(ref_id)
            if archived:
                return JSONResponse({"archived": True, **archived})
            raise HTTPException(410, detail="Giveaway archived and data unavailable")
    try:
        data = await _build_panel_data(panel)
    except Exception as e:
        logger.error(f"user_panel_data build failed token={token}: {e}")
        raise HTTPException(500, detail="Failed to build panel data")
    return JSONResponse(data)


@app.post("/panel/{token}/delete")
async def user_panel_delete(token: str, request: Request):
    """Panel owner can delete — token in URL is the auth."""
    _rate_guard(request)
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
        db = get_db()
        panels = await db.panels.find({"is_deleted": False}).sort("created_at", -1).limit(200).to_list(None)
        # Filter out panels whose giveaway has been archived (deleted from live DB)
        live_ids = set()
        giveaway_panels = [p for p in panels if p.get("panel_type") == "giveaway"]
        refer_panels    = [p for p in panels if p.get("panel_type") != "giveaway"]
        if giveaway_panels:
            ref_ids = [p["ref_id"] for p in giveaway_panels]
            live_docs = await db.giveaways.find(
                {"giveaway_id": {"$in": ref_ids}}, {"giveaway_id": 1}
            ).to_list(None)
            live_ids = {d["giveaway_id"] for d in live_docs}
        result = []
        for p in refer_panels:
            result.append({"token": p["token"], "panel_type": p["panel_type"],
                           "channel_title": p.get("channel_title", ""),
                           "channel_username": p.get("channel_username", ""),
                           "created_at": str(p.get("created_at", ""))[:10]})
        for p in giveaway_panels:
            if p["ref_id"] in live_ids:  # only show if still live in DB
                result.append({"token": p["token"], "panel_type": p["panel_type"],
                               "channel_title": p.get("channel_title", ""),
                               "channel_username": p.get("channel_username", ""),
                               "created_at": str(p.get("created_at", ""))[:10]})
        result.sort(key=lambda x: x["created_at"], reverse=True)
        return result[:100]
    import aiosqlite
    async with aiosqlite.connect(get_sqlite_path()) as conn:
        conn.row_factory = aiosqlite.Row
        try:
            async with conn.execute("SELECT * FROM panels WHERE is_deleted=0 ORDER BY created_at DESC LIMIT 200") as cur:
                rows = await cur.fetchall()
            panels = [dict(r) for r in rows]
            result = []
            for d in panels:
                if d.get("panel_type") == "giveaway":
                    # Check if giveaway still exists in live DB
                    async with conn.execute(
                        "SELECT giveaway_id FROM giveaways WHERE giveaway_id=?", (d["ref_id"],)
                    ) as cur2:
                        exists = await cur2.fetchone()
                    if not exists:
                        continue  # archived — skip from admin panel
                result.append({"token": d["token"], "panel_type": d["panel_type"],
                               "channel_title": d.get("channel_title", ""),
                               "channel_username": d.get("channel_username", ""),
                               "created_at": str(d.get("created_at", ""))[:10]})
            return result
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

async def _fetch_archived_giveaway(giveaway_id: str) -> dict | None:
    """
    Download full giveaway JSON from Telegram DATABASE_CHANNEL via file_id.
    Used when giveaway has been closed and purged from MongoDB.
    """
    import io, json as _json
    from utils.db import get_db, is_mongo, get_sqlite_path
    from utils.log_utils import get_main_bot
    try:
        file_id = None
        if is_mongo():
            ref = await get_db().giveaway_archive_refs.find_one({"giveaway_id": giveaway_id})
            if ref:
                file_id = ref.get("file_id")
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                async with conn.execute(
                    "SELECT file_id FROM giveaway_archive_refs WHERE giveaway_id=?", (giveaway_id,)
                ) as cur:
                    row = await cur.fetchone()
                if row:
                    file_id = row[0]
        if not file_id:
            return None
        bot = get_main_bot()
        if not bot:
            return None
        file = await bot.get_file(file_id)
        buf = io.BytesIO()
        await bot.download_file(file.file_path, destination=buf)
        buf.seek(0)
        archive = _json.loads(buf.read())
        g = archive.get("giveaway", {})
        if not g:
            return None
        prizes  = g.get("prizes", [])
        options = g.get("options", [])
        votes   = g.get("votes", {})
        if isinstance(prizes, str):  prizes  = _json.loads(prizes)
        if isinstance(options, str): options = _json.loads(options)
        if isinstance(votes, str):   votes   = _json.loads(votes)
        raw_votes = {int(k): v for k, v in votes.items()}
        votes_data = [{"name": options[i], "votes": raw_votes.get(i, 0)} for i in range(len(options))]
        votes_data.sort(key=lambda x: x["votes"], reverse=True)
        return {
            "options":     options,
            "prizes":      prizes,
            "total_votes": g.get("total_votes", 0),
            "votes_data":  votes_data,
            "is_active":   False,
            "title":       g.get("title", giveaway_id),
        }
    except Exception as e:
        logger.warning(f"_fetch_archived_giveaway {giveaway_id}: {e}")
        return None


async def _build_panel_data(panel: dict) -> dict:
    """Build full data for the user panel page."""
    from utils.db import get_db, is_mongo, get_sqlite_path
    import json as _json
    panel_type = panel.get("panel_type")
    ref_id = panel.get("ref_id")
    votes_data, options, prizes, total_votes = [], [], [], 0
    refer_data, top_referrers = [], []
    total_refs = 0
    is_archived = False

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

    # Channel growth snapshots — field may be a JSON string (SQLite) or list (Mongo)
    snapshots = panel.get("member_snapshots", [])
    if isinstance(snapshots, str):
        try:
            import json as _json2
            snapshots = _json2.loads(snapshots) if snapshots else []
        except Exception:
            snapshots = []
    if not isinstance(snapshots, list):
        snapshots = []
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

@app.get(f"/adminpanel/{PANEL_SECRET}/api/premium_users")
async def api_premium_users(request: Request):
    """List all premium users with status info."""
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    from utils.db import get_db, is_mongo, get_sqlite_path
    now = datetime.utcnow()
    users = []
    try:
        if is_mongo():
            docs = await get_db().premium_users.find({}).sort("granted_at", -1).to_list(None)
            for d in docs:
                exp = d.get("expires_at", "")
                try:
                    exp_dt = datetime.fromisoformat(str(exp).replace("Z",""))
                    days_left = (exp_dt - now).days
                    if days_left > 3:
                        status = "active"
                    elif days_left >= 0:
                        status = "expiring"
                    else:
                        status = "expired"
                except Exception:
                    days_left = -1
                    status = "expired"
                users.append({
                    "user_id": d.get("user_id"),
                    "granted_by": d.get("granted_by", ""),
                    "expires_at": str(exp)[:10],
                    "granted_at": str(d.get("granted_at",""))[:10],
                    "status": status,
                    "days_left": days_left,
                })
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                try:
                    async with conn.execute("SELECT * FROM premium_users ORDER BY granted_at DESC") as cur:
                        rows = await cur.fetchall()
                except Exception:
                    rows = []
                for r in rows:
                    d = dict(r)
                    exp = d.get("expires_at","")
                    try:
                        exp_dt = datetime.fromisoformat(str(exp))
                        days_left = (exp_dt - now).days
                        if days_left > 3:
                            status = "active"
                        elif days_left >= 0:
                            status = "expiring"
                        else:
                            status = "expired"
                    except Exception:
                        days_left = -1
                        status = "expired"
                    users.append({
                        "user_id": d.get("user_id"),
                        "granted_by": d.get("granted_by",""),
                        "expires_at": str(exp)[:10],
                        "granted_at": str(d.get("granted_at",""))[:10],
                        "status": status,
                        "days_left": days_left,
                    })
    except Exception as e:
        logger.error(f"api_premium_users error: {e}")
    return JSONResponse({"users": users, "total": len(users)})


@app.post(f"/adminpanel/{PANEL_SECRET}/api/grant_premium")
async def api_grant_premium(request: Request):
    """Grant premium to a user (called from admin dashboard)."""
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    data = await request.json()
    user_id = data.get("user_id")
    days = int(data.get("days", 30))
    if not user_id:
        return JSONResponse({"ok": False, "error": "Missing user_id"})
    try:
        user_id = int(user_id)
    except (ValueError, TypeError):
        return JSONResponse({"ok": False, "error": "Invalid user_id"})
    from utils.premium import add_premium
    # Try to get session to find who granted
    token = _get_token(request)
    granted_by = _sessions.get(token, {}).get("user", "admin")
    expires_at = await add_premium(user_id, days, granted_by=0)
    return JSONResponse({"ok": True, "expires_at": expires_at.strftime("%Y-%m-%d")})


@app.post(f"/adminpanel/{PANEL_SECRET}/api/revoke_premium")
async def api_revoke_premium(request: Request):
    """Revoke premium from a user."""
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    data = await request.json()
    user_id = data.get("user_id")
    if not user_id:
        return JSONResponse({"ok": False, "error": "Missing user_id"})
    try:
        user_id = int(user_id)
    except (ValueError, TypeError):
        return JSONResponse({"ok": False, "error": "Invalid user_id"})
    from utils.premium import remove_premium
    removed = await remove_premium(user_id)
    return JSONResponse({"ok": removed, "error": "" if removed else "User had no premium"})


@app.post(f"/adminpanel/{PANEL_SECRET}/api/extend_premium")
async def api_extend_premium(request: Request):
    """Extend (add days on top of current expiry) premium for a user."""
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    data = await request.json()
    user_id = data.get("user_id")
    days = int(data.get("days", 30))
    if not user_id:
        return JSONResponse({"ok": False, "error": "Missing user_id"})
    try:
        user_id = int(user_id)
    except (ValueError, TypeError):
        return JSONResponse({"ok": False, "error": "Invalid user_id"})
    from utils.db import get_db, is_mongo, get_sqlite_path
    from datetime import timedelta, timezone
    try:
        if is_mongo():
            doc = await get_db().premium_users.find_one({"user_id": user_id})
            if doc:
                try:
                    current_exp = datetime.fromisoformat(str(doc["expires_at"]).replace("Z",""))
                except Exception:
                    current_exp = datetime.utcnow()
            else:
                current_exp = datetime.utcnow()
            new_exp = (current_exp if current_exp > datetime.utcnow() else datetime.utcnow()) + timedelta(days=days)
            await get_db().premium_users.update_one(
                {"user_id": user_id},
                {"$set": {"user_id": user_id, "expires_at": new_exp.isoformat(), "granted_at": datetime.utcnow().isoformat()}},
                upsert=True,
            )
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                async with conn.execute("SELECT expires_at FROM premium_users WHERE user_id=?", (user_id,)) as cur:
                    row = await cur.fetchone()
                if row:
                    try:
                        current_exp = datetime.fromisoformat(row[0])
                    except Exception:
                        current_exp = datetime.utcnow()
                else:
                    current_exp = datetime.utcnow()
                new_exp = (current_exp if current_exp > datetime.utcnow() else datetime.utcnow()) + timedelta(days=days)
                await conn.execute(
                    """INSERT INTO premium_users (user_id, granted_by, expires_at, granted_at)
                       VALUES (?,?,?,?)
                       ON CONFLICT(user_id) DO UPDATE SET expires_at=excluded.expires_at""",
                    (user_id, 0, new_exp.isoformat(), datetime.utcnow().isoformat())
                )
                await conn.commit()
        return JSONResponse({"ok": True, "new_expires_at": new_exp.strftime("%Y-%m-%d")})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.get(f"/adminpanel/{PANEL_SECRET}/api/activity_log")
async def api_activity_log(request: Request):
    """Return recent activity log entries from the database."""
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    from utils.db import get_db, is_mongo, get_sqlite_path
    logs = []
    try:
        if is_mongo():
            db = get_db()
            # Recent premium grants
            async for d in db.premium_users.find({}).sort("granted_at", -1).limit(5):
                logs.append({
                    "color": "var(--yellow)",
                    "msg": f"⭐ Premium granted to user {d.get('user_id')}",
                    "time": str(d.get("granted_at",""))[:19],
                })
            # Recent bans
            async for d in db.banned_users.find({}).limit(5):
                logs.append({
                    "color": "var(--red)",
                    "msg": f"🚫 User {d.get('user_id')} banned",
                    "time": str(d.get("banned_at",""))[:19],
                })
            # Recent clone bots
            async for d in db.clone_bots.find({}).sort("created_at",-1).limit(5):
                logs.append({
                    "color": "var(--green)",
                    "msg": f"🤖 Clone bot @{d.get('bot_username','?')} registered",
                    "time": str(d.get("created_at",""))[:19],
                })
            # Recent giveaways
            async for d in db.giveaways.find({}).sort("created_at",-1).limit(5):
                logs.append({
                    "color": "var(--accent)",
                    "msg": f"🗳 Giveaway \"{d.get('title','?')}\" created",
                    "time": str(d.get("created_at",""))[:19],
                })
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                try:
                    async with conn.execute("SELECT user_id, granted_at FROM premium_users ORDER BY granted_at DESC LIMIT 5") as cur:
                        for r in await cur.fetchall():
                            logs.append({"color":"var(--yellow)","msg":f"⭐ Premium granted to user {r['user_id']}","time":str(r['granted_at'] or '')[:19]})
                except Exception: pass
                try:
                    async with conn.execute("SELECT user_id FROM banned_users LIMIT 5") as cur:
                        for r in await cur.fetchall():
                            logs.append({"color":"var(--red)","msg":f"🚫 User {r['user_id']} banned","time":""})
                except Exception: pass
                try:
                    async with conn.execute("SELECT bot_username, created_at FROM clone_bots ORDER BY created_at DESC LIMIT 5") as cur:
                        for r in await cur.fetchall():
                            logs.append({"color":"var(--green)","msg":f"🤖 Clone bot @{r['bot_username'] or '?'} registered","time":str(r['created_at'] or '')[:19]})
                except Exception: pass
                try:
                    async with conn.execute("SELECT title, created_at FROM giveaways ORDER BY created_at DESC LIMIT 5") as cur:
                        for r in await cur.fetchall():
                            logs.append({"color":"var(--accent)","msg":f"🗳 Giveaway \"{r['title'] or '?'}\" created","time":str(r['created_at'] or '')[:19]})
                except Exception: pass
        # sort by time desc
        logs.sort(key=lambda x: x.get("time",""), reverse=True)
    except Exception as e:
        logger.error(f"activity_log error: {e}")
    return JSONResponse({"logs": logs[:20]})


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
    """Returns the full admin dashboard HTML with injected API base path."""
    with open(__file__.replace("app.py","admin_dashboard.html")) as f:
        html = f.read()
    # Inject the real API base so JS never has a hardcoded wrong path
    api_base = f"/adminpanel/{PANEL_SECRET}"
    html = html.replace(
        "const API_BASE = '/adminpanel/royalisbest';",
        f"const API_BASE = '{api_base}';"
    )
    # Also replace any leftover hardcoded paths (belt+suspenders)
    html = html.replace("/adminpanel/royalisbest/", f"{api_base}/")
    return html




def _not_found_html():
    return """<!DOCTYPE html><html><head><title>Not Found</title>
<style>body{background:#07070e;color:#f1f0ff;font-family:sans-serif;
display:flex;align-items:center;justify-content:center;min-height:100vh;text-align:center}
h1{font-size:48px;margin-bottom:12px}p{color:#6b7280}</style></head>
<body><div><h1>🔍</h1><h2>Panel Not Found</h2>
<p>This panel link is invalid or has been deleted.</p></div></body></html>"""


def _giveaway_ended_html(panel: dict) -> str:
    title   = panel.get("channel_title", "This Giveaway")
    created = str(panel.get("created_at", ""))[:10]
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Giveaway Ended</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#07070e;color:#f1f0ff;font-family:'Segoe UI',sans-serif;
       display:flex;align-items:center;justify-content:center;min-height:100vh;padding:24px}}
  .card{{background:#12121f;border:1px solid #2a2a40;border-radius:20px;
         padding:48px 36px;text-align:center;max-width:420px;width:100%;
         box-shadow:0 8px 40px rgba(0,0,0,.5)}}
  .icon{{font-size:64px;margin-bottom:16px}}
  h1{{font-size:26px;font-weight:700;margin-bottom:10px;color:#f1f0ff}}
  .subtitle{{color:#6b7280;font-size:15px;line-height:1.6;margin-bottom:28px}}
  .badge{{display:inline-block;background:#1e1e30;border:1px solid #3a3a55;
          border-radius:999px;padding:6px 18px;font-size:13px;color:#a0aec0;margin-bottom:8px}}
  .info{{background:#0f0f1c;border-radius:12px;padding:16px 20px;
         font-size:13px;color:#6b7280;margin-top:24px;border:1px solid #1e1e30}}
  .info strong{{color:#a0aec0}}
</style></head>
<body><div class="card">
  <div class="icon">🏁</div>
  <h1>Giveaway Has Ended</h1>
  <p class="subtitle">The analytics for <strong>{title}</strong> are no longer available
  because this giveaway has been closed and its data archived.</p>
  <span class="badge">📦 Archived</span>
  <div class="info">
    Created: <strong>{created}</strong><br>
    Status: <strong>Closed &amp; Archived</strong>
  </div>
</div></body></html>"""


def _archived_panel_html(panel: dict) -> str:
    """Page with loading screen that fetches archived data from Telegram via JS."""
    title   = panel.get("channel_title", "Giveaway")
    token   = panel.get("token", "")
    created = str(panel.get("created_at", ""))[:10]
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} — Archived Analytics</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#07070e;color:#f1f0ff;font-family:'Segoe UI',sans-serif;min-height:100vh;padding:24px}}

  /* ── Loading screen ── */
  #loading{{display:flex;flex-direction:column;align-items:center;justify-content:center;
            min-height:80vh;gap:16px;text-align:center}}
  .spinner{{width:52px;height:52px;border:4px solid #2a2a40;border-top-color:#7c6aff;
            border-radius:50%;animation:spin 0.9s linear infinite}}
  @keyframes spin{{to{{transform:rotate(360deg)}}}}
  #loading h2{{font-size:20px;color:#a0aec0}}
  #loading p{{font-size:13px;color:#4b5563}}

  /* ── Error screen ── */
  #error{{display:none;align-items:center;justify-content:center;min-height:80vh;
          text-align:center;flex-direction:column;gap:12px}}
  #error .icon{{font-size:48px}}
  #error h2{{font-size:20px;color:#f87171}}
  #error p{{color:#6b7280;font-size:14px}}

  /* ── Data screen ── */
  #data{{display:none}}
  .header{{margin-bottom:28px}}
  .header h1{{font-size:24px;font-weight:700}}
  .header .meta{{color:#6b7280;font-size:13px;margin-top:4px}}
  .badge{{display:inline-block;background:#1e1e30;border:1px solid #7c6aff44;
          border-radius:999px;padding:4px 14px;font-size:12px;color:#7c6aff;margin-top:8px}}
  .card{{background:#12121f;border:1px solid #2a2a40;border-radius:16px;padding:24px;margin-bottom:20px}}
  .card h3{{font-size:15px;color:#a0aec0;margin-bottom:16px;text-transform:uppercase;
            letter-spacing:.05em;font-weight:600}}
  .stat-row{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:20px}}
  .stat{{background:#12121f;border:1px solid #2a2a40;border-radius:12px;
         padding:20px;flex:1;min-width:120px;text-align:center}}
  .stat .val{{font-size:28px;font-weight:700;color:#7c6aff}}
  .stat .lbl{{font-size:12px;color:#6b7280;margin-top:4px}}
  .bar-row{{margin-bottom:12px}}
  .bar-label{{display:flex;justify-content:space-between;font-size:13px;margin-bottom:5px}}
  .bar-label .name{{color:#e5e7eb;max-width:70%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
  .bar-label .count{{color:#7c6aff;font-weight:600}}
  .bar-bg{{background:#1e1e30;border-radius:999px;height:10px;overflow:hidden}}
  .bar-fill{{height:10px;border-radius:999px;background:linear-gradient(90deg,#7c6aff,#a78bfa);
             transition:width 0.6s ease}}
  .prizes{{display:flex;flex-wrap:wrap;gap:10px}}
  .prize{{background:#1a1a2e;border:1px solid #2a2a40;border-radius:10px;
          padding:10px 16px;font-size:14px;color:#e5e7eb}}
</style></head>
<body>

<div id="loading">
  <div class="spinner"></div>
  <h2>Loading archived data…</h2>
  <p>Fetching results from the database. This may take a moment.</p>
</div>

<div id="error">
  <div class="icon">⚠️</div>
  <h2>Could not load data</h2>
  <p id="error-msg">The archived data could not be retrieved. Please try again later.</p>
</div>

<div id="data">
  <div class="header">
    <h1 id="d-title">{title}</h1>
    <div class="meta">Created: {created}</div>
    <span class="badge">📦 Archived Giveaway</span>
  </div>
  <div class="stat-row">
    <div class="stat"><div class="val" id="d-votes">—</div><div class="lbl">Total Votes</div></div>
    <div class="stat"><div class="val" id="d-opts">—</div><div class="lbl">Options</div></div>
    <div class="stat"><div class="val" id="d-prizes">—</div><div class="lbl">Prizes</div></div>
  </div>
  <div class="card">
    <h3>🏆 Vote Results</h3>
    <div id="d-bars"></div>
  </div>
  <div class="card">
    <h3>🎁 Prizes</h3>
    <div class="prizes" id="d-prize-list"></div>
  </div>
</div>

<script>
(async () => {{
  try {{
    const res = await fetch('/panel/{token}/api/data');
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const d = await res.json();

    // Populate stats
    document.getElementById('d-votes').textContent = d.total_votes ?? 0;
    const vd = d.votes_data || [];
    document.getElementById('d-opts').textContent = vd.length;
    document.getElementById('d-prizes').textContent = (d.prizes || []).length;

    // Vote bars
    const maxV = vd.length ? Math.max(...vd.map(x => x.votes), 1) : 1;
    const bars = document.getElementById('d-bars');
    bars.innerHTML = vd.length ? vd.map(o => `
      <div class="bar-row">
        <div class="bar-label">
          <span class="name">${{o.name}}</span>
          <span class="count">${{o.votes}}</span>
        </div>
        <div class="bar-bg"><div class="bar-fill" style="width:${{Math.round(o.votes/maxV*100)}}%"></div></div>
      </div>`).join('') : '<p style="color:#6b7280;font-size:14px">No votes recorded.</p>';

    // Prizes
    const pl = document.getElementById('d-prize-list');
    pl.innerHTML = (d.prizes || []).length
      ? (d.prizes || []).map(p => `<div class="prize">🎁 ${{p}}</div>`).join('')
      : '<p style="color:#6b7280;font-size:14px">No prizes listed.</p>';

    document.getElementById('loading').style.display = 'none';
    document.getElementById('data').style.display = 'block';
  }} catch(e) {{
    document.getElementById('loading').style.display = 'none';
    document.getElementById('error-msg').textContent = e.message || 'Unknown error';
    document.getElementById('error').style.display = 'flex';
  }}
}})();
</script>
</body></html>"""


# ══════════════════════════════════════════════════════════════
# MISSING ENDPOINTS — required by new admin_dashboard.html
# ══════════════════════════════════════════════════════════════

@app.get(f"/adminpanel/{PANEL_SECRET}/api/clone_users")
async def api_clone_users_query(token: str, request: Request, username: str = ""):
    """
    New dashboard calls /api/clone_users?token=X&username=Y (query params).
    Old route used path param /api/clone_users/{token}.
    This new endpoint supports BOTH calling styles.
    """
    _rate_guard(request)
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
        logger.error(f"api_clone_users_query error: {e}")
        raise HTTPException(500, detail=str(e))
    return JSONResponse({"token": token, "users": users, "total": len(users)})


@app.get(f"/adminpanel/{PANEL_SECRET}/api/giveaway_participants")
async def api_giveaway_participants_query(giveaway_id: str, request: Request):
    """
    New dashboard calls /api/giveaway_participants?giveaway_id=X (query param).
    Old route used path param /api/giveaway_participants/{giveaway_id}.
    """
    _rate_guard(request)
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
                ref_doc = await db.referrals.find_one({"user_id": uid})
                refer_count = ref_doc.get("refer_count", 0) if ref_doc else 0
                participants.append({
                    "user_id":     uid,
                    "user_name":   v.get("user_name") or "Unknown",
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
                        "ORDER BY refer_count DESC LIMIT 1", (uid,)
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
        logger.error(f"giveaway_participants_query error: {e}")
        raise HTTPException(500, detail=str(e))
    participants.sort(key=lambda x: x["refer_count"], reverse=True)
    return JSONResponse({
        "giveaway_id": giveaway_id,
        "participants": participants,
        "total": len(participants),
    })


@app.get(f"/adminpanel/{PANEL_SECRET}/api/archive_list")
async def api_archive_list(request: Request):
    """Returns archived giveaways list — called by new dashboard."""
    _rate_guard(request)
    if not _is_auth(_get_token(request)):
        raise HTTPException(401)
    from utils.giveaway_archive import get_old_giveaways
    data = await get_old_giveaways(limit=200)
    return JSONResponse({"giveaways": data, "total": len(data)})


@app.post(f"/adminpanel/{PANEL_SECRET}/api/delete_old_archives")
async def api_delete_old_archives(request: Request):
    """Delete archived giveaways older than N months — called by new dashboard."""
    if not _is_auth(_get_token(request)):
        raise HTTPException(401)
    body = await request.json()
    months = int(body.get("months", 0))
    if months < 1:
        return JSONResponse({"ok": False, "error": "months must be >= 1"}, status_code=400)
    from utils.giveaway_archive import delete_old_giveaways_before
    deleted = await delete_old_giveaways_before(months)
    return JSONResponse({"ok": True, "deleted": deleted})


@app.post(f"/adminpanel/{PANEL_SECRET}/api/unban_clone")
async def api_unban_clone(request: Request):
    """Unban a clone bot — called by new dashboard."""
    if not _is_auth(_get_token(request)):
        raise HTTPException(401)
    data = await request.json()
    token = data.get("token", "")
    if not token:
        return JSONResponse({"ok": False, "error": "Missing token"})
    from utils.db import get_db, is_mongo, get_sqlite_path
    try:
        if is_mongo():
            await get_db().clone_bots.update_one(
                {"token": token},
                {"$set": {"is_banned": False}}
            )
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                await conn.execute(
                    "UPDATE clone_bots SET is_banned=0 WHERE token=?", (token,)
                )
                await conn.commit()
        # Re-start the clone if it was stopped
        try:
            from utils.clone_manager import get_clone_manager
            await get_clone_manager().start_clone(token)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"unban_clone error: {e}")
        return JSONResponse({"ok": False, "error": str(e)})
    return JSONResponse({"ok": True})


@app.post(f"/adminpanel/{PANEL_SECRET}/api/close_giveaway")
async def api_close_giveaway(request: Request):
    """Close/end a live giveaway and archive it with full stats."""
    if not _is_auth(_get_token(request)):
        raise HTTPException(401)
    data = await request.json()
    giveaway_id = data.get("giveaway_id", "")
    if not giveaway_id:
        return JSONResponse({"ok": False, "error": "Missing giveaway_id"})
    from utils.db import get_db, is_mongo, get_sqlite_path
    try:
        if is_mongo():
            await get_db().giveaways.update_one(
                {"giveaway_id": giveaway_id},
                {"$set": {"is_active": False, "closed_at": datetime.utcnow().isoformat()}}
            )
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                await conn.execute(
                    "UPDATE giveaways SET is_active=0 WHERE giveaway_id=?",
                    (giveaway_id,)
                )
                await conn.commit()
    except Exception as e:
        logger.error(f"close_giveaway error: {e}")
        return JSONResponse({"ok": False, "error": str(e)})

    # Archive in background — don't block the response
    from utils.log_utils import get_main_bot
    bot = get_main_bot()
    if bot:
        import asyncio as _asyncio
        async def _bg_archive():
            try:
                from utils.giveaway_archive import archive_and_purge
                await archive_and_purge(bot, giveaway_id)
                logger.info(f"✅ Admin dashboard archived giveaway {giveaway_id}")
            except Exception as _e:
                logger.warning(f"Admin close_giveaway archive failed (non-fatal): {_e}")
        _asyncio.create_task(_bg_archive())
        return JSONResponse({"ok": True, "archiving": True})

    return JSONResponse({"ok": True, "archiving": False})


@app.get(f"/adminpanel/{PANEL_SECRET}/api/download_giveaway")
async def api_download_giveaway(request: Request, giveaway_id: str):
    """
    Download the archived giveaway JSON file directly.
    Fetches from Telegram and streams it back to the admin browser.
    """
    if not _is_auth(_get_token(request)):
        raise HTTPException(401)
    if not giveaway_id:
        raise HTTPException(400, detail="Missing giveaway_id")

    from utils.db import get_db, is_mongo, get_sqlite_path
    import io

    # 1. Look up file_id from archive refs
    file_id = None
    meta = {}
    try:
        if is_mongo():
            doc = await get_db().giveaway_archive_refs.find_one({"giveaway_id": giveaway_id.upper()})
            if doc:
                file_id = doc.get("file_id")
                meta = doc
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(
                    "SELECT * FROM giveaway_archive_refs WHERE giveaway_id=?", (giveaway_id.upper(),)
                ) as cur:
                    row = await cur.fetchone()
                if row:
                    meta = dict(row)
                    file_id = meta.get("file_id")
    except Exception as e:
        logger.error(f"download_giveaway lookup error: {e}")
        raise HTTPException(500, detail=str(e))

    if not file_id:
        raise HTTPException(404, detail="No archived file found for this giveaway")

    # 2. Fetch from Telegram
    from utils.log_utils import get_main_bot
    bot = get_main_bot()
    if not bot:
        raise HTTPException(503, detail="Bot not ready — try again in a moment")
    try:
        tg_file = await bot.get_file(file_id)
        buf = io.BytesIO()
        await bot.download_file(tg_file.file_path, destination=buf)
        buf.seek(0)
        content_bytes = buf.read()
    except Exception as e:
        logger.error(f"download_giveaway Telegram fetch error: {e}")
        raise HTTPException(502, detail=f"Could not fetch file from Telegram: {e}")

    filename = f"giveaway_{giveaway_id}.json"
    from starlette.responses import Response
    return Response(
        content=content_bytes,
        media_type="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(content_bytes)),
        }
    )


# ══════════════════════════════════════════════════════════════
# PREMIUM USER DASHBOARD  (/tg/premium/...)
# Path: /tg/premium/user/tg/security
# ══════════════════════════════════════════════════════════════
#
# Authentication flow:
#   1. Admin runs /addpreuser <tg_id>:<username>:<password> in bot
#   2. User visits /tg/premium/login  (Telegram Mini App)
#   3. On success → redirected to /tg/premium/user/tg/security
#   4. Dashboard fetches live data via /tg/premium/api/* endpoints
#
# Session cookies are separate from the admin panel sessions.
# ──────────────────────────────────────────────────────────────

import hashlib as _hashlib
import secrets as _secrets
import time as _time_prem

_prem_sessions: dict = {}          # token → expiry
_PREM_SESSION_TTL = 28800          # 8 hours


def _hash_prem_pw(pw: str) -> str:
    return _hashlib.sha256(pw.encode()).hexdigest()


def _get_prem_token(request: Request) -> str | None:
    return request.cookies.get("prem_session")


def _is_prem_auth(token: str | None) -> bool:
    if not token or token not in _prem_sessions:
        return False
    if _prem_sessions[token] < _time_prem.time():
        del _prem_sessions[token]
        return False
    return True


async def _check_prem_creds(username: str, password: str) -> bool:
    """Check username/password against premium_panel_users table."""
    try:
        from utils.db import get_db, is_mongo, get_sqlite_path
        hashed = _hash_prem_pw(password)
        if is_mongo():
            db = get_db()
            if db is None:
                return False
            import re as _re
            return bool(await db.premium_panel_users.find_one(
                {"username": {"$regex": f"^{_re.escape(username)}$", "$options": "i"}, "password": hashed}
            ))
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS premium_panel_users "
                "(id INTEGER PRIMARY KEY, tg_id INTEGER, username TEXT UNIQUE, password TEXT)"
            )
            await conn.commit()
            async with conn.execute(
                "SELECT id FROM premium_panel_users WHERE LOWER(username)=LOWER(?) AND password=?",
                (username, hashed)
            ) as cur:
                return await cur.fetchone() is not None
    except Exception as e:
        logger.error(f"_check_prem_creds error: {e}")
        return False


def _prem_login_html(error: str = "") -> str:
    """Load and render premium login page."""
    import os
    html_path = os.path.join(os.path.dirname(__file__), "premium_login.html")
    try:
        with open(html_path) as f:
            html = f.read()
    except FileNotFoundError:
        # Fallback inline login
        html = """<!DOCTYPE html><html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Premium Login</title>
<style>*{box-sizing:border-box;margin:0;padding:0}
body{background:#07070e;color:#f1f0ff;font-family:sans-serif;min-height:100vh;
display:flex;align-items:center;justify-content:center;padding:24px}
.card{background:#0e0e1a;border:1px solid #1e1e32;border-radius:20px;padding:40px;
width:100%;max-width:380px}
h1{font-size:22px;margin-bottom:24px;text-align:center}
label{font-size:12px;color:#5a5880;display:block;margin-bottom:5px;text-transform:uppercase}
input{width:100%;background:#0d0d18;border:1px solid #1e1e32;border-radius:10px;
padding:12px 14px;color:#f1f0ff;font-size:14px;outline:none;margin-bottom:14px}
input:focus{border-color:#6c5ce7}
button{width:100%;background:#6c5ce7;border:none;border-radius:10px;
padding:13px;color:#fff;font-size:15px;font-weight:700;cursor:pointer}
.error{background:rgba(225,112,85,.1);border:1px solid rgba(225,112,85,.3);
color:#e17055;border-radius:8px;padding:10px;text-align:center;margin-bottom:14px;font-size:13px}
</style></head><body>
<div class="card">
  <h1>👑 Premium Login</h1>
  __ERROR_BLOCK__
  <form method="POST">
    <label>Username</label><input type="text" name="username" required>
    <label>Password</label><input type="password" name="password" required>
    <button type="submit">Sign In →</button>
  </form>
</div></body></html>"""

    err_html = f'<div class="error">{error}</div>' if error else ""
    return html.replace("__ERROR_BLOCK__", err_html)


def _prem_403_html() -> str:
    return """<!DOCTYPE html><html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Access Denied</title>
<style>*{box-sizing:border-box;margin:0;padding:0}
body{background:#07070e;color:#f1f0ff;font-family:sans-serif;min-height:100vh;
display:flex;align-items:center;justify-content:center;text-align:center;padding:24px}
.icon{font-size:64px;margin-bottom:16px}h1{font-size:24px;margin-bottom:10px}
p{color:#5a5880;font-size:14px;line-height:1.6;max-width:320px}
a{color:#6c5ce7;text-decoration:none;display:inline-block;margin-top:20px;
font-size:13px;border:1px solid #6c5ce7;border-radius:8px;padding:8px 20px}
</style></head><body><div>
<div class="icon">🔒</div>
<h1>Premium Access Only</h1>
<p>This dashboard is available to premium users only.<br>
Contact your admin to get access.</p>
<a href="/tg/premium/login">← Go to Login</a>
</div></body></html>"""


# ── Login ──────────────────────────────────────────────────────
@app.get("/tg/premium/login", response_class=HTMLResponse)
async def prem_login_get():
    return HTMLResponse(_prem_login_html())


@app.post("/tg/premium/login")
async def prem_login_post(request: Request):
    _rate_guard(request)
    try:
        form = await request.form()
        username = form.get("username", "").strip()
        password = form.get("password", "")
        if await _check_prem_creds(username, password):
            tok = _secrets.token_hex(32)
            _prem_sessions[tok] = _time_prem.time() + _PREM_SESSION_TTL
            r = RedirectResponse(url="/tg/premium/user/tg/security", status_code=302)
            r.set_cookie("prem_session", tok, httponly=True, max_age=_PREM_SESSION_TTL, samesite="lax")
            return r
        return HTMLResponse(_prem_login_html("❌ Invalid username or password"))
    except Exception as e:
        logger.error(f"prem_login_post error: {e}")
        return HTMLResponse(_prem_login_html("❌ Server error, please try again"))


@app.get("/tg/premium/logout")
async def prem_logout(request: Request):
    tok = _get_prem_token(request)
    if tok and tok in _prem_sessions:
        del _prem_sessions[tok]
    r = RedirectResponse(url="/tg/premium/login", status_code=302)
    r.delete_cookie("prem_session")
    return r


# ── Main dashboard (the Mini App entry point) ──────────────────
@app.get("/tg/premium/user/tg/security", response_class=HTMLResponse)
async def prem_dashboard(request: Request):
    _rate_guard(request)
    if not _is_prem_auth(_get_prem_token(request)):
        return RedirectResponse(url="/tg/premium/login", status_code=302)
    import os
    html_path = os.path.join(os.path.dirname(__file__), "premium_dashboard.html")
    try:
        with open(html_path) as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>Dashboard file not found</h1>", status_code=500)


# ── Premium API endpoints (re-expose existing data under /tg/premium/api/) ──
# These proxy the existing data builders so the dashboard JS can call them
# without needing admin-panel session cookies.

def _prem_guard(request: Request):
    """Raise 401 if not a valid premium session."""
    if not _is_prem_auth(_get_prem_token(request)):
        raise HTTPException(status_code=401, detail="Not authenticated")


@app.get("/tg/premium/api/stats")
async def prem_api_stats(request: Request):
    _rate_guard(request)
    _prem_guard(request)
    return JSONResponse(await _build_stats())


@app.get("/tg/premium/api/clones")
async def prem_api_clones(request: Request):
    _rate_guard(request)
    _prem_guard(request)
    return JSONResponse(await _build_clones())


@app.get("/tg/premium/api/giveaways")
async def prem_api_giveaways(request: Request):
    _rate_guard(request)
    _prem_guard(request)
    return JSONResponse(await _build_giveaways())


@app.get("/tg/premium/api/users")
async def prem_api_users(request: Request, page: int = 1, search: str = ""):
    _rate_guard(request)
    _prem_guard(request)
    return JSONResponse(await _build_users(page=page, search=search))


@app.get("/tg/premium/api/live_users")
async def prem_api_live_users(request: Request):
    _rate_guard(request)
    _prem_guard(request)
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


@app.get("/tg/premium/api/clone_users/{token}")
async def prem_api_clone_users(token: str, request: Request):
    _rate_guard(request)
    _prem_guard(request)
    from utils.db import get_db, is_mongo, get_sqlite_path
    users = []
    try:
        if is_mongo():
            db = get_db()
            docs = await db.referrals.find({"clone_token": token}).sort("refer_count", -1).to_list(None)
            users = [{"user_id": d.get("user_id"), "user_name": d.get("user_name") or "—",
                      "refer_count": d.get("refer_count", 0), "joined_at": str(d.get("joined_at", ""))[:10]}
                     for d in docs]
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(
                    "SELECT user_id, user_name, refer_count, joined_at FROM referrals "
                    "WHERE clone_token=? ORDER BY refer_count DESC", (token,)
                ) as cur:
                    rows = await cur.fetchall()
            users = [{"user_id": r["user_id"], "user_name": r["user_name"] or "—",
                      "refer_count": r["refer_count"] or 0, "joined_at": str(r["joined_at"] or "")[:10]}
                     for r in rows]
    except Exception as e:
        logger.error(f"prem clone_users error: {e}")
        raise HTTPException(500, detail=str(e))
    return JSONResponse({"token": token, "users": users, "total": len(users)})


@app.get("/tg/premium/api/panels")
async def prem_api_panels(request: Request):
    _rate_guard(request)
    _prem_guard(request)
    return JSONResponse(await _build_panels())


# ── Add premium panel user (called from Telegram bot admin command) ──
async def add_premium_panel_user(tg_id: int, username: str, password: str) -> bool:
    """
    Store a premium dashboard user in premium_panel_users table.
    Called by the /addpreuser bot command handler in handlers/admin.py
    """
    try:
        from utils.db import get_db, is_mongo, get_sqlite_path
        hashed = _hash_prem_pw(password)
        if is_mongo():
            db = get_db()
            await db.premium_panel_users.update_one(
                {"username": username},
                {"$set": {"tg_id": tg_id, "username": username, "password": hashed}},
                upsert=True,
            )
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                await conn.execute(
                    "CREATE TABLE IF NOT EXISTS premium_panel_users "
                    "(id INTEGER PRIMARY KEY, tg_id INTEGER, username TEXT UNIQUE, password TEXT)"
                )
                await conn.execute(
                    "INSERT OR REPLACE INTO premium_panel_users (tg_id, username, password) VALUES (?,?,?)",
                    (tg_id, username, hashed)
                )
                await conn.commit()
        logger.info(f"✅ Premium panel user added: @{username} (tg_id={tg_id})")
        return True
    except Exception as e:
        logger.error(f"add_premium_panel_user error: {e}")
        return False


async def remove_premium_panel_user(username: str) -> bool:
    """Remove a premium dashboard user. Called by /removepreuser bot command."""
    try:
        from utils.db import get_db, is_mongo, get_sqlite_path
        if is_mongo():
            result = await get_db().premium_panel_users.delete_one({"username": username})
            return result.deleted_count > 0
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                cur = await conn.execute(
                    "DELETE FROM premium_panel_users WHERE username=?", (username,)
                )
                await conn.commit()
                return cur.rowcount > 0
    except Exception as e:
        logger.error(f"remove_premium_panel_user error: {e}")
        return False
