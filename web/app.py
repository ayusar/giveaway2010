"""
Web server — FastAPI
Admin panel: /adminpanel/royalisbest/a?b3c
User panel:  /panel/<token>
"""
import time, secrets, hashlib, json
from datetime import datetime, timedelta
from typing import Optional
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
import asyncio

app = FastAPI(docs_url=None, redoc_url=None)

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


@app.get(f"/adminpanel/{PANEL_SECRET}/api/giveaways")
async def api_giveaways(request: Request):
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    return JSONResponse(await _build_giveaways())


@app.get(f"/adminpanel/{PANEL_SECRET}/api/panels")
async def api_panels(request: Request):
    if not _is_auth(_get_token(request)): raise HTTPException(401)
    return JSONResponse(await _build_panels())


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
