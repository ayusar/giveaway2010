import asyncio
import hashlib
from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.types import Message
from config.settings import settings
from models.referral import ban_clone_bot, get_all_clone_bots
from utils.clone_manager import get_clone_manager
from utils.db import get_db, is_mongo, get_sqlite_path

router = Router()


def _hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


def is_superadmin(user_id: int) -> bool:
    import logging
    _log = logging.getLogger(__name__)
    _log.warning(f"[SUPERADMIN CHECK] user_id={user_id} SUPERADMIN_IDS={settings.SUPERADMIN_IDS} result={user_id in settings.SUPERADMIN_IDS}")
    return user_id in settings.SUPERADMIN_IDS


# ─── Panel user management ────────────────────────────────────

@router.message(Command("addadmin"))
async def add_admin_user(message: Message):
    if not is_superadmin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or ":" not in parts[1]:
        await message.answer(
            "Usage: /addadmin <username>:<password>\n"
            "Example: /addadmin <code>myadmin:mypassword123</code>",
            parse_mode="HTML"
        )
        return
    username, password = parts[1].strip().split(":", 1)
    hashed = _hash_pw(password)
    if is_mongo():
        db = get_db()
        await db.panel_users.update_one(
            {"username": username},
            {"$set": {"username": username, "password": hashed}},
            upsert=True
        )
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS panel_users "
                "(id INTEGER PRIMARY KEY, username TEXT UNIQUE, password TEXT)"
            )
            await conn.execute(
                "INSERT OR REPLACE INTO panel_users (username, password) VALUES (?,?)",
                (username, hashed)
            )
            await conn.commit()
    await message.answer(
        f"✅ Admin panel user created!\n\n"
        f"👤 Username: <code>{username}</code>\n"
        f"🔗 Login at: <code>/adminpanel/royalisbest/a?b3c</code>",
        parse_mode="HTML"
    )


@router.message(Command("removeadmin"))
async def remove_admin_user(message: Message):
    if not is_superadmin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: /removeadmin <username>")
        return
    username = parts[1].strip()
    if is_mongo():
        await get_db().panel_users.delete_one({"username": username})
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            await conn.execute("DELETE FROM panel_users WHERE username=?", (username,))
            await conn.commit()
    await message.answer(f"✅ Admin user <code>{username}</code> removed.", parse_mode="HTML")


# ─── Superadmin overview ──────────────────────────────────────

@router.message(Command("admin"))
async def admin_panel(message: Message):
    if not is_superadmin(message.from_user.id):
        await message.answer(f"❌ Not superadmin. Your ID: {message.from_user.id} | Allowed: {settings.SUPERADMIN_IDS}")
        return
    mongo = is_mongo()
    if mongo:
        db = get_db()
        total_clones     = await db.clone_bots.count_documents({"is_active": True})
        total_users      = await db.referrals.count_documents({})
        total_giveaways  = await db.giveaways.count_documents({})
        active_giveaways = await db.giveaways.count_documents({"is_active": True})
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            async def count(q):
                async with conn.execute(q) as c:
                    return (await c.fetchone())[0]
            total_clones     = await count("SELECT COUNT(*) FROM clone_bots WHERE is_active=1")
            total_users      = await count("SELECT COUNT(*) FROM referrals")
            total_giveaways  = await count("SELECT COUNT(*) FROM giveaways")
            active_giveaways = await count("SELECT COUNT(*) FROM giveaways WHERE is_active=1")

    await message.answer(
        f"🛠 <b>Superadmin Panel</b>\n\n"
        f"📊 <b>Stats:</b>\n"
        f"• Active clone bots: {total_clones}\n"
        f"• Total referral users: {total_users}\n"
        f"• Total giveaways: {total_giveaways}\n"
        f"• Active giveaways: {active_giveaways}\n\n"
        f"<b>👥 User Commands:</b>\n"
        f"/banuser &lt;user_id&gt; — Ban a user\n"
        f"/unbanuser &lt;user_id&gt; — Unban a user\n\n"
        f"<b>⭐ Premium Commands:</b>\n"
        f"/addpremium &lt;user_id&gt; &lt;days&gt; — Grant premium\n"
        f"/removepremium &lt;user_id&gt; — Revoke premium\n"
        f"/extendpremium &lt;user_id&gt; &lt;days&gt; — Extend expiry\n"
        f"/checkpremium &lt;user_id&gt; — Check status\n"
        f"/addpreuserpanel &lt;id&gt;:&lt;user&gt;:&lt;pass&gt; — Grant premium + panel\n\n"
        f"<b>🤖 Bot Commands:</b>\n"
        f"/listclones — All clone bots\n"
        f"/banclone &lt;token&gt; — Ban a clone\n"
        f"/globalbroadcast &lt;msg&gt; — Broadcast to all\n\n"
        f"<b>🔧 Panel Commands:</b>\n"
        f"/addadmin user:pass — Create admin panel login\n"
        f"/removeadmin user — Remove admin panel login\n"
        f"/addpreuser &lt;id&gt;:&lt;user&gt;:&lt;pass&gt; — Create premium panel user\n"
        f"/removepreuser &lt;user&gt; — Remove premium panel user\n"
        f"/listpreusers — List all premium panel users\n\n"
        f"🌐 Panel: <code>/adminpanel/royalisbest/a?b3c</code>",
        parse_mode="HTML"
    )


@router.message(Command("listclones"))
async def list_clones(message: Message):
    if not is_superadmin(message.from_user.id):
        return
    clones = await get_all_clone_bots()
    if not clones:
        await message.answer("No active clone bots.")
        return
    lines = [f"🤖 <b>Active Clone Bots ({len(clones)})</b>\n"]
    for c in clones:
        lines.append(
            f"• @{c.get('bot_username','unknown')} | "
            f"Owner: <code>{c['owner_id']}</code> | "
            f"{'🚫 BANNED' if c.get('is_banned') else '✅ Active'}"
        )
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("banclone"))
async def ban_clone(message: Message):
    if not is_superadmin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage: /banclone <token>")
        return
    token = parts[1].strip()
    await get_clone_manager().stop_clone(token)
    await ban_clone_bot(token)
    await message.answer("✅ Clone bot banned and stopped.")


@router.message(Command("globalbroadcast"))
async def global_broadcast(message: Message, bot: Bot):
    if not is_superadmin(message.from_user.id):
        return
    text = message.text.partition(" ")[2].strip()
    if not text:
        await message.answer("Usage: /globalbroadcast <message>")
        return
    from web.broadcaster import do_global_broadcast
    status_msg = await message.answer("📤 Broadcasting...")
    await do_global_broadcast(text)
    await status_msg.edit_text("✅ Global broadcast complete!")


@router.message(Command("banuser"))
async def ban_user(message: Message):
    if not is_superadmin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: /banuser <user_id>")
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Invalid user ID.")
        return
    if is_mongo():
        await get_db().banned_users.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "banned": True}},
            upsert=True
        )
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            await conn.execute(
                "INSERT OR REPLACE INTO banned_users (user_id, banned) VALUES (?,1)", (user_id,)
            )
            await conn.commit()
    await message.answer(f"✅ User <code>{user_id}</code> banned globally.", parse_mode="HTML")


@router.message(Command("unbanuser"))
async def unban_user(message: Message):
    """Unban a user — clears both banned_users table and main_bot_users.is_banned."""
    if not is_superadmin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: /unbanuser <user_id>")
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Invalid user ID.")
        return
    if is_mongo():
        db = get_db()
        await db.main_bot_users.update_one(
            {"user_id": user_id}, {"$set": {"is_banned": False}}
        )
        await db.banned_users.delete_one({"user_id": user_id})
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
    await message.answer(
        f"✅ User <code>{user_id}</code> has been unbanned.\n"
        f"They can now use the bot again.",
        parse_mode="HTML"
    )


@router.message(Command("mypanel"))
async def my_panel(message: Message):
    """Let a user retrieve all their panel URLs."""
    user_id = message.from_user.id
    from config.settings import settings as _settings
    domain = getattr(_settings, "WEB_DOMAIN", "")
    if is_mongo():
        db = get_db()
        panels = await db.panels.find({"owner_id": user_id, "is_deleted": False}).to_list(None)
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            conn.row_factory = aiosqlite.Row
            try:
                async with conn.execute(
                    "SELECT * FROM panels WHERE owner_id=? AND is_deleted=0", (user_id,)
                ) as cur:
                    rows = await cur.fetchall()
                panels = [dict(r) for r in rows]
            except Exception:
                panels = []
    if not panels:
        await message.answer("📋 You have no active panels yet.")
        return
    lines = [f"🔗 <b>Your Panels ({len(panels)}):</b>\n"]
    for p in panels[:10]:
        token = p.get("token", "")
        title = p.get("channel_title") or p.get("channel_username") or "Panel"
        ptype = p.get("panel_type", "referral")
        url = f"{domain}/panel/{token}" if domain else f"/panel/{token}"
        lines.append(f"• <b>{title}</b> [{ptype}]\n  🔗 <code>{url}</code>")
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("extendpremium"))
async def cmd_extend_premium(message: Message):
    """Add extra days on top of current expiry: /extendpremium <user_id> <days>"""
    if not is_superadmin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 3:
        await message.answer(
            "Usage: <code>/extendpremium &lt;user_id&gt; &lt;days&gt;</code>\n"
            "Example: <code>/extendpremium 123456789 30</code>",
            parse_mode="HTML"
        )
        return
    try:
        user_id = int(parts[1])
        days = int(parts[2])
        if days <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Invalid user_id or days. Both must be positive numbers.")
        return

    from utils.db import get_db as _get_db, is_mongo as _is_mongo, get_sqlite_path as _get_sqlite
    from datetime import timedelta
    import aiosqlite as _aiosqlite

    now_dt = __import__("datetime").datetime.utcnow()

    if _is_mongo():
        db = _get_db()
        doc = await db.premium_users.find_one({"user_id": user_id})
        if doc:
            try:
                from datetime import datetime as _dt
                curr = _dt.fromisoformat(str(doc["expires_at"]).replace("Z",""))
            except Exception:
                curr = now_dt
        else:
            curr = now_dt
        base = curr if curr > now_dt else now_dt
        new_exp = base + timedelta(days=days)
        await db.premium_users.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "expires_at": new_exp.isoformat(), "granted_by": message.from_user.id}},
            upsert=True,
        )
    else:
        from datetime import datetime as _dt
        async with _aiosqlite.connect(_get_sqlite()) as conn:
            async with conn.execute("SELECT expires_at FROM premium_users WHERE user_id=?", (user_id,)) as cur:
                row = await cur.fetchone()
            if row:
                try:
                    curr = _dt.fromisoformat(row[0])
                except Exception:
                    curr = now_dt
            else:
                curr = now_dt
            base = curr if curr > now_dt else now_dt
            new_exp = base + timedelta(days=days)
            await conn.execute(
                """INSERT INTO premium_users (user_id, granted_by, expires_at, granted_at)
                   VALUES (?,?,?,?)
                   ON CONFLICT(user_id) DO UPDATE SET expires_at=excluded.expires_at""",
                (user_id, message.from_user.id, new_exp.isoformat(), now_dt.isoformat())
            )
            await conn.commit()

    await message.answer(
        f"✅ <b>Premium extended!</b>\n\n"
        f"👤 User: <code>{user_id}</code>\n"
        f"➕ Added: <b>{days} days</b>\n"
        f"⏳ New Expiry: <b>{new_exp.strftime('%Y-%m-%d')}</b>",
        parse_mode="HTML"
    )
    # Notify user
    try:
        await message.bot.send_message(
            chat_id=user_id,
            text=(
                f"🎉 <b>Your Premium has been extended!</b>\n\n"
                f"➕ Added: <b>{days} more days</b>\n"
                f"⏳ New expiry: <b>{new_exp.strftime('%Y-%m-%d')}</b>\n\n"
                f"Thank you for being a Premium member! ⭐"
            ),
            parse_mode="HTML"
        )
    except Exception:
        pass


@router.message(Command("addpreuserpanel"))
async def cmd_add_pre_user_panel(message: Message):
    """
    /addpreuserpanel <user_id>:<username>:<password>
    Grant premium + panel access in one command. Notifies user with beautiful message.
    """
    if not is_superadmin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or parts[1].count(":") < 2:
        await message.answer(
            "❌ <b>Usage:</b>\n"
            "<code>/addpreuserpanel &lt;user_id&gt;:&lt;username&gt;:&lt;password&gt;</code>\n\n"
            "📌 <b>Example:</b>\n"
            "<code>/addpreuserpanel 123456789:rajesh:mypass123</code>\n\n"
            "This grants 30 days of premium and creates their panel login.",
            parse_mode="HTML"
        )
        return

    raw = parts[1].strip()
    try:
        first_colon  = raw.index(":")
        second_colon = raw.index(":", first_colon + 1)
        user_id_str  = raw[:first_colon]
        username     = raw[first_colon + 1:second_colon]
        password     = raw[second_colon + 1:]
    except ValueError:
        await message.answer("❌ Invalid format. Use: <code>user_id:username:password</code>", parse_mode="HTML")
        return

    if not user_id_str.lstrip("-").isdigit():
        await message.answer("❌ <code>user_id</code> must be a numeric Telegram user ID.", parse_mode="HTML")
        return
    if not username or not password:
        await message.answer("❌ Username and password cannot be empty.", parse_mode="HTML")
        return

    user_id = int(user_id_str)
    DAYS = 30  # Default premium duration

    # 1. Grant premium
    from utils.premium import add_premium
    from datetime import datetime, timezone
    granted_at = datetime.now(timezone.utc)
    expires_at = await add_premium(user_id, DAYS, granted_by=message.from_user.id)

    # 2. Create panel login
    try:
        from web.app import add_premium_panel_user
        await add_premium_panel_user(user_id, username, password)
    except Exception:
        # Fallback: write directly to DB
        from utils.db import get_db as _gdb, is_mongo as _im, get_sqlite_path as _gsp
        hashed = hashlib.sha256(password.encode()).hexdigest()
        try:
            if _im():
                await _gdb().premium_panel_users.update_one(
                    {"username": username},
                    {"$set": {"tg_id": user_id, "username": username, "password": hashed}},
                    upsert=True,
                )
            else:
                import aiosqlite
                async with aiosqlite.connect(_gsp()) as conn:
                    await conn.execute(
                        "CREATE TABLE IF NOT EXISTS premium_panel_users "
                        "(id INTEGER PRIMARY KEY, tg_id INTEGER, username TEXT UNIQUE, password TEXT)"
                    )
                    await conn.execute(
                        "INSERT OR REPLACE INTO premium_panel_users (tg_id, username, password) VALUES (?,?,?)",
                        (user_id, username, hashed)
                    )
                    await conn.commit()
        except Exception as e:
            await message.answer(f"⚠️ Premium granted but panel account creation failed: <code>{e}</code>", parse_mode="HTML")
            return

    # 3. Notify admin
    await message.answer(
        f"✅ <b>Premium + Panel Access Granted!</b>\n\n"
        f"👤 User ID: <code>{user_id}</code>\n"
        f"🔑 Username: <code>{username}</code>\n"
        f"📅 Duration: <b>{DAYS} days</b>\n"
        f"⏳ Expires: <b>{expires_at.strftime('%Y-%m-%d')}</b>\n\n"
        f"📱 User has been notified.",
        parse_mode="HTML"
    )

    # 4. Notify the user with a beautiful message
    from config.settings import settings as _settings
    domain = getattr(_settings, "WEB_DOMAIN", "")
    panel_url = f"{domain}/tg/premium/login" if domain else "/tg/premium/login"

    benefits = (
        "✦ Direct user panel access\n"
        "✦ No watermark on your giveaways\n"
        "✦ Unlimited giveaways at once\n"
        "✦ Priority support\n"
        "✦ Advanced analytics dashboard"
    )

    try:
        await message.bot.send_message(
            chat_id=user_id,
            text=(
                "╔══════════════════════╗\n"
                "║  🎉 CONGRATULATIONS!  ║\n"
                "╚══════════════════════╝\n\n"
                "You are now a <b>Premium Member</b>! 🌟\n\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 <b>Username</b> : <code>{username}</code>\n"
                f"🔑 <b>Password</b>  : <code>{password}</code>\n"
                f"🌐 <b>Panel URL</b>  : {panel_url}\n"
                "━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📅 <b>Date</b>   : <b>{granted_at.strftime('%Y-%m-%d')}</b>\n"
                f"⏳ <b>Till</b>    : <b>{expires_at.strftime('%Y-%m-%d')}</b>\n\n"
                "🎁 <b>Your Premium Benefits:</b>\n"
                f"{benefits}\n\n"
                "Thank you for being part of our premium family! 💎\n"
                "Use /help to see all your premium commands."
            ),
            parse_mode="HTML"
        )
    except Exception:
        await message.answer(
            "⚠️ Premium granted but could not DM the user (they may have blocked the bot).",
            parse_mode="HTML"
        )



async def cmd_add_premium(message: Message):
    if not is_superadmin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 3:
        await message.answer(
            "Usage: <code>/addpremium &lt;user_id&gt; &lt;days&gt;</code>\n"
            "Example: <code>/addpremium 123456789 30</code>",
            parse_mode="HTML"
        )
        return
    try:
        user_id = int(parts[1])
        days    = int(parts[2])
        if days <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Invalid user_id or days. Both must be positive numbers.")
        return

    from utils.premium import add_premium
    from datetime import datetime, timezone
    granted_at = datetime.now(timezone.utc)
    expires_at = await add_premium(user_id, days, granted_by=message.from_user.id)

    # ── Notify the admin ──────────────────────────────────────
    await message.answer(
        f"⭐ <b>Premium granted!</b>\n\n"
        f"👤 User: <code>{user_id}</code>\n"
        f"📅 Duration: <b>{days} days</b>\n"
        f"⏳ Expires: <b>{expires_at.strftime('%Y-%m-%d')}</b>\n\n"
        f"✅ Bot stamp removed from their giveaways & messages.",
        parse_mode="HTML"
    )

    # ── Notify the user with their premium details ────────────
    try:
        # Try to get panel credentials if available
        panel_username = None
        panel_password = None
        try:
            if is_mongo():
                doc = await get_db().panel_users.find_one({"user_id": user_id})
                if doc:
                    panel_username = doc.get("username")
                    panel_password = "****"  # never expose real password
            else:
                import aiosqlite
                async with aiosqlite.connect(get_sqlite_path()) as conn:
                    conn.row_factory = aiosqlite.Row
                    async with conn.execute(
                        "SELECT username FROM panel_users WHERE id=?", (user_id,)
                    ) as cur:
                        row = await cur.fetchone()
                    if row:
                        panel_username = row["username"]
                        panel_password = "****"
        except Exception:
            pass

        creds_block = ""
        if panel_username:
            creds_block = (
                f"\n👤 Username : <code>{panel_username}</code>\n"
                f"🔑 Password : <code>{panel_password}</code>\n"
                f"🌐 To access user panel\n"
            )

        await message.bot.send_message(
            chat_id=user_id,
            text=(
                "🎉 <b>Congratulations!</b> Now you are a Premium Member\n\n"
                f"{creds_block}"
                f"\n📅 Date    : <b>{granted_at.strftime('%Y-%m-%d')}</b>\n"
                f"⏳ Till      : <b>{expires_at.strftime('%Y-%m-%d')}</b>\n\n"
                "✨ Enjoy your premium benefits:\n"
                "• No bot stamp on your giveaways\n"
                "• Unlimited giveaways at once\n"
                "• Priority support"
            ),
            parse_mode="HTML"
        )
    except Exception:
        pass  # User may have blocked the bot


@router.message(Command("removepremium"))
async def cmd_remove_premium(message: Message):
    if not is_superadmin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: <code>/removepremium &lt;user_id&gt;</code>", parse_mode="HTML")
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Invalid user ID.")
        return

    from utils.premium import remove_premium
    removed = await remove_premium(user_id)
    if removed:
        await message.answer(
            f"✅ Premium removed for <code>{user_id}</code>.\n"
            f"Stamp will appear on their future giveaways & messages.",
            parse_mode="HTML"
        )
    else:
        await message.answer(f"❌ User <code>{user_id}</code> had no premium.", parse_mode="HTML")


@router.message(Command("checkpremium"))
async def cmd_check_premium(message: Message):
    if not is_superadmin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: <code>/checkpremium &lt;user_id&gt;</code>", parse_mode="HTML")
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Invalid user ID.")
        return

    from utils.premium import get_premium_info, is_premium
    info   = await get_premium_info(user_id)
    active = await is_premium(user_id)
    if not info:
        await message.answer(f"❌ User <code>{user_id}</code> has no premium.", parse_mode="HTML")
        return

    expires = info.get("expires_at", "?")[:10]
    status  = "⭐ Active" if active else "❌ Expired"
    await message.answer(
        f"<b>Premium Status</b>\n\n"
        f"👤 User: <code>{user_id}</code>\n"
        f"Status: {status}\n"
        f"⏳ Expires: <b>{expires}</b>\n"
        f"📅 Granted: {info.get('granted_at','?')[:10]}",
        parse_mode="HTML"
    )
@router.message(Command("addpreuser"))
async def add_prem_user(message: Message):
    """
    /addpreuser <tg_id>:<username>:<password>
    Grants a Telegram user access to the Premium Dashboard at /tg/premium/
    """
    if not is_superadmin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or parts[1].count(":") < 2:
        await message.answer(
            "❌ <b>Usage:</b> <code>/addpreuser &lt;tg_id&gt;:&lt;username&gt;:&lt;password&gt;</code>\n\n"
            "📌 <b>Example:</b>\n"
            "<code>/addpreuser 123456789:rajesh:mypass123</code>\n\n"
            "🔗 User can then log in at:\n"
            "<code>/tg/premium/login</code>",
            parse_mode="HTML"
        )
        return

    raw = parts[1].strip()
    # Split on first two colons only (password may contain colons)
    first_colon  = raw.index(":")
    second_colon = raw.index(":", first_colon + 1)
    tg_id_str = raw[:first_colon]
    username  = raw[first_colon + 1:second_colon]
    password  = raw[second_colon + 1:]

    if not tg_id_str.lstrip("-").isdigit():
        await message.answer("❌ <code>tg_id</code> must be a numeric Telegram user ID.", parse_mode="HTML")
        return
    if not username or not password:
        await message.answer("❌ Username and password cannot be empty.", parse_mode="HTML")
        return

    tg_id = int(tg_id_str)

    # Import the helper from web.app (works at runtime)
    try:
        from web.app import add_premium_panel_user
        ok = await add_premium_panel_user(tg_id, username, password)
    except ImportError:
        # Fallback: write directly to DB
        import hashlib
        from utils.db import get_db, is_mongo, get_sqlite_path
        hashed = hashlib.sha256(password.encode()).hexdigest()
        ok = False
        try:
            if is_mongo():
                await get_db().premium_panel_users.update_one(
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
            ok = True
        except Exception as e:
            await message.answer(f"❌ DB error: <code>{e}</code>", parse_mode="HTML")
            return

    if ok:
        await message.answer(
            f"✅ <b>Premium dashboard user created!</b>\n\n"
            f"👤 <b>TG ID:</b> <code>{tg_id}</code>\n"
            f"🔑 <b>Username:</b> <code>{username}</code>\n"
            f"🔒 <b>Password:</b> <code>{password}</code>\n\n"
            f"🔗 <b>Login URL:</b>\n"
            f"<code>/tg/premium/login</code>\n\n"
            f"📱 <b>Dashboard URL:</b>\n"
            f"<code>/tg/premium/user/tg/security</code>",
            parse_mode="HTML"
        )
    else:
        await message.answer("❌ Failed to create premium user. Check logs.", parse_mode="HTML")


@router.message(Command("removepreuser"))
async def remove_prem_user(message: Message):
    """
    /removepreuser <username>
    Revokes premium dashboard access for a user.
    """
    if not is_superadmin(message.from_user.id):
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.answer(
            "❌ <b>Usage:</b> <code>/removepreuser &lt;username&gt;</code>",
            parse_mode="HTML"
        )
        return

    username = parts[1].strip()

    try:
        from web.app import remove_premium_panel_user
        removed = await remove_premium_panel_user(username)
    except ImportError:
        from utils.db import get_db, is_mongo, get_sqlite_path
        removed = False
        try:
            if is_mongo():
                result = await get_db().premium_panel_users.delete_one({"username": username})
                removed = result.deleted_count > 0
            else:
                import aiosqlite
                async with aiosqlite.connect(get_sqlite_path()) as conn:
                    cur = await conn.execute(
                        "DELETE FROM premium_panel_users WHERE username=?", (username,)
                    )
                    await conn.commit()
                    removed = cur.rowcount > 0
        except Exception as e:
            await message.answer(f"❌ DB error: <code>{e}</code>", parse_mode="HTML")
            return

    if removed:
        await message.answer(
            f"✅ Premium user <code>{username}</code> removed.\n"
            f"They can no longer log in to the dashboard.",
            parse_mode="HTML"
        )
    else:
        await message.answer(
            f"⚠️ User <code>{username}</code> not found in premium users.",
            parse_mode="HTML"
        )


@router.message(Command("listpreusers"))
async def list_prem_users(message: Message):
    """
    /listpreusers
    Lists all premium dashboard users.
    """
    if not is_superadmin(message.from_user.id):
        return

    try:
        from utils.db import get_db, is_mongo, get_sqlite_path
        users = []
        if is_mongo():
            docs = await get_db().premium_panel_users.find({}).to_list(None)
            users = [{"tg_id": d.get("tg_id", "—"), "username": d.get("username", "—")} for d in docs]
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                try:
                    conn.row_factory = aiosqlite.Row
                    async with conn.execute(
                        "SELECT tg_id, username FROM premium_panel_users ORDER BY id DESC"
                    ) as cur:
                        rows = await cur.fetchall()
                    users = [{"tg_id": r["tg_id"], "username": r["username"]} for r in rows]
                except Exception:
                    users = []

        if not users:
            await message.answer("📋 No premium dashboard users yet.\nUse /addpreuser to add one.", parse_mode="HTML")
            return

        lines = [f"👥 <b>Premium Dashboard Users ({len(users)}):</b>\n"]
        for u in users:
            lines.append(f"• <code>{u['username']}</code> (TG: <code>{u['tg_id']}</code>)")

        await message.answer("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        await message.answer(f"❌ Error: <code>{e}</code>", parse_mode="HTML")

