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
        f"<b>Bot Commands:</b>\n"
        f"/listclones — All clone bots\n"
        f"/banclone <token> — Ban a clone\n"
        f"/globalbroadcast <msg> — Broadcast to all\n"
        f"/banuser <user_id> — Ban a user\n\n"
        f"<b>Panel Commands:</b>\n"
        f"/addadmin user:pass — Create panel login\n"
        f"/removeadmin user — Remove panel login\n\n"
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


@router.message(Command("addpremium"))
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
    expires_at = await add_premium(user_id, days, granted_by=message.from_user.id)
    await message.answer(
        f"⭐ <b>Premium granted!</b>\n\n"
        f"👤 User: <code>{user_id}</code>\n"
        f"📅 Duration: <b>{days} days</b>\n"
        f"⏳ Expires: <b>{expires_at.strftime('%Y-%m-%d')}</b>\n\n"
        f"✅ Bot stamp removed from their giveaways & messages.",
        parse_mode="HTML"
    )


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
