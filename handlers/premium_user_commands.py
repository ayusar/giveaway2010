
# ─── Premium Dashboard User Management ───────────────────────────────────────
# Add these handlers to handlers/admin.py (they use the same router and
# is_superadmin() check already defined in that file).
#
# Command format:
#   /addpreuser <tg_id>:<username>:<password>
#   /removepreuser <username>
#
# Example:
#   /addpreuser 123456789:rajesh:mypassword123
# ──────────────────────────────────────────────────────────────────────────────

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
