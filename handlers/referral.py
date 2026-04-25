from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from utils.db import get_db, is_mongo, get_sqlite_path
import json

router = Router()


@router.message(Command("mygiveaways"))
@router.callback_query(F.data == "menu:my_giveaways")
async def my_giveaways(event):
    msg = event if isinstance(event, Message) else event.message
    if isinstance(event, CallbackQuery):
        await event.answer()

    user_id = event.from_user.id
    active_giveaways = []
    archived_giveaways = []
    clone_bot = None

    # ── Active/closed giveaway polls (live DB) ────────────────
    if is_mongo():
        db = get_db()
        cursor = db.giveaways.find({"creator_id": user_id}).sort("created_at", -1).limit(10)
        active_giveaways = await cursor.to_list(length=10)
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT * FROM giveaways WHERE creator_id=? ORDER BY created_at DESC LIMIT 10",
                (user_id,)
            ) as cur:
                rows = await cur.fetchall()
        for r in rows:
            d = dict(r)
            d["prizes"] = json.loads(d["prizes"])
            d["options"] = json.loads(d["options"])
            d["votes"] = json.loads(d["votes"])
            d["is_active"] = bool(d["is_active"])
            active_giveaways.append(d)

    # ── Archived giveaways (purged from live DB) ──────────────
    try:
        if is_mongo():
            db = get_db()
            cursor = db.giveaway_archive_refs.find(
                {"creator_id": user_id}
            ).sort("archived_at", -1).limit(5)
            archived_giveaways = await cursor.to_list(length=5)
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                try:
                    async with conn.execute(
                        """SELECT giveaway_id, title, total_votes, archived_at
                           FROM giveaway_archive_refs
                           WHERE creator_id=?
                           ORDER BY archived_at DESC LIMIT 5""",
                        (user_id,)
                    ) as cur:
                        archived_giveaways = [dict(r) for r in await cur.fetchall()]
                except Exception:
                    archived_giveaways = []
    except Exception:
        archived_giveaways = []

    # ── Clone bot info ────────────────────────────────────────
    try:
        from models.referral import get_clone_bot_by_owner
        clone_bot = await get_clone_bot_by_owner(user_id)
    except Exception:
        clone_bot = None

    # ── Build response ────────────────────────────────────────
    has_anything = active_giveaways or archived_giveaways or clone_bot
    if not has_anything:
        await msg.answer(
            "📋 <b>My Dashboard</b>\n\n"
            "You haven't created anything yet.\n\n"
            "• Use /creategiveaway to create a poll\n"
            "• Use /clonebot to set up a referral bot",
            parse_mode="HTML"
        )
        return

    lines = ["📋 <b>My Dashboard</b>\n"]

    # Clone bot section
    if clone_bot:
        username = clone_bot.get("bot_username", "unknown")
        channel = clone_bot.get("channel_link", "—") or "—"
        # Get referral user count
        try:
            if is_mongo():
                db = get_db()
                user_count = await db.referrals.count_documents({"clone_token": clone_bot["token"]})
            else:
                import aiosqlite
                async with aiosqlite.connect(get_sqlite_path()) as conn:
                    async with conn.execute(
                        "SELECT COUNT(*) FROM referrals WHERE clone_token=?",
                        (clone_bot["token"],)
                    ) as cur:
                        user_count = (await cur.fetchone())[0]
        except Exception:
            user_count = 0

        lines.append(
            f"🤖 <b>Your Clone Bot</b>\n"
            f"  @{username}\n"
            f"  👥 {user_count} referral users\n"
            f"  📢 Channel: {channel}\n"
            f"  Use /deleteclone to remove"
        )

    # Active/closed polls
    if active_giveaways:
        lines.append("\n🗳 <b>Giveaway Polls</b>")
        for g in active_giveaways:
            status = "✅ Active" if g["is_active"] else "🔒 Closed"
            lines.append(
                f"• <code>{g['giveaway_id']}</code> — {g['title']}\n"
                f"  {status} | 👥 {g['total_votes']} votes"
            )

    # Archived polls
    if archived_giveaways:
        lines.append("\n📦 <b>Archived Polls</b>")
        for g in archived_giveaways:
            lines.append(
                f"• <code>{g['giveaway_id']}</code> — {g.get('title', '—')}\n"
                f"  🗃 Archived | 👥 {g.get('total_votes', 0)} votes\n"
                f"  /getgiveaway <code>{g['giveaway_id']}</code>"
            )

    await msg.answer("\n".join(lines), parse_mode="HTML")


@router.callback_query(F.data == "menu:help")
async def menu_help(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "📖 <b>Help Guide</b>\n\n"
        "<b>Giveaway Poll:</b>\n"
        "1. Add this bot as admin in your channel\n"
        "2. Use /creategiveaway and follow the steps\n"
        "3. The poll posts live with vote bars\n"
        "4. Users must join your channel to vote\n\n"
        "<b>Clone Refer Bot:</b>\n"
        "1. Create a bot via @BotFather\n"
        "2. Use /clonebot and paste your token\n"
        "3. Share your bot — it tracks referrals!\n\n"
        "<b>Commands:</b>\n"
        "/creategiveaway — New giveaway poll\n"
        "/mygiveaways — Your dashboard\n"
        "/clonebot — Set up referral bot\n"
        "/deleteclone — Remove your clone bot",
        parse_mode="HTML"
    )
