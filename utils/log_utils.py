# utils/log_utils.py
"""
utils/log_utils.py

All LOG_CHANNEL messages are sent via the MAIN BOT only.
Clone bots pass their data here, but the actual send always
uses the main bot instance stored with set_main_bot().
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from aiogram import Bot
from config.settings import settings

logger = logging.getLogger(__name__)

# ── Main bot reference (set once at startup in main.py) ───────
_main_bot: Bot | None = None


def set_main_bot(bot: Bot) -> None:
    global _main_bot
    _main_bot = bot


def get_main_bot() -> Bot | None:
    return _main_bot


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ─── New user report ──────────────────────────────────────────

async def log_new_user(
    user_id: int,
    first_name: str,
    last_name: str | None,
    username: str | None,
    dc_id: int | None,
    reported_by: str,        # always the main bot name e.g. "@MainBot"
) -> None:
    """Send a new-user report to LOG_CHANNEL via the MAIN BOT only."""
    if not settings.LOG_CHANNEL or not _main_bot:
        return

    now   = _now_utc()
    uname = f"@{username}" if username else "—"
    lname = last_name or "—"
    dc    = str(dc_id) if dc_id else "—"

    text = (
        "👤 <b>New User Joined</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"📛 <b>Name :</b> {first_name}\n"
        f"🔤 <b>Last Name :</b> {lname}\n"
        f"🔗 <b>Username :</b> {uname}\n"
        f"🌐 <b>DC :</b> {dc}\n"
        f"📅 <b>Date :</b> {now.strftime('%Y-%m-%d')}\n"
        f"🕐 <b>Time :</b> {now.strftime('%H:%M:%S')} UTC\n"
        f"🤖 <b>By :</b> {reported_by}"
    )
    try:
        await _main_bot.send_message(settings.LOG_CHANNEL, text, parse_mode="HTML")
    except Exception as e:
        logger.warning(f"log_new_user: failed to send to LOG_CHANNEL: {e}")

    # Save user to main_bot_users for panel/broadcast
    try:
        from utils.db import get_db, is_mongo, get_sqlite_path
        from datetime import datetime, timezone
        joined = datetime.now(timezone.utc).replace(tzinfo=None)
        if is_mongo():
            db = get_db()
            await db.main_bot_users.update_one(
                {"user_id": user_id},
                {"$set": {"user_id": user_id, "first_name": first_name,
                          "last_name": last_name, "username": username, "is_banned": False},
                 "$setOnInsert": {"joined_at": joined}},
                upsert=True
            )
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                await conn.execute(
                    """CREATE TABLE IF NOT EXISTS main_bot_users
                       (user_id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT,
                        username TEXT, is_banned INTEGER DEFAULT 0, joined_at TEXT)""")
                await conn.execute(
                    """INSERT INTO main_bot_users (user_id,first_name,last_name,username,is_banned,joined_at)
                       VALUES (?,?,?,?,0,?)
                       ON CONFLICT(user_id) DO UPDATE SET
                       first_name=excluded.first_name, last_name=excluded.last_name,
                       username=excluded.username""",
                    (user_id, first_name, last_name, username, str(joined))
                )
                await conn.commit()
    except Exception as e:
        logger.warning(f"log_new_user: failed to save to main_bot_users: {e}")


# ─── New clone-bot report ─────────────────────────────────────

async def log_new_bot(
    owner_first_name: str,
    owner_last_name: str | None,
    bot_username: str,
    dc_id: int | None,
    reported_by: str,        # always the main bot name
) -> None:
    """Send a new-clone-bot report to LOG_CHANNEL via the MAIN BOT only."""
    if not settings.LOG_CHANNEL or not _main_bot:
        return

    lname = owner_last_name or "—"
    dc    = str(dc_id) if dc_id else "—"

    text = (
        "🤖 <b>New Bot Added</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"📛 <b>Name :</b> {owner_first_name}\n"
        f"🔤 <b>Last Name :</b> {lname}\n"
        f"🌐 <b>DC :</b> {dc}\n"
        f"🔗 <b>Bot Username +clone+ :</b> @{bot_username}\n"
        f"🤖 <b>By :</b> {reported_by}"
    )
    try:
        await _main_bot.send_message(settings.LOG_CHANNEL, text, parse_mode="HTML")
    except Exception as e:
        logger.warning(f"log_new_bot: failed to send to LOG_CHANNEL: {e}")


# ─── Force-join check ─────────────────────────────────────────

async def check_force_join(bot: Bot, user_id: int) -> tuple[bool, str | None]:
    """
    Returns (is_member, channel_link).
    Only applies to the MAIN bot (FORCE_JOIN_CHANNEL setting).
    Clone bots manage their own channel_link separately.
    """
    channel = settings.FORCE_JOIN_CHANNEL
    if not channel:
        return True, None

    if channel.startswith("https://t.me/"):
        link   = channel
        lookup = "@" + channel.replace("https://t.me/", "")
    elif channel.startswith("@"):
        link   = f"https://t.me/{channel[1:]}"
        lookup = channel
    else:
        link   = f"https://t.me/{channel}"
        lookup = f"@{channel}"

    try:
        member = await bot.get_chat_member(lookup, user_id)
        if member.status in ("left", "kicked"):
            return False, link
        return True, link
    except Exception:
        return True, link  # can't check → don't block
