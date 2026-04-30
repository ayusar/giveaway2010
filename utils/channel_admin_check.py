"""
utils/channel_admin_check.py

IMPORTANT: Never call bot.get_chat() — it crashes on channels with paid reactions
in aiogram 3.7.0. Use get_chat_administrators() only, which is safe.
"""
import logging
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

logger = logging.getLogger(__name__)

_ADMIN_STATUSES = {"creator", "administrator"}


async def verify_channel_admin(
    bot: Bot,
    user_id: int,
    channel: str,
) -> tuple[bool, str, str, str]:
    """
    Returns: (ok, error_msg, chat_id, chat_title)
    - ok=True  → user is verified admin, chat_id and chat_title are populated
    - ok=False → error_msg explains why, chat_id and chat_title are empty
    """
    # ── Fetch admin list — safe, does not deserialize reactions ──
    try:
        admins = await bot.get_chat_administrators(channel)
    except TelegramForbiddenError:
        return False, (
            "❌ Bot is not an admin in this channel.\n\n"
            "Please add this bot as Admin to your channel first, then try again."
        ), "", ""
    except TelegramBadRequest as e:
        if "chat not found" in str(e).lower():
            return False, f"❌ Channel not found: {channel}\n\nCheck the username and try again.", "", ""
        return False, f"❌ Could not access channel {channel}\n\nError: {str(e)}", "", ""
    except Exception as e:
        logger.warning(f"verify_channel_admin: get_chat_administrators failed: {e}")
        return False, f"❌ Error checking channel: {str(e)}", "", ""

    # Get chat_id and chat_title from the admin list chat info
    # admins[0].chat is not available, but we can use the channel identifier
    # and get title from the first admin's info — actually use raw API via bot
    chat_id = channel
    chat_title = channel

    # Try getting just the chat id/title via a raw Telegram API call
    # that doesn't fail on paid reactions: sendChatAction is lightweight
    # Actually the safest way: parse from get_chat_administrators response
    # The ChatMemberOwner has no chat attr, but we already have channel string.
    # Use channel as chat_id (works for @username and numeric id both).

    # ── Check user is admin ──────────────────────────────────
    user_entry = next((m for m in admins if m.user.id == user_id), None)
    if user_entry is None or user_entry.status not in _ADMIN_STATUSES:
        return False, (
            "❌ You are not an admin of this channel.\n\n"
            "Only owners and admins can create giveaways.\n"
            "Ask the channel owner to make you an admin first."
        ), "", ""

    # ── Check bot is admin ───────────────────────────────────
    try:
        me = await bot.get_me()
        bot_entry = next((m for m in admins if m.user.id == me.id), None)
        if bot_entry is None or bot_entry.status not in _ADMIN_STATUSES:
            return False, (
                "❌ This bot is not an admin in your channel.\n\n"
                "Please add this bot as Admin in channel settings, then try again."
            ), "", ""
    except Exception as e:
        logger.warning(f"verify_channel_admin: bot self-check failed: {e}")

    logger.info(f"verify_channel_admin: user={user_id} verified in {channel}")
    return True, "", chat_id, chat_title
