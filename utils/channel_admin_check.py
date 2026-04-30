"""
utils/channel_admin_chek.py

Utility to verify that a Telegram user is an admin (or owner) of a channel
before letting them create a giveaway poll or referral bot for that channel.

Usage:
    ok, err_msg = await verify_channel_admin(bot, user_id, channel_identifier)
    if not ok:
        await message.answer(err_msg, parse_mode="HTML")
        return

FIX NOTE:
    The original code used bot.get_chat_member(chat_id, user_id) to check admin
    status. This ALWAYS FAILS for Telegram broadcast channels — the Bot API raises
    USER_NOT_PARTICIPANT for any user who is not an admin, because regular channel
    members are invisible to bots. The fix uses get_chat_administrators() instead,
    which returns the full admin list that we can search through.
"""
import logging
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

logger = logging.getLogger(__name__)

# Statuses that count as "admin" in Telegram
_ADMIN_STATUSES = {"creator", "administrator"}


async def verify_channel_admin(
    bot: Bot,
    user_id: int,
    channel: str,
) -> tuple[bool, str]:
    """
    Check that `user_id` is an admin or owner of `channel`.

    Returns:
        (True, "")           — user is verified admin
        (False, error_text)  — not an admin, error_text has the rejection message
    """
    # ── 1. Resolve the chat object ──────────────────────────
    try:
        chat = await bot.get_chat(channel)
        chat_id = chat.id
        chat_title = chat.title or channel
    except TelegramForbiddenError:
        return False, (
            "❌ <b>Bot not in channel</b>\n\n"
            f"The bot hasn't been added to <code>{channel}</code> yet.\n\n"
            "Please:\n"
            "1. Add this bot as an <b>Admin</b> to your channel\n"
            "2. Then try again with /creategiveaway"
        )
    except TelegramBadRequest as e:
        if "chat not found" in str(e).lower():
            return False, (
                f"❌ <b>Channel not found:</b> <code>{channel}</code>\n\n"
                "Make sure the username is correct (e.g. <code>@mychannel</code>)"
            )
        return False, f"❌ Could not access channel <code>{channel}</code>: <code>{e}</code>"
    except Exception as e:
        return False, f"❌ Error looking up channel: <code>{e}</code>"

    # ── 2. Must be a channel or supergroup ─────────────────
    if chat.type not in ("channel", "supergroup"):
        return False, (
            "❌ <b>Not a channel or supergroup.</b>\n\n"
            f"<code>{channel}</code> is a <b>{chat.type}</b>.\n"
            "Please enter a public channel (e.g. <code>@mychannel</code>)."
        )

    # ── 3. Fetch the admin list and search it ───────────────
    #
    # IMPORTANT: bot.get_chat_member(chat_id, user_id) is BROKEN for channels.
    # Telegram's Bot API raises USER_NOT_PARTICIPANT for any user who messages
    # the bot privately — even if they are a channel admin — because channel
    # subscribers are invisible to bots. The only reliable way to check admin
    # status is get_chat_administrators(), which returns all admins directly.
    #
    try:
        admins = await bot.get_chat_administrators(chat_id)
    except TelegramForbiddenError:
        return False, (
            "❌ <b>Bot lacks permission</b>\n\n"
            f"The bot cannot read the admin list for <code>{chat_title}</code>.\n"
            "Please make sure the bot is an <b>Admin</b> with at least "
            "<i>Manage channel</i> permission."
        )
    except Exception as e:
        logger.warning(f"verify_channel_admin: get_chat_administrators failed for {channel}: {e}")
        return False, (
            f"❌ Could not fetch admin list for <code>{chat_title}</code>.\n\n"
            f"Error: <code>{e}</code>"
        )

    # Search the admin list for the user
    user_entry = next((m for m in admins if m.user.id == user_id), None)

    if user_entry is None or user_entry.status not in _ADMIN_STATUSES:
        status_str = user_entry.status if user_entry else "not in admin list"
        return False, (
            f"🚫 <b>Access Denied</b>\n\n"
            f"You are <b>not an admin</b> of <b>{chat_title}</b>.\n\n"
            f"Your status: <code>{status_str}</code>\n\n"
            "Only channel <b>owners</b> and <b>admins</b> can create giveaways "
            "or referral bots for a channel.\n\n"
            "👉 Ask the channel owner to make you an admin first."
        )

    status = user_entry.status

    # ── 4. Also verify the bot itself is admin ──────────────
    # Reuse the already-fetched admin list — no extra API call needed.
    try:
        me = await bot.get_me()
        bot_entry = next((m for m in admins if m.user.id == me.id), None)
        if bot_entry is None or bot_entry.status not in _ADMIN_STATUSES:
            return False, (
                f"⚠️ <b>Bot is not an admin in {chat_title}</b>\n\n"
                "The bot needs to be an <b>Admin</b> in your channel to post giveaways.\n\n"
                "Please:\n"
                "1. Go to your channel settings\n"
                "2. Add this bot as an <b>Admin</b>\n"
                "3. Then try again"
            )
    except Exception as e:
        logger.warning(f"verify_channel_admin: bot self-check failed for {channel}: {e}")
        # Non-fatal — proceed even if we can't verify the bot's own status

    logger.info(
        f"verify_channel_admin: ✅ user={user_id} is {status} in "
        f"chat_id={chat_id} title={chat_title!r}"
    )
    return True, ""
