"""
utils/channel_admin_check.py

Utility to verify that a Telegram user is an admin (or owner) of a channel
before letting them create a giveaway poll or referral bot for that channel.

Usage:
    ok, err_msg = await verify_channel_admin(bot, user_id, channel_identifier)
    if not ok:
        await message.answer(err_msg, parse_mode="HTML")
        return
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

    # Only channels & supergroups can have polls/referral bots
    if chat.type not in ("channel", "supergroup"):
        return False, (
            "❌ <b>Not a channel or supergroup.</b>\n\n"
            f"<code>{channel}</code> is a <b>{chat.type}</b>.\n"
            "Please enter a public channel (e.g. <code>@mychannel</code>)."
        )

    # ── 2. Check membership status via admin list ────────────
    # IMPORTANT: For broadcast channels, get_chat_member() raises an error
    # for non-admins because regular members are not exposed by the Bot API.
    # The correct and reliable approach is to fetch the full admin list and
    # check if the user is present there.
    try:
        admins = await bot.get_chat_administrators(chat_id)
        user_admin_entry = next((m for m in admins if m.user.id == user_id), None)

        if user_admin_entry is None:
            return False, (
                f"🚫 <b>Access Denied</b>\n\n"
                f"You are <b>not an admin</b> of <b>{chat_title}</b>.\n\n"
                "Only channel <b>owners</b> and <b>admins</b> can create giveaways "
                "or referral bots for a channel.\n\n"
                "👉 Ask the channel owner to make you an admin first."
            )

        status = user_admin_entry.status
        if status not in _ADMIN_STATUSES:
            return False, (
                f"🚫 <b>Access Denied</b>\n\n"
                f"You are <b>not an admin</b> of <b>{chat_title}</b>.\n\n"
                f"Your status: <code>{status}</code>\n\n"
                "Only channel <b>owners</b> and <b>admins</b> can create giveaways "
                "or referral bots for a channel.\n\n"
                "👉 Ask the channel owner to make you an admin first."
            )

    except TelegramForbiddenError:
        return False, (
            "❌ <b>Bot lacks permission</b>\n\n"
            f"The bot cannot read the admin list for <code>{chat_title}</code>.\n"
            "Please make sure the bot is an <b>Admin</b> with at least "
            "<i>Manage channel</i> permission."
        )
    except Exception as e:
        logger.warning(
            f"verify_channel_admin: get_chat_administrators failed for {channel}: {e}"
        )
        return False, (
            f"❌ Could not verify your admin status in <code>{chat_title}</code>.\n\n"
            f"Error: <code>{e}</code>"
        )

    # ── 3. Also verify the bot itself is admin ───────────────
    try:
        me = await bot.get_me()
        bot_is_admin = any(m.user.id == me.id for m in admins)
        if not bot_is_admin:
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
        # Non-fatal — proceed if we can't verify the bot's own status

    logger.info(
        f"verify_channel_admin: ✅ user={user_id} is {status} in "
        f"chat_id={chat_id} title={chat_title!r}"
    )
    return True, ""
