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

    # ── 2. Check membership status ──────────────────────────
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        status = member.status
    except TelegramForbiddenError:
        return False, (
            "❌ <b>Bot lacks permission</b>\n\n"
            f"The bot cannot read admin list for <code>{chat_title}</code>.\n"
            "Please make sure the bot is an <b>Admin</b> with at least "
            "<i>Manage channel</i> permission."
        )
    except Exception as e:
        logger.warning(f"verify_channel_admin: get_chat_member failed for {user_id} in {channel}: {e}")
        return False, (
            f"❌ Could not verify your admin status in <code>{chat_title}</code>.\n\n"
            f"Error: <code>{e}</code>"
        )

    if status not in _ADMIN_STATUSES:
        return False, (
            f"🚫 <b>Access Denied</b>\n\n"
            f"You are <b>not an admin</b> of <b>{chat_title}</b>.\n\n"
            f"Your status: <code>{status}</code>\n\n"
            "Only channel <b>owners</b> and <b>admins</b> can create giveaways "
            "or referral bots for a channel.\n\n"
            "👉 Ask the channel owner to make you an admin first."
        )

    # ── 3. Also verify the bot itself is admin ───────────────
    try:
        me = await bot.get_me()
        bot_member = await bot.get_chat_member(chat_id, me.id)
        if bot_member.status not in _ADMIN_STATUSES:
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
        # Non-fatal — some channels allow non-admin bots to post if added manually

    logger.info(
        f"verify_channel_admin: ✅ user={user_id} is {status} in "
        f"chat_id={chat_id} title={chat_title!r}"
    )
    return True, ""
