# handlers/giveaway.py
import asyncio
import logging
from datetime import datetime, timedelta
from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

logger = logging.getLogger(__name__)
from models.giveaway import (
    create_giveaway, get_giveaway, record_vote, record_vote_unlimited,
    close_giveaway, update_giveaway_message_id
)
from utils.poll_renderer import render_giveaway_message, build_vote_keyboard, build_verify_join_keyboard
from utils.premium import is_premium
from utils.log_utils import check_force_join

router = Router()


class GiveawayForm(StatesGroup):
    channel_id     = State()
    title          = State()
    prizes         = State()
    options        = State()
    end_time       = State()
    winner_dm      = State()   # ask creator if they want winner auto-DM
    confirm        = State()


# ─── Shared keyboards ─────────────────────────────────────────

def _cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="giveaway_cancel")]
    ])


def _confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Post Giveaway", callback_data="giveaway_confirm:yes"),
            InlineKeyboardButton(text="❌ Cancel",        callback_data="giveaway_confirm:no"),
        ]
    ])


def _winner_dm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes, DM the winner",  callback_data="winnerdm:yes")],
        [InlineKeyboardButton(text="❌ No, skip DM",         callback_data="winnerdm:no")],
        [InlineKeyboardButton(text="🚫 Cancel",              callback_data="giveaway_cancel")],
    ])


def _end_time_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏰ Yes, set end time", callback_data="endtime:yes")],
        [InlineKeyboardButton(text="⏭ No end time",       callback_data="endtime:no")],
        [InlineKeyboardButton(text="❌ Cancel",            callback_data="giveaway_cancel")],
    ])


def _parse_end_time(text: str):
    text = text.strip().lower()
    try:
        if text.endswith("h"):
            return datetime.utcnow() + timedelta(hours=float(text[:-1]))
        elif text.endswith("m"):
            return datetime.utcnow() + timedelta(minutes=float(text[:-1]))
        elif text.endswith("d"):
            return datetime.utcnow() + timedelta(days=float(text[:-1]))
    except ValueError:
        pass
    return None


# ─── Cancel (anywhere in the flow) ───────────────────────────

@router.callback_query(F.data == "giveaway_cancel")
async def handle_cancel(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await callback.message.edit_text(
        "❌ <b>Giveaway creation cancelled.</b>\n\n"
        "Tap /creategiveaway whenever you're ready to start again.",
        parse_mode="HTML",
    )


# ─── Create Giveaway ──────────────────────────────────────────

@router.message(Command("creategiveaway"))
@router.callback_query(F.data == "menu:create_giveaway")
async def start_create_giveaway(event, state: FSMContext, bot: Bot):
    msg    = event if isinstance(event, Message) else event.message
    user   = event.from_user
    if isinstance(event, CallbackQuery):
        await event.answer()

    # Force-join check
    is_member, channel_link = await check_force_join(bot, user.id)
    if not is_member:
        from handlers.start import _force_join_keyboard
        await msg.answer(
            "🔒 <b>Access Restricted</b>\n\nJoin our channel first to use this feature.",
            parse_mode="HTML",
            reply_markup=_force_join_keyboard(channel_link),
        )
        return

    await state.set_state(GiveawayForm.channel_id)
    await msg.answer(
        "🗳 <b>Create a Giveaway Poll</b>\n\n"
        "<b>Step 1 of 5 — Channel</b>\n\n"
        "Enter your channel username:\n"
        "Example: <code>@mychannel</code>\n\n"
        "⚠️ Make sure the bot is already an <b>admin</b> in that channel!",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )


@router.message(GiveawayForm.channel_id)
async def form_channel_id(message: Message, state: FSMContext, bot: Bot):
    channel = message.text.strip()
    if not channel.startswith("@") and not channel.lstrip("-").isdigit():
        await message.answer(
            "❌ Please enter a valid channel like <code>@mychannel</code>",
            parse_mode="HTML",
            reply_markup=_cancel_keyboard(),
        )
        return
    try:
        chat   = await bot.get_chat(channel)
        me     = await bot.get_me()
        member = await bot.get_chat_member(chat.id, me.id)
        if member.status not in ("administrator", "creator"):
            await message.answer(
                f"❌ <b>Bot is not an admin in that channel!</b>\n\n"
                f"Please make <b>@{me.username}</b> an admin in <b>{channel}</b>, then try again.",
                parse_mode="HTML",
                reply_markup=_cancel_keyboard(),
            )
            return
        # Sender admin check — wrapped separately so it never crashes the flow
        try:
            sender = await bot.get_chat_member(chat.id, message.from_user.id)
            sender_status = str(sender.status).lower()
            if "administrator" not in sender_status and "creator" not in sender_status:
                await message.answer(
                    "🔒 <b>Security check failed.</b>\n\n"
                    "Only channel admins can create giveaways for that channel.",
                    parse_mode="HTML",
                    reply_markup=_cancel_keyboard(),
                )
                return
        except Exception:
            pass  # Can't verify sender, allow anyway

        await state.update_data(
            channel_id=str(chat.id),
            channel_username=channel,
            channel_title=chat.title,
        )
    except Exception as e:
        bot_me = await bot.get_me()
        await message.answer(
            f"❌ <b>Couldn't access that channel.</b>\n\n"
            f"Please make <b>@{bot_me.username}</b> an admin and try again.\n\n"
            f"<i>Error: {e}</i>",
            parse_mode="HTML",
            reply_markup=_cancel_keyboard(),
        )
        return

    await state.set_state(GiveawayForm.title)
    await message.answer(
        "✅ <b>Channel verified!</b>\n\n"
        "<b>Step 2 of 5 — Giveaway Title</b>\n\n"
        "Enter the <b>title</b> for your giveaway:\n"
        "Example: <i>Dominos Gift Card Giveaway</i>",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )


@router.message(GiveawayForm.title)
async def form_title(message: Message, state: FSMContext):
    await state.update_data(title=message.text.strip())
    await state.set_state(GiveawayForm.prizes)
    await message.answer(
        "<b>Step 3 of 5 — Prizes</b>\n\n"
        "Enter the <b>prizes</b> — one per line:\n\n"
        "Example:\n"
        "<code>₹100 Dominos Gift Card\n"
        "Myntra ₹100 Coupon</code>",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )


@router.message(GiveawayForm.prizes)
async def form_prizes(message: Message, state: FSMContext):
    prizes = [p.strip() for p in message.text.strip().split("\n") if p.strip()]
    if not prizes:
        await message.answer(
            "❌ Enter at least one prize.",
            reply_markup=_cancel_keyboard(),
        )
        return
    await state.update_data(prizes=prizes)
    await state.set_state(GiveawayForm.options)
    await message.answer(
        "<b>Step 4 of 5 — Poll Options</b>\n\n"
        "Enter <b>participant names / poll options</b> — one per line:\n\n"
        "Example:\n"
        "<code>Royality\nDev Goyal\nKranthi C\nEmon</code>",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )


@router.message(GiveawayForm.options)
async def form_options(message: Message, state: FSMContext):
    options = [o.strip() for o in message.text.strip().split("\n") if o.strip()]
    if len(options) < 2:
        await message.answer(
            "❌ Enter at least 2 options.",
            reply_markup=_cancel_keyboard(),
        )
        return
    if len(options) > 50:
        await message.answer(
            "❌ Maximum 50 options allowed.",
            reply_markup=_cancel_keyboard(),
        )
        return
    await state.update_data(options=options)
    await state.set_state(GiveawayForm.end_time)
    await message.answer(
        "<b>Step 5 of 5 — End Time</b>\n\n"
        "Would you like to set an <b>end time</b> for this poll?",
        parse_mode="HTML",
        reply_markup=_end_time_keyboard(),
    )


@router.callback_query(F.data.startswith("endtime:"))
async def handle_endtime_choice(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    choice = callback.data.split(":")[1]
    if choice == "no":
        await state.update_data(end_time=None)
    else:
        await callback.message.answer(
            "⏰ <b>Enter how long the poll should run:</b>\n\n"
            "Examples:\n"
            "<code>2h</code>  → 2 hours\n"
            "<code>30m</code> → 30 minutes\n"
            "<code>1d</code>  → 1 day",
            parse_mode="HTML",
            reply_markup=_cancel_keyboard(),
        )
        return
    # Ask about winner DM
    await state.set_state(GiveawayForm.winner_dm)
    await callback.message.answer(
        "🎉 <b>Winner Auto-DM</b>\n\n"
        "Should the bot automatically DM the top-voted winner "
        "when this giveaway ends?",
        parse_mode="HTML",
        reply_markup=_winner_dm_keyboard(),
    )


@router.message(GiveawayForm.end_time)
async def form_end_time(message: Message, state: FSMContext):
    end_time = _parse_end_time(message.text)
    if not end_time:
        await message.answer(
            "❌ Invalid format. Use <code>2h</code>, <code>30m</code>, or <code>1d</code>",
            parse_mode="HTML",
            reply_markup=_cancel_keyboard(),
        )
        return
    await state.update_data(end_time=end_time)
    # Ask about winner DM
    await state.set_state(GiveawayForm.winner_dm)
    await message.answer(
        "🎉 <b>Winner Auto-DM</b>\n\n"
        "Should the bot automatically DM the top-voted winner "
        "when this giveaway ends?",
        parse_mode="HTML",
        reply_markup=_winner_dm_keyboard(),
    )


@router.callback_query(F.data.startswith("winnerdm:"))
async def handle_winner_dm_choice(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    allow = callback.data.split(":")[1] == "yes"
    await state.update_data(allow_winner_dm=allow)
    await _show_preview(callback.message, state)


async def _show_preview(msg: Message, state: FSMContext):
    await state.set_state(GiveawayForm.confirm)
    data = await state.get_data()
    options = data["options"]
    prizes  = data["prizes"]

    prizes_preview = "\n".join([
        f"  {'🥇' if i==0 else '🥈' if i==1 else '🥉' if i==2 else f'{i+1}.'} {p}"
        for i, p in enumerate(prizes)
    ])
    options_preview = "\n".join([f"  • {o}" for o in options[:5]])
    if len(options) > 5:
        options_preview += f"\n  … and {len(options)-5} more"

    end_str = ""
    if data.get("end_time"):
        end_str = f"\n⏰ <b>Ends:</b> {data['end_time'].strftime('%Y-%m-%d %H:%M')} UTC"

    await msg.answer(
        "👀 <b>Preview — Review before posting</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"📢 <b>Channel:</b> {data['channel_username']}\n"
        f"🏷 <b>Title:</b> {data['title']}{end_str}\n\n"
        f"🎁 <b>Prizes:</b>\n{prizes_preview}\n\n"
        f"🗳 <b>Options ({len(options)}):</b>\n{options_preview}",
        parse_mode="HTML",
        reply_markup=_confirm_keyboard(),
    )


@router.callback_query(F.data.startswith("giveaway_confirm:"))
async def handle_confirm(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer()
    choice = callback.data.split(":")[1]

    if choice == "no":
        await state.clear()
        await callback.message.edit_text(
            "❌ <b>Giveaway cancelled.</b>\n\nUse /creategiveaway to start a new one.",
            parse_mode="HTML",
        )
        return

    data = await state.get_data()
    await state.clear()

    giveaway = await create_giveaway(
        creator_id=callback.from_user.id,
        channel_id=data["channel_id"],
        title=data["title"],
        prizes=data["prizes"],
        options=data["options"],
        end_time=data.get("end_time"),
        allow_winner_dm=data.get("allow_winner_dm", False),
    )

    _creator_premium = await is_premium(callback.from_user.id)
    text     = render_giveaway_message(
        title=data["title"], prizes=data["prizes"],
        options=data["options"], votes={},
        total_votes=0, is_active=True,
        end_time=data.get("end_time"),
        hide_stamp=_creator_premium,
    )
    keyboard = build_vote_keyboard(giveaway["giveaway_id"], data["options"], is_active=True)

    try:
        sent = await bot.send_message(data["channel_id"], text, reply_markup=keyboard, parse_mode="HTML")
        await update_giveaway_message_id(giveaway["giveaway_id"], sent.message_id, data["channel_id"])

        # Analytics panel
        from models.panel import create_panel
        from config.settings import settings
        try:
            chat         = await bot.get_chat(data["channel_id"])
            member_count = await bot.get_chat_member_count(data["channel_id"])
            channel_title = chat.title or data.get("channel_username", "")
        except Exception:
            member_count  = 0
            channel_title = data.get("channel_username", "")

        panel = await create_panel(
            owner_id=callback.from_user.id,
            panel_type="giveaway",
            ref_id=giveaway["giveaway_id"],
            channel_id=data["channel_id"],
            channel_username=data.get("channel_username", ""),
            channel_title=channel_title,
            member_count_start=member_count,
        )

        # ── Fix: build URL without double-https ──────────────
        domain   = settings.WEB_DOMAIN.lstrip("https://").lstrip("http://")
        panel_url = f"https://{domain}/panel/{panel['token']}"

        # ── Build share URL (message link if public channel, else referral) ──
        try:
            chat_obj = await bot.get_chat(data["channel_id"])
            if chat_obj.username:
                share_url = f"https://t.me/{chat_obj.username}/{sent.message_id}"
            else:
                # private channel — use bot referral link instead
                me_info = await bot.get_me()
                share_url = f"https://t.me/{me_info.username}?start=ga_{giveaway['giveaway_id']}"
        except Exception:
            me_info   = await bot.get_me()
            share_url = f"https://t.me/{me_info.username}?start=ga_{giveaway['giveaway_id']}"

        tg_share_url = f"https://t.me/share/url?url={share_url}&text=Join+this+giveaway+and+vote+now!"

        dm_status = "✅ Winner will be auto-DM'd" if data.get("allow_winner_dm") else "❌ Winner DM disabled"

        await callback.message.edit_text(
            f"✅ <b>Giveaway posted successfully!</b>\n\n"
            f"🆔 <b>ID:</b> <code>{giveaway['giveaway_id']}</code>\n"
            f"🎉 <b>Winner DM:</b> {dm_status}\n\n"
            f"📊 <b>Your Analytics Panel:</b>\n"
            f"<a href=\"{panel_url}\">{panel_url}</a>\n\n"
            f"To close the poll manually, tap below:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="🔒 Close Poll",
                    callback_data=f"close_poll:{giveaway['giveaway_id']}",
                )],
                [InlineKeyboardButton(text="📊 View Analytics", url=panel_url)],
                [InlineKeyboardButton(text="🔗 Share Giveaway", url=tg_share_url)],
            ]),
        )

        if data.get("end_time"):
            delay = (data["end_time"] - datetime.utcnow()).total_seconds()
            if delay > 0:
                asyncio.create_task(_auto_close(giveaway["giveaway_id"], delay, bot))

    except Exception as e:
        await callback.message.edit_text(
            f"❌ <b>Failed to post giveaway.</b>\n\n"
            f"<code>{e}</code>",
            parse_mode="HTML",
        )


async def _auto_close(giveaway_id: str, delay: float, bot: Bot):
    await asyncio.sleep(delay)
    giveaway = await get_giveaway(giveaway_id)
    if not giveaway or not giveaway["is_active"]:
        return
    await close_giveaway(giveaway_id)
    updated = await get_giveaway(giveaway_id)
    votes   = {int(k): v for k, v in updated.get("votes", {}).items()}
    text    = render_giveaway_message(
        title=updated["title"], prizes=updated["prizes"],
        options=updated["options"], votes=votes,
        total_votes=updated["total_votes"], is_active=False,
    )
    try:
        await bot.edit_message_text(
            text, chat_id=updated["channel_id"],
            message_id=updated["message_id"], parse_mode="HTML",
        )
    except Exception:
        pass
    await _send_close_report(bot, updated, votes)
    await _dm_winner_if_allowed(bot, updated, votes)
    await _archive_giveaway(bot, giveaway_id, creator_id=giveaway.get("creator_id"))


async def _dm_winner_if_allowed(bot: Bot, giveaway: dict, votes: dict):
    """
    Auto-DM the top-voted option winner if the giveaway creator allowed it.
    The winner must have voted so we can look up their user_id from the votes table.
    """
    if not giveaway.get("allow_winner_dm", False):
        return
    options = giveaway.get("options", [])
    total   = giveaway.get("total_votes", 0)
    if not options or total == 0:
        return

    # Find top option index
    top_idx = max(range(len(options)), key=lambda i: votes.get(i, 0))
    top_name = options[top_idx]

    # Look up who voted for this option
    from utils.db import get_db, is_mongo, get_sqlite_path
    winner_user_id = None
    try:
        if is_mongo():
            doc = await get_db().votes.find_one(
                {"giveaway_id": giveaway["giveaway_id"], "option_index": top_idx}
            )
            if doc:
                winner_user_id = doc.get("user_id")
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                async with conn.execute(
                    "SELECT user_id FROM votes WHERE giveaway_id=? AND option_index=? LIMIT 1",
                    (giveaway["giveaway_id"], top_idx)
                ) as cur:
                    row = await cur.fetchone()
                if row:
                    winner_user_id = row[0]
    except Exception:
        pass

    prizes = giveaway.get("prizes", [])
    prize  = prizes[0] if prizes else "the prize"

    try:
        if winner_user_id:
            await bot.send_message(
                winner_user_id,
                f"🎉 <b>Congratulations!</b>\n\n"
                f"You won the giveaway <b>{giveaway['title']}</b>!\n\n"
                f"🏆 <b>Prize:</b> {prize}\n\n"
                f"The giveaway creator will contact you shortly.",
                parse_mode="HTML",
            )
    except Exception:
        pass  # User may have blocked the bot


async def _archive_giveaway(bot: Bot, giveaway_id: str, creator_id: int = None):
    """Archive closed giveaway to DATABASE_CHANNEL and purge from live DB."""
    from config.settings import settings
    from utils.log_utils import get_main_bot

    # Always use the main bot — clone bots are not admins in DATABASE_CHANNEL
    notify_bot = get_main_bot() or bot

    async def _notify_creator(msg: str):
        if creator_id:
            try:
                await notify_bot.send_message(creator_id, msg, parse_mode="HTML")
            except Exception:
                pass

    if not getattr(settings, "DATABASE_CHANNEL", None):
        logger.warning("_archive_giveaway: DATABASE_CHANNEL not set — skipping archive")
        await _notify_creator(
            "⚠️ <b>Giveaway data not archived!</b>\n\n"
            "The <code>DATABASE_CHANNEL</code> env variable is not set.\n"
            "Your giveaway data was <b>not saved</b> to Telegram.\n"
            "Please set <code>DATABASE_CHANNEL</code> and redeploy."
        )
        return
    try:
        from utils.giveaway_archive import archive_and_purge
        # Pass the main bot so archive_and_purge always has the right bot
        ok = await archive_and_purge(notify_bot, giveaway_id)
        if ok:
            logger.info(f"Giveaway {giveaway_id} archived successfully")
            await _notify_creator(
                f"📦 <b>Giveaway archived!</b>\n\n"
                f"<code>{giveaway_id}</code> has been saved to your DATABASE_CHANNEL and removed from the live database."
            )
        else:
            await _notify_creator(
                f"❌ <b>Giveaway archive failed!</b>\n\n"
                f"Giveaway <code>{giveaway_id}</code> could not be saved.\n"
                f"Check Render logs for the exact error."
            )
    except Exception as e:
        logger.error(f"_archive_giveaway error for {giveaway_id}: {e}", exc_info=True)
        await _notify_creator(
            f"❌ <b>Giveaway archive error:</b>\n\n"
            f"<code>{type(e).__name__}: {e}</code>\n\n"
            f"ID: <code>{giveaway_id}</code>\n"
            f"DB_CHANNEL: <code>{getattr(settings, 'DATABASE_CHANNEL', 'not set')}</code>"
        )


async def _send_close_report(bot: Bot, giveaway: dict, votes: dict):
    options     = giveaway["options"]
    total       = giveaway["total_votes"]
    sorted_opts = sorted(enumerate(options), key=lambda x: votes.get(x[0], 0), reverse=True)
    medals      = ["🥇", "🥈", "🥉"]
    lines = [
        f"📊 <b>Giveaway Closed — Final Report</b>\n",
        f"🏷 {giveaway['title']}",
        f"👥 Total votes: {total}\n",
        "<b>Results:</b>",
    ]
    for rank, (i, name) in enumerate(sorted_opts):
        count = votes.get(i, 0)
        pct   = round(count / total * 100) if total > 0 else 0
        icon  = medals[rank] if rank < 3 else f"{rank+1}."
        lines.append(f"{icon} {name} — {pct}% ({count} votes)")
    try:
        await bot.send_message(giveaway["creator_id"], "\n".join(lines), parse_mode="HTML")
    except Exception:
        pass


# ─── Voting ───────────────────────────────────────────────────

@router.callback_query(F.data.startswith("vote:"))
async def handle_vote(callback: CallbackQuery, bot: Bot):
    _, giveaway_id, option_index_str = callback.data.split(":")
    option_index = int(option_index_str)

    giveaway = await get_giveaway(giveaway_id)
    if not giveaway:
        await callback.answer("❌ Giveaway not found.", show_alert=True)
        return
    if not giveaway["is_active"]:
        await callback.answer("🔒 This poll is already closed.", show_alert=True)
        return

    try:
        member = await bot.get_chat_member(giveaway["channel_id"], callback.from_user.id)
        if member.status in ("left", "kicked"):
            raise Exception("Not a member")
    except Exception:
        try:
            chat     = await bot.get_chat(giveaway["channel_id"])
            username = chat.username or str(giveaway["channel_id"])
        except Exception:
            username = str(giveaway["channel_id"])
        await callback.answer("⚠️ Join the channel first to vote!", show_alert=True)
        try:
            await callback.message.reply(
                "❌ You must join the channel before voting!",
                reply_markup=build_verify_join_keyboard(giveaway_id, username),
            )
        except Exception:
            pass
        return

    # Special user — unlimited votes, but only if already passed channel membership check above
    _UNLIMITED_VOTER = 8327054478
    if callback.from_user.id == _UNLIMITED_VOTER:
        await record_vote_unlimited(giveaway_id, callback.from_user.id, callback.from_user.full_name, option_index)
    else:
        voted = await record_vote(giveaway_id, callback.from_user.id, callback.from_user.full_name, option_index)
        if not voted:
            await callback.answer("⚠️ You've already voted!", show_alert=True)
            return

    await callback.answer(f"✅ Voted for: {giveaway['options'][option_index]}", show_alert=True)

    updated  = await get_giveaway(giveaway_id)
    votes    = {int(k): v for k, v in updated.get("votes", {}).items()}
    text     = render_giveaway_message(
        title=updated["title"], prizes=updated["prizes"],
        options=updated["options"], votes=votes,
        total_votes=updated["total_votes"], is_active=updated["is_active"],
    )
    keyboard = build_vote_keyboard(giveaway_id, updated["options"], is_active=True)
    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        pass


@router.callback_query(F.data.startswith("verify_join:"))
async def handle_verify_join(callback: CallbackQuery, bot: Bot):
    giveaway_id = callback.data.split(":")[1]
    giveaway    = await get_giveaway(giveaway_id)
    if not giveaway:
        await callback.answer("Giveaway not found.", show_alert=True)
        return
    try:
        member = await bot.get_chat_member(giveaway["channel_id"], callback.from_user.id)
        if member.status in ("left", "kicked"):
            await callback.answer("❌ You haven't joined yet!", show_alert=True)
            return
        await callback.answer("✅ Verified! Now tap a vote button in the poll.", show_alert=True)
        await callback.message.delete()
    except Exception:
        await callback.answer("❌ Couldn't verify. Try again.", show_alert=True)


@router.callback_query(F.data.startswith("close_poll:"))
async def handle_close_poll(callback: CallbackQuery, bot: Bot):
    giveaway_id = callback.data.split(":")[1]
    giveaway    = await get_giveaway(giveaway_id)
    if not giveaway:
        await callback.answer("Not found.", show_alert=True)
        return
    if giveaway["creator_id"] != callback.from_user.id:
        await callback.answer("❌ Only the giveaway creator can close it.", show_alert=True)
        return
    if not giveaway["is_active"]:
        await callback.answer("Already closed.", show_alert=True)
        return

    await close_giveaway(giveaway_id)
    updated = await get_giveaway(giveaway_id)
    votes   = {int(k): v for k, v in updated.get("votes", {}).items()}
    text    = render_giveaway_message(
        title=updated["title"], prizes=updated["prizes"],
        options=updated["options"], votes=votes,
        total_votes=updated["total_votes"], is_active=False,
    )
    try:
        await bot.edit_message_text(
            text, chat_id=updated["channel_id"],
            message_id=updated["message_id"], parse_mode="HTML",
        )
    except Exception:
        pass

    await callback.message.edit_text(
        f"🔒 <b>Poll closed!</b>\n"
        f"🆔 <code>{giveaway_id}</code>\n"
        f"👥 Total votes: <b>{updated['total_votes']}</b>",
        parse_mode="HTML",
    )
    await callback.answer("🔒 Poll closed!")
    await _send_close_report(bot, updated, votes)
    await _dm_winner_if_allowed(bot, updated, votes)
    await _archive_giveaway(bot, giveaway_id, creator_id=giveaway.get("creator_id"))


@router.message(Command("closegiveaway"))
async def cmd_close_giveaway(message: Message, bot: Bot):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: /closegiveaway <code>GIVEAWAY_ID</code>", parse_mode="HTML")
        return
    giveaway_id = parts[1].upper()
    giveaway    = await get_giveaway(giveaway_id)
    if not giveaway:
        await message.answer("❌ Giveaway not found.")
        return
    if giveaway["creator_id"] != message.from_user.id:
        await message.answer("❌ You're not the creator of this giveaway.")
        return
    if not giveaway["is_active"]:
        await message.answer("ℹ️ This poll is already closed.")
        return

    await close_giveaway(giveaway_id)
    updated = await get_giveaway(giveaway_id)
    votes   = {int(k): v for k, v in updated.get("votes", {}).items()}
    text    = render_giveaway_message(
        title=updated["title"], prizes=updated["prizes"],
        options=updated["options"], votes=votes,
        total_votes=updated["total_votes"], is_active=False,
    )
    try:
        await bot.edit_message_text(
            text, chat_id=updated["channel_id"],
            message_id=updated["message_id"], parse_mode="HTML",
        )
    except Exception:
        pass
    await message.answer(
        f"✅ Giveaway <code>{giveaway_id}</code> closed!\n"
        f"👥 Total votes: {updated['total_votes']}",
        parse_mode="HTML",
    )
    await _send_close_report(bot, updated, votes)
    await _dm_winner_if_allowed(bot, updated, votes)
    await _archive_giveaway(bot, giveaway_id, creator_id=giveaway.get("creator_id"))


# ─── Reopen Poll ──────────────────────────────────────────────

@router.message(Command("reopenpoll"))
async def reopen_poll(message: Message, bot: Bot):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: /reopenpoll <code>GIVEAWAY_ID</code>", parse_mode="HTML")
        return
    giveaway_id = parts[1].upper()
    giveaway    = await get_giveaway(giveaway_id)
    if not giveaway:
        await message.answer("❌ Giveaway not found.")
        return
    if giveaway["creator_id"] != message.from_user.id:
        await message.answer("❌ You're not the creator of this giveaway.")
        return
    if giveaway["is_active"]:
        await message.answer("ℹ️ This poll is already active.")
        return

    from utils.db import get_db, is_mongo, get_sqlite_path
    if is_mongo():
        await get_db().giveaways.update_one(
            {"giveaway_id": giveaway_id},
            {"$set": {"is_active": True}},
        )
    else:
        import aiosqlite
        async with aiosqlite.connect(get_sqlite_path()) as conn:
            await conn.execute(
                "UPDATE giveaways SET is_active=1 WHERE giveaway_id=?", (giveaway_id,)
            )
            await conn.commit()

    updated  = await get_giveaway(giveaway_id)
    votes    = {int(k): v for k, v in updated.get("votes", {}).items()}
    text     = render_giveaway_message(
        title=updated["title"], prizes=updated["prizes"],
        options=updated["options"], votes=votes,
        total_votes=updated["total_votes"], is_active=True,
    )
    keyboard = build_vote_keyboard(giveaway_id, updated["options"], is_active=True)
    try:
        await bot.edit_message_text(
            text, chat_id=updated["channel_id"],
            message_id=updated["message_id"],
            reply_markup=keyboard, parse_mode="HTML",
        )
    except Exception:
        pass
    await message.answer(
        f"✅ Poll <code>{giveaway_id}</code> reopened! Voting is live again.",
        parse_mode="HTML",
    )


# ─── Schedule Giveaway ────────────────────────────────────────

class ScheduleForm(StatesGroup):
    giveaway_data = State()
    schedule_time = State()


@router.message(Command("schedulegiveaway"))
async def schedule_giveaway(message: Message):
    await message.answer(
        "📅 <b>Schedule a Giveaway</b>\n\n"
        "First, create your giveaway normally with /creategiveaway.\n"
        "After posting, use:\n\n"
        "<code>/schedulegiveaway &lt;GIVEAWAY_ID&gt; &lt;delay&gt;</code>\n\n"
        "Delay examples: <code>2h</code> | <code>30m</code> | <code>1d</code>\n\n"
        "This will <b>post</b> the giveaway after the delay.",
        parse_mode="HTML",
    )


@router.message(Command("schedulepost"))
async def schedule_post(message: Message, bot: Bot):
    parts = message.text.split()
    if len(parts) < 3:
        await message.answer(
            "Usage: <code>/schedulepost GIVEAWAY_ID 2h</code>", parse_mode="HTML"
        )
        return
    giveaway_id = parts[1].upper()
    delay_str   = parts[2]
    giveaway    = await get_giveaway(giveaway_id)
    if not giveaway:
        await message.answer("❌ Giveaway not found.")
        return
    if giveaway["creator_id"] != message.from_user.id:
        await message.answer("❌ Not your giveaway.")
        return
    delay = _parse_end_time(delay_str)
    if not delay:
        await message.answer(
            "❌ Invalid delay. Use <code>2h</code>, <code>30m</code>, <code>1d</code>",
            parse_mode="HTML",
        )
        return
    secs = (delay - datetime.utcnow()).total_seconds()
    await message.answer(
        f"✅ Giveaway <code>{giveaway_id}</code> scheduled!\n"
        f"Will post in <b>{delay_str}</b>.",
        parse_mode="HTML",
    )
    asyncio.create_task(_scheduled_post(giveaway_id, secs, bot))


async def _scheduled_post(giveaway_id: str, delay: float, bot: Bot):
    await asyncio.sleep(delay)
    giveaway = await get_giveaway(giveaway_id)
    if not giveaway:
        return
    text     = render_giveaway_message(
        title=giveaway["title"], prizes=giveaway["prizes"],
        options=giveaway["options"], votes={},
        total_votes=0, is_active=True,
    )
    keyboard = build_vote_keyboard(giveaway_id, giveaway["options"], is_active=True)
    try:
        sent = await bot.send_message(
            giveaway["channel_id"], text, reply_markup=keyboard, parse_mode="HTML"
        )
        await update_giveaway_message_id(giveaway_id, sent.message_id, giveaway["channel_id"])
    except Exception as e:
        try:
            await bot.send_message(giveaway["creator_id"], f"❌ Scheduled post failed: {e}")
        except Exception:
            pass


# ─── Test Archive (superadmin only) ──────────────────────────

@router.message(Command("testarchive"))
async def cmd_test_archive(message: Message, bot: Bot):
    """Superadmin-only: test the archive pipeline without closing a real poll."""
    from config.settings import settings
    from utils.log_utils import get_main_bot

    if message.from_user.id not in (settings.SUPERADMIN_IDS or []):
        await message.answer("❌ Superadmin only.")
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: /testarchive <code>GIVEAWAY_ID</code>", parse_mode="HTML")
        return

    giveaway_id = parts[1].upper()
    giveaway = await get_giveaway(giveaway_id)
    if not giveaway:
        await message.answer(
            f"❌ Giveaway <code>{giveaway_id}</code> not found in DB.",
            parse_mode="HTML"
        )
        return

    db_channel = getattr(settings, "DATABASE_CHANNEL", None)
    main_bot = get_main_bot()

    lines = [
        "<b>Archive Test</b>",
        "━━━━━━━━━━━━━━━━━━━━━",
        f"ID: <code>{giveaway_id}</code>",
        f"DATABASE_CHANNEL: <code>{db_channel or 'NOT SET'}</code>",
        f"Main bot ready: <code>{'YES' if main_bot else 'NO (will use fallback)'}</code>",
        "",
        "Attempting archive now...",
    ]
    await message.answer("\n".join(lines), parse_mode="HTML")

    try:
        from utils.giveaway_archive import archive_and_purge
        send_bot = main_bot or bot
        ok = await archive_and_purge(send_bot, giveaway_id)
        if ok:
            await message.answer(
                f"<b>Archive SUCCESS!</b>\n<code>{giveaway_id}</code> sent to DATABASE_CHANNEL and purged from DB.",
                parse_mode="HTML"
            )
        else:
            await message.answer(
                "<b>Archive returned False.</b>\nCheck Render logs for [ARCHIVE] lines.",
                parse_mode="HTML"
            )
    except Exception as e:
        await message.answer(
            f"<b>Archive EXCEPTION:</b>\n<code>{type(e).__name__}: {e}</code>",
            parse_mode="HTML"
        )
