# handlers/giveaway.py
import asyncio
import html
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

router = Router()


class GiveawayForm(StatesGroup):
    channel_id     = State()
    title          = State()
    prizes         = State()
    options        = State()
    end_time       = State()
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
    logger.info(f"[GIVEAWAY] handle_cancel: user={callback.from_user.id} cancelled giveaway creation")
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
@router.callback_query(F.data == "menu:create_giveaway_poll")
async def start_create_giveaway(event, state: FSMContext, bot: Bot):
    msg    = event if isinstance(event, Message) else event.message
    user   = event.from_user
    if isinstance(event, CallbackQuery):
        await event.answer()

    logger.info(f"[GIVEAWAY] start_create_giveaway triggered by user={user.id} username=@{user.username}")
    await state.set_state(GiveawayForm.channel_id)
    logger.info(f"[GIVEAWAY] State set to channel_id for user={user.id}")
    await msg.answer(
        "🗳 <b>Create a Giveaway Poll</b>\n\n"
        "<b>Step 1 of 5 — Channel</b>\n\n"
        "Enter your channel username:\n"
        "Example: <code>@mychannel</code>\n\n"
        "🔐 <b>Requirements (both must be met):</b>\n"
        "1️⃣ <b>You</b> must be an <b>Admin</b> of the channel\n"
        "2️⃣ <b>This bot</b> must be an <b>Admin</b> in the channel\n\n"
        "⚠️ Admin verification is automatic — unauthorized users will be blocked.",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )


@router.message(GiveawayForm.channel_id)
async def form_channel_id(message: Message, state: FSMContext, bot: Bot):
    channel = message.text.strip()
    user_id = message.from_user.id
    logger.info(f"[GIVEAWAY] form_channel_id: user={user_id} entered channel={channel}")

    if not channel.startswith("@") and not channel.lstrip("-").isdigit():
        logger.warning(f"[GIVEAWAY] form_channel_id: invalid format user={user_id} input={channel}")
        await message.answer(
            "❌ Please enter a valid channel like @mychannel",
            reply_markup=_cancel_keyboard(),
        )
        return

    # ── Security: verify user is actually admin of this channel ──
    verifying_msg = await message.answer(
        "🔍 <b>Verifying your admin status…</b>",
        parse_mode="HTML",
    )
    from utils.channel_admin_check import verify_channel_admin
    ok, err = await verify_channel_admin(bot, user_id, channel)
    try:
        await verifying_msg.delete()
    except Exception:
        pass
    if not ok:
        logger.warning(f"[GIVEAWAY] form_channel_id: admin check FAILED user={user_id} channel={channel}")
        await message.answer(err, parse_mode="HTML", reply_markup=_cancel_keyboard())
        return
    logger.info(f"[GIVEAWAY] form_channel_id: admin check PASSED user={user_id} channel={channel}")
    # ─────────────────────────────────────────────────────────────

    # Resolve chat ID and title (already confirmed accessible above)
    chat_id = channel
    chat_title = channel
    try:
        chat = await bot.get_chat(channel)
        chat_id = str(chat.id)
        chat_title = chat.title or channel
        logger.info(f"[GIVEAWAY] form_channel_id: resolved chat_id={chat_id} title={chat_title}")
    except Exception as e:
        logger.warning(f"[GIVEAWAY] form_channel_id: get_chat failed ({e}), using raw input as chat_id")

    await state.update_data(
        channel_id=chat_id,
        channel_username=channel,
        channel_title=chat_title,
    )
    await state.set_state(GiveawayForm.title)
    logger.info(f"[GIVEAWAY] form_channel_id: proceeding to title state for user={user_id}")
    await message.answer(
        f"✅ <b>Channel verified!</b> <code>{chat_title}</code>\n\n"
        "<b>Step 2 of 5 — Giveaway Title</b>\n\n"
        "Enter the title for your giveaway:\n"
        "Example: <code>iPhone 15 Giveaway</code>",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )


@router.message(GiveawayForm.title)
async def form_title(message: Message, state: FSMContext):
    logger.info(f"[GIVEAWAY] form_title: user={message.from_user.id} title={message.text.strip()[:50]}")
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
    logger.info(f"[GIVEAWAY] form_prizes: user={message.from_user.id}")
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
    logger.info(f"[GIVEAWAY] form_options: user={message.from_user.id}")
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
    await _show_preview(callback.message, state)


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
    await _show_preview(message, state)




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
    logger.info(f"[GIVEAWAY] handle_confirm: user={callback.from_user.id} choice={callback.data}")
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

    # Resolve channel_id — must be numeric for send_message to work reliably
    channel_id = data["channel_id"]
    try:
        if not str(channel_id).lstrip("-").isdigit():
            chat = await bot.get_chat(channel_id)
            channel_id = str(chat.id)
            logger.info(f"[GIVEAWAY] handle_confirm: resolved {data['channel_id']} → {channel_id}")
    except Exception as e:
        logger.error(f"[GIVEAWAY] handle_confirm: failed to resolve channel_id={channel_id} error={e}")

    try:
        logger.info(f"[GIVEAWAY] handle_confirm: sending to channel_id={channel_id}")
        sent = await bot.send_message(channel_id, text, reply_markup=keyboard, parse_mode="HTML")
        logger.info(f"[GIVEAWAY] handle_confirm: sent! message_id={sent.message_id}")
        await update_giveaway_message_id(giveaway["giveaway_id"], sent.message_id, channel_id)

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

        await callback.message.edit_text(
            f"✅ <b>Giveaway posted successfully!</b>\n\n"
            f"🆔 <b>ID:</b> <code>{giveaway['giveaway_id']}</code>\n"            f"📊 <b>Your Analytics Panel:</b>\n"
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
        logger.error(f"[GIVEAWAY] handle_confirm: FAILED to post giveaway — {type(e).__name__}: {e}", exc_info=True)
        await callback.message.edit_text(
            f"❌ Failed to post giveaway.\n\n{type(e).__name__}: {e}",
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
    # ── Parse callback data safely ────────────────────────────
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("❌ Invalid vote data.", show_alert=True)
        return
    _, giveaway_id, option_index_str = parts
    try:
        option_index = int(option_index_str)
    except ValueError:
        await callback.answer("❌ Invalid option.", show_alert=True)
        return

    # ── Load giveaway ─────────────────────────────────────────
    giveaway = await get_giveaway(giveaway_id)
    if not giveaway:
        await callback.answer("❌ Giveaway not found.", show_alert=True)
        return
    if not giveaway.get("is_active", False):
        await callback.answer("🔒 This poll is already closed.", show_alert=True)
        return

    # ── Bounds check option_index ─────────────────────────────
    options = giveaway.get("options") or []
    if option_index < 0 or option_index >= len(options):
        await callback.answer("❌ Invalid option index.", show_alert=True)
        return

    # ── Resolve channel_id (may be int or str) ────────────────
    raw_channel_id = giveaway.get("channel_id", "")
    try:
        channel_id = int(raw_channel_id)
    except (ValueError, TypeError):
        channel_id = raw_channel_id  # keep as @username string

    # ── Channel membership check ──────────────────────────────
    not_member = False
    try:
        member = await bot.get_chat_member(channel_id, callback.from_user.id)
        if member.status in ("left", "kicked"):
            not_member = True
    except Exception as e:
        logger.warning(f"handle_vote: membership check error for {callback.from_user.id}: {e}")
        # Bot lacks permission — allow vote rather than blocking everyone

    if not_member:
        try:
            chat = await bot.get_chat(channel_id)
            username = chat.username or str(raw_channel_id).lstrip("@")
        except Exception:
            username = str(raw_channel_id).lstrip("@")
        await callback.answer("⚠️ Join the channel first to vote!", show_alert=True)
        try:
            await callback.message.reply(
                "❌ You must join the channel before voting!",
                reply_markup=build_verify_join_keyboard(giveaway_id, username),
            )
        except Exception:
            pass
        return

    # ── Check superadmin for unlimited votes ──────────────────
    from config.settings import settings as _s
    superadmin_ids = getattr(_s, "SUPERADMIN_IDS", [])
    if isinstance(superadmin_ids, int):
        superadmin_ids = [superadmin_ids]
    elif not isinstance(superadmin_ids, (list, tuple, set)):
        superadmin_ids = []
    is_super = int(callback.from_user.id) in [int(x) for x in superadmin_ids]

    # ── Record vote ───────────────────────────────────────────
    if is_super:
        await record_vote_unlimited(
            giveaway_id, callback.from_user.id,
            callback.from_user.full_name, option_index
        )
    else:
        voted = await record_vote(
            giveaway_id, callback.from_user.id,
            callback.from_user.full_name, option_index
        )
        if not voted:
            await callback.answer("⚠️ You've already voted!", show_alert=True)
            return

    # ── Acknowledge ───────────────────────────────────────────
    try:
        await callback.answer(f"✅ Voted for: {options[option_index]}", show_alert=True)
    except Exception:
        pass

    # ── Refresh poll message ──────────────────────────────────
    try:
        updated = await get_giveaway(giveaway_id)
        if not updated:
            return
        votes_raw = updated.get("votes", {}) or {}
        if isinstance(votes_raw, str):
            import json as _j
            try:
                votes_raw = _j.loads(votes_raw)
            except Exception:
                votes_raw = {}
        votes = {int(k): v for k, v in votes_raw.items()}
        text = render_giveaway_message(
            title=updated["title"],
            prizes=updated.get("prizes", []),
            options=updated["options"],
            votes=votes,
            total_votes=updated.get("total_votes", 0),
            is_active=updated.get("is_active", True),
        )
        keyboard = build_vote_keyboard(
            giveaway_id, updated["options"],
            is_active=updated.get("is_active", True)
        )
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        logger.debug(f"handle_vote: edit_text skipped: {e}")


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


# ─── Debug: catch unhandled messages (remove after debugging) ─
@router.message()
async def debug_unhandled(message: Message, state: FSMContext):
    current = await state.get_state()
    logger.warning(
        f"[GIVEAWAY] UNHANDLED MESSAGE: user={message.from_user.id} "
        f"text={repr(message.text)} current_state={current}"
    )


# ─── /giveawayinfo <id> ────────────────────────────────────────
@router.message(Command("giveawayinfo"))
async def cmd_giveaway_info(message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer(
            "❌ Usage: /giveawayinfo <code>&lt;giveaway_id&gt;</code>\n"
            "Example: /giveawayinfo 9A31157D",
            parse_mode="HTML"
        )
        return

    giveaway_id = parts[1].upper()
    user_id = message.from_user.id
    giveaway = await get_giveaway(giveaway_id)

    if not giveaway:
        try:
            from utils.db import get_db, is_mongo, get_sqlite_path
            if is_mongo():
                db = get_db()
                ref = await db.giveaway_archive_refs.find_one({"giveaway_id": giveaway_id})
                if ref:
                    giveaway = ref
                    giveaway["is_active"] = False
                    giveaway["archived"] = True
            else:
                import aiosqlite
                async with aiosqlite.connect(get_sqlite_path()) as conn:
                    conn.row_factory = aiosqlite.Row
                    async with conn.execute(
                        "SELECT * FROM giveaway_archive_refs WHERE giveaway_id=?",
                        (giveaway_id,)
                    ) as cur:
                        row = await cur.fetchone()
                    if row:
                        giveaway = dict(row)
                        giveaway["is_active"] = False
                        giveaway["archived"] = True
        except Exception:
            pass

    if not giveaway:
        await message.answer(
            f"❌ Giveaway <code>{giveaway_id}</code> not found.",
            parse_mode="HTML"
        )
        return

    from handlers.admin import is_superadmin
    if giveaway.get("creator_id") != user_id and not is_superadmin(user_id):
        await message.answer("❌ You don't have access to this giveaway.")
        return

    status = "✅ Active" if giveaway.get("is_active") else ("📦 Archived" if giveaway.get("archived") else "🔒 Closed")
    prizes = giveaway.get("prizes", [])
    options = giveaway.get("options", [])
    prizes_text = "\n".join([f"  {i+1}. {p}" for i, p in enumerate(prizes)]) if prizes else "  —"
    options_text = "\n".join([f"  • {o}" for o in options]) if options else "  —"

    await message.answer(
        f"📊 <b>Giveaway Info</b>\n\n"
        f"🆔 ID: <code>{giveaway_id}</code>\n"
        f"📌 Title: <b>{giveaway.get('title', '—')}</b>\n"
        f"📢 Channel: {giveaway.get('channel_username', '—')}\n"
        f"🔘 Status: {status}\n"
        f"👥 Total Votes: <b>{giveaway.get('total_votes', 0)}</b>\n"
        f"📅 Created: {str(giveaway.get('created_at', '—'))[:10]}\n\n"
        f"🏆 Prizes:\n{prizes_text}\n\n"
        f"🗳 Options:\n{options_text}",
        parse_mode="HTML"
    )


# ─── /duplicategiveaway <id> ───────────────────────────────────
@router.message(Command("duplicategiveaway"))
async def cmd_duplicate_giveaway(message: Message, state: FSMContext):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer(
            "❌ Usage: /duplicategiveaway <code>&lt;giveaway_id&gt;</code>\n"
            "Example: /duplicategiveaway 9A31157D",
            parse_mode="HTML"
        )
        return

    giveaway_id = parts[1].upper()
    user_id = message.from_user.id
    giveaway = await get_giveaway(giveaway_id)

    if not giveaway:
        try:
            from utils.db import get_db, is_mongo, get_sqlite_path
            from config.settings import settings as _s
            import json as _json

            ref = None
            if is_mongo():
                db = get_db()
                ref = await db.giveaway_archive_refs.find_one({"giveaway_id": giveaway_id})
            else:
                import aiosqlite
                async with aiosqlite.connect(get_sqlite_path()) as conn:
                    conn.row_factory = aiosqlite.Row
                    async with conn.execute(
                        "SELECT * FROM giveaway_archive_refs WHERE giveaway_id=?",
                        (giveaway_id,)
                    ) as cur:
                        row = await cur.fetchone()
                    if row:
                        ref = dict(row)

            if ref and ref.get("file_id") and getattr(_s, "DATABASE_CHANNEL", None):
                file = await message.bot.get_file(ref["file_id"])
                file_bytes = await message.bot.download_file(file.file_path)
                data = _json.loads(file_bytes.read().decode())
                giveaway = data.get("giveaway", data)
        except Exception as e:
            logger.warning(f"duplicategiveaway: archive fetch failed: {e}")

    if not giveaway:
        await message.answer(
            f"❌ Giveaway <code>{giveaway_id}</code> not found.\n\n"
            "Note: Archived giveaways require <code>DATABASE_CHANNEL</code> to be set.",
            parse_mode="HTML"
        )
        return

    from handlers.admin import is_superadmin
    if giveaway.get("creator_id") != user_id and not is_superadmin(user_id):
        await message.answer("❌ You can only duplicate your own giveaways.")
        return

    await state.set_state(GiveawayForm.channel)
    await state.update_data(
        prefill_title=giveaway.get("title", ""),
        prefill_prizes=giveaway.get("prizes", []),
        prefill_options=giveaway.get("options", []),
        prefill_channel=giveaway.get("channel_username", ""),
    )

    await message.answer(
        f"♻️ <b>Duplicate Giveaway</b>\n\n"
        f"Loaded from <code>{giveaway_id}</code>:\n"
        f"📌 Title: <b>{giveaway.get('title', '—')}</b>\n"
        f"🏆 Prizes: <b>{len(giveaway.get('prizes', []))} prizes</b>\n"
        f"🗳 Options: <b>{len(giveaway.get('options', []))} options</b>\n\n"
        f"Now enter your channel username:\n"
        f"Example: <code>@mychannel</code>",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard()
    )


# ─── /mypremium ───────────────────────────────────────────────
@router.message(Command("mypremium"))
async def cmd_my_premium(message: Message):
    user_id = message.from_user.id
    try:
        from utils.premium import get_premium
        data = await get_premium(user_id)
    except Exception:
        data = None

    if not data or not data.get("is_active"):
        await message.answer(
            "💎 <b>My Premium</b>\n\n"
            "You don't have an active premium plan.\n\n"
            "Tap <b>Buy Premium</b> in the main menu to upgrade!",
            parse_mode="HTML"
        )
        return

    from datetime import datetime
    expires = data.get("expires_at")
    if expires:
        exp_str = str(expires)[:10]
        try:
            exp_dt = datetime.fromisoformat(str(expires).replace("Z", "")).replace(tzinfo=None)
            days_left = (exp_dt - datetime.utcnow()).days
        except Exception:
            days_left = "?"
    else:
        exp_str = "Lifetime"
        days_left = "∞"

    await message.answer(
        f"💎 <b>My Premium</b>\n\n"
        f"✅ Status: <b>Active</b>\n"
        f"📅 Expires: <b>{exp_str}</b>\n"
        f"⏳ Days left: <b>{days_left}</b>\n\n"
        f"Use /premiumbenefits to see what you get.",
        parse_mode="HTML"
    )


# ─── /premiumbenefits ─────────────────────────────────────────
@router.message(Command("premiumbenefits"))
async def cmd_premium_benefits(message: Message):
    await message.answer(
        "💎 <b>Premium Benefits</b>\n\n"
        "✦ <b>Unlimited giveaways</b> at once\n"
        "✦ <b>No bot watermark</b> on your giveaways\n"
        "✦ <b>Personal analytics dashboard</b>\n"
        "✦ <b>Channel growth tracking</b>\n"
        "✦ <b>Clone/Referral bot</b> access\n"
        "✦ <b>Priority support</b>\n"
        "✦ <b>Advanced stats</b> — votes, participants, top share\n"
        "✦ <b>Winner announcement</b> customization\n\n"
        "📌 Free users limited to <b>1 active giveaway</b> at a time.\n\n"
        "Tap <b>Buy Premium</b> in the main menu to upgrade! 🚀",
        parse_mode="HTML"
    )
