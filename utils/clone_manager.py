# utils/clone_manager.py
import asyncio
import csv
import io
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart, Command
from aiogram.fsm.storage.memory import MemoryStorage
from models.referral import (
    add_referral_user, get_referral_user, get_leaderboard,
    get_all_users_for_clone, get_top_referrer, get_clone_bot,
    get_referred_by_user, reset_referral_count, update_user_lang,
    update_clone_bot, DEFAULT_COMMANDS
)
from utils.languages import t
from config.settings import settings

logger = logging.getLogger(__name__)

MAIN_BOT_USERNAME = None  # Set at startup

DEFAULT_WELCOME_EN = (
    "👋 <b>Welcome!</b>\n\n"
    "Use /refer to get your referral link.\n"
    "📊 /leaderboard — Top referrers\n"
    "📈 /mystats — Your stats"
)

DEFAULT_CAPTION_EN = "Share your link and invite friends! 🚀"


def lang_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🇬🇧 English", callback_data="setlang:en"),
            InlineKeyboardButton(text="🇮🇳 हिंदी", callback_data="setlang:hi"),
        ]
    ])


def join_keyboard(channel_link: str, lang: str) -> InlineKeyboardMarkup:
    # Ensure URL is valid for Telegram button (must start with https://)
    url = channel_link.strip()
    if url.startswith("@"):
        url = f"https://t.me/{url[1:]}"
    elif not url.startswith("http"):
        url = f"https://t.me/{url}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "join_btn"), url=url)],
        [InlineKeyboardButton(text=t(lang, "verify_btn"), callback_data="verify_channel")]
    ])


async def _check_channel_membership(bot: Bot, channel_link: str, user_id: int) -> bool:
    """Returns True if user is a member of the channel."""
    if not channel_link:
        return True
    try:
        raw = channel_link.strip()
        # Private invite link (t.me/+xxx) — cannot check membership, let through
        if "t.me/+" in raw or "telegram.me/+" in raw:
            return True
        # Numeric ID e.g. -1001234567890
        if raw.lstrip("-").isdigit():
            member = await bot.get_chat_member(int(raw), user_id)
        else:
            # Public @username or https://t.me/username
            username = raw.replace("https://t.me/", "").replace("http://t.me/", "").replace("@", "").strip("/")
            member = await bot.get_chat_member(f"@{username}", user_id)
        return member.status not in ("left", "kicked")
    except Exception:
        return False  # Can't check → treat as not member, show force join


def build_clone_router(clone_token: str, main_bot_username: str) -> Router:
    router = Router()

    async def _get_lang(user_id: int) -> str:
        user = await get_referral_user(clone_token, user_id)
        return user.get("lang", "en") if user else "en"

    async def _is_cmd_enabled(cmd: str) -> bool:
        clone = await get_clone_bot(clone_token)
        enabled = clone.get("enabled_commands", DEFAULT_COMMANDS) if clone else DEFAULT_COMMANDS
        return cmd in enabled

    def _powered_by(lang: str, main_uname: str) -> str:
        return t(lang, "powered_by", main_bot=f"@{main_uname}")

    # ── /start ────────────────────────────────────────────────

    @router.message(CommandStart())
    async def clone_start(message: Message, bot: Bot):
        clone = await get_clone_bot(clone_token)
        if not clone:
            return

        args = message.text.split()
        referred_by = None
        if len(args) > 1:
            try:
                ref_id = int(args[1])
                if ref_id != message.from_user.id:
                    referred_by = ref_id
            except ValueError:
                pass

        # Check if user is already registered — RESTORE their session on restart
        existing = await get_referral_user(clone_token, message.from_user.id)
        lang = existing.get("lang", "en") if existing else "en"
        welcome = clone.get("welcome_message") or DEFAULT_WELCOME_EN
        footer = _powered_by(lang, main_bot_username)

        # Channel join gate — check FIRST before anything else
        channel_link = clone.get("channel_link", "")
        if channel_link:
            is_member = await _check_channel_membership(bot, channel_link, message.from_user.id)
            if not is_member:
                await message.answer(
                    t(lang, "not_joined") + _powered_by(lang, main_bot_username),
                    reply_markup=join_keyboard(channel_link, lang),
                    parse_mode="HTML"
                )
                return  # Do NOT register until they actually join

        # Returning user after restart — restore their state, skip re-registration flow
        if existing and not referred_by:
            await message.answer(
                welcome + footer,
                parse_mode="HTML"
            )
            return

        is_new = await add_referral_user(
            clone_token, message.from_user.id,
            message.from_user.full_name, referred_by, lang
        )

        # Log new user to LOG_CHANNEL via main bot
        if is_new:
            try:
                from utils.log_utils import log_new_user, get_main_bot
                main_bot = get_main_bot()
                if main_bot:
                    me_info = await bot.get_me()
                    reported_by = f"@{me_info.username} (clone)"
                    dc_id = getattr(message.from_user, "dc_id", None)
                    await log_new_user(
                        user_id=message.from_user.id,
                        first_name=message.from_user.first_name or "",
                        last_name=message.from_user.last_name,
                        username=message.from_user.username,
                        dc_id=dc_id,
                        reported_by=reported_by,
                    )
            except Exception:
                pass  # Never block user flow due to logging failure

        # Notify referrer when someone joins via their link
        if is_new and referred_by:
            username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.full_name
            try:
                updated_referrer = await get_referral_user(clone_token, referred_by)
                new_count = updated_referrer.get("refer_count", 0) if updated_referrer else 0
                await bot.send_message(
                    referred_by,
                    f"🎉 <b>New Referral!</b>\n\n"
                    f"👤 {username} joined using your link.\n"
                    f"🔗 Your total referrals: <b>{new_count}</b>",
                    parse_mode="HTML"
                )
            except Exception:
                pass  # Referrer may have blocked the bot

        async def _send_welcome(target_msg, w_text, w_footer, w_lang, w_clone):
            image_id = w_clone.get("welcome_image") if w_clone else None
            if image_id:
                await target_msg.answer_photo(
                    image_id,
                    caption=w_text + w_footer,
                    parse_mode="HTML"
                )
            else:
                await target_msg.answer(w_text + w_footer, parse_mode="HTML")

        if not existing:
            # Brand new user — ask language first
            await message.answer(
                t(lang, "choose_language"),
                reply_markup=lang_keyboard()
            )
        else:
            await _send_welcome(message, welcome, footer, lang, clone)

    # ── Language selection ─────────────────────────────────────

    @router.callback_query(F.data.startswith("setlang:"))
    async def set_language(callback: CallbackQuery, bot: Bot):
        lang = callback.data.split(":")[1]
        await update_user_lang(clone_token, callback.from_user.id, lang)
        await callback.answer(t(lang, "language_set"), show_alert=True)

        clone = await get_clone_bot(clone_token)
        welcome = clone.get("welcome_message") or DEFAULT_WELCOME_EN
        footer = _powered_by(lang, main_bot_username)
        await callback.message.edit_text(
            welcome + footer,
            parse_mode="HTML"
        )

    # ── Channel verify ─────────────────────────────────────────

    @router.callback_query(F.data == "verify_channel")
    async def verify_channel(callback: CallbackQuery, bot: Bot):
        clone = await get_clone_bot(clone_token)
        channel_link = clone.get("channel_link", "") if clone else ""
        lang = await _get_lang(callback.from_user.id)

        is_member = await _check_channel_membership(bot, channel_link, callback.from_user.id)
        if not is_member:
            await callback.answer(t(lang, "not_verified"), show_alert=True)
            return

        # ✅ Register user in DB now that they have joined
        await add_referral_user(
            clone_token, callback.from_user.id,
            callback.from_user.full_name, None, lang
        )

        await callback.answer(t(lang, "verified"), show_alert=True)
        welcome = clone.get("welcome_message") or DEFAULT_WELCOME_EN
        footer = _powered_by(lang, main_bot_username)

        # Ask language preference for new users
        existing = await get_referral_user(clone_token, callback.from_user.id)
        if existing and existing.get("lang") == "en":
            await callback.message.edit_text(t(lang, "choose_language"), reply_markup=lang_keyboard())
        else:
            await callback.message.edit_text(welcome + footer, parse_mode="HTML")

    # ── /refer ────────────────────────────────────────────────

    @router.message(Command("refer"))
    async def clone_refer(message: Message, bot: Bot):
        lang = await _get_lang(message.from_user.id)
        if not await _is_cmd_enabled("refer"):
            await message.answer(t(lang, "cmd_disabled"))
            return
        me = await bot.get_me()
        user = await get_referral_user(clone_token, message.from_user.id)
        if not user:
            await message.answer(t(lang, "not_started"))
            return
        count = user.get("refer_count", 0)
        link = f"https://t.me/{me.username}?start={message.from_user.id}"
        clone = await get_clone_bot(clone_token)
        caption = clone.get("referral_caption") or DEFAULT_CAPTION_EN
        footer = _powered_by(lang, main_bot_username)
        await message.answer(
            t(lang, "refer_msg", link=link, caption=caption, count=count) + footer,
            parse_mode="HTML"
        )

    # ── /mystats ──────────────────────────────────────────────

    @router.message(Command("mystats"))
    async def clone_mystats(message: Message):
        lang = await _get_lang(message.from_user.id)
        if not await _is_cmd_enabled("mystats"):
            await message.answer(t(lang, "cmd_disabled"))
            return
        user = await get_referral_user(clone_token, message.from_user.id)
        if not user:
            await message.answer(t(lang, "not_started"))
            return
        top = await get_top_referrer(clone_token)
        top_count = top.get("refer_count", 0) if top else 0
        footer = _powered_by(lang, main_bot_username)
        await message.answer(
            t(lang, "mystats", name=user["user_name"],
              count=user.get("refer_count", 0), top=top_count) + footer,
            parse_mode="HTML"
        )

    # ── /myreferrals ──────────────────────────────────────────

    @router.message(Command("myreferrals"))
    async def clone_myreferrals(message: Message):
        lang = await _get_lang(message.from_user.id)
        if not await _is_cmd_enabled("myreferrals"):
            await message.answer(t(lang, "cmd_disabled"))
            return
        referred = await get_referred_by_user(clone_token, message.from_user.id)
        footer = _powered_by(lang, main_bot_username)
        if not referred:
            await message.answer(t(lang, "no_referrals") + footer, parse_mode="HTML")
            return
        lines = [t(lang, "myreferrals_header", count=len(referred))]
        for i, u in enumerate(referred, 1):
            lines.append(f"{i}. {u['user_name']}")
        await message.answer("\n".join(lines) + footer, parse_mode="HTML")

    # ── /leaderboard ──────────────────────────────────────────

    @router.message(Command("leaderboard"))
    async def clone_leaderboard(message: Message):
        lang = await _get_lang(message.from_user.id)
        if not await _is_cmd_enabled("leaderboard"):
            await message.answer(t(lang, "cmd_disabled"))
            return
        users, total = await get_leaderboard(clone_token, page=1, per_page=10)
        if not users:
            await message.answer("No participants yet.")
            return
        top = users[0]
        medals = ["👑", "🥈", "🥉"]
        lines = [
            f"👥 <b>Top Referrers</b>",
            f"Total: {total} | 🏆 Leader has {top.get('refer_count',0)} referrals",
            "━━━━━━━━━━━━━━━━━━━━━"
        ]
        for i, u in enumerate(users):
            icon = medals[i] if i < 3 else "▫️"
            lines.append(f"{icon} {i+1}. {u['user_name']} ({u.get('refer_count',0)})")
        footer = _powered_by(lang, main_bot_username)
        await message.answer("\n".join(lines) + footer, parse_mode="HTML")

    # ── /all (owner) ──────────────────────────────────────────

    @router.message(Command("all"))
    async def clone_all(message: Message):
        clone = await get_clone_bot(clone_token)
        if not clone or message.from_user.id != clone["owner_id"]:
            await message.answer("❌ Owner only command.")
            return
        users, total = await get_leaderboard(clone_token, page=1, per_page=50)
        top = users[0] if users else None
        top_count = top.get("refer_count", 0) if top else 0
        medals = ["👑", "🥈", "🥉"]
        lines = [
            f"👥 <b>All Participants</b>",
            f"Total: {total} | 🏆 Leader has {top_count} referrals",
            "━━━━━━━━━━━━━━━━━━━━━"
        ]
        for i, u in enumerate(users):
            icon = medals[i] if i < 3 else "▫️"
            lines.append(f"{icon} {i+1}. {u['user_name']} ({u.get('refer_count',0)})")
        if total > 50:
            lines.append(f"\n... and {total-50} more")
        text = "\n".join(lines)
        for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
            await message.answer(chunk, parse_mode="HTML")

    # ── /resetreferral (owner) ────────────────────────────────

    @router.message(Command("resetreferral"))
    async def clone_reset_referral(message: Message):
        clone = await get_clone_bot(clone_token)
        if not clone or message.from_user.id != clone["owner_id"]:
            await message.answer("❌ Owner only command.")
            return
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer("Usage: /resetreferral <user_id>")
            return
        try:
            target_id = int(parts[1])
        except ValueError:
            await message.answer("❌ Invalid user ID.")
            return
        user = await get_referral_user(clone_token, target_id)
        if not user:
            await message.answer("❌ User not found in this bot.")
            return
        await reset_referral_count(clone_token, target_id)
        await message.answer(f"✅ Referral count reset for <code>{target_id}</code> ({user['user_name']}).", parse_mode="HTML")

    # ── /botstats (owner) ─────────────────────────────────────

    @router.message(Command("botstats"))
    async def clone_botstats(message: Message):
        clone = await get_clone_bot(clone_token)
        if not clone or message.from_user.id != clone["owner_id"]:
            await message.answer("❌ Owner only command.")
            return
        all_users = await get_all_users_for_clone(clone_token)
        total = len(all_users)
        top = await get_top_referrer(clone_token)
        top_name = top["user_name"] if top else "N/A"
        top_count = top.get("refer_count", 0) if top else 0

        # Daily joins last 7 days
        recent = await get_daily_joins_local(clone_token)
        daily = defaultdict(int)
        for u in recent:
            joined = u.get("joined_at", "")
            if joined:
                try:
                    day = joined[:10]
                    daily[day] += 1
                except Exception:
                    pass

        daily_lines = ""
        for i in range(6, -1, -1):
            day = (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
            count = daily.get(day, 0)
            bar = "█" * min(count, 10) + "░" * max(0, 10 - min(count, 10))
            daily_lines += f"\n{day[-5:]}  {bar} {count}"

        await message.answer(
            f"📊 <b>Bot Stats</b>\n\n"
            f"👥 Total users: <b>{total}</b>\n"
            f"🏆 Top referrer: <b>{top_name}</b> ({top_count} refs)\n\n"
            f"<b>Daily Joins (last 7 days):</b>"
            f"<code>{daily_lines}</code>",
            parse_mode="HTML"
        )

    # ── /exportusers (owner) ──────────────────────────────────

    @router.message(Command("exportusers"))
    async def clone_export(message: Message, bot: Bot):
        clone = await get_clone_bot(clone_token)
        if not clone or message.from_user.id != clone["owner_id"]:
            await message.answer("❌ Owner only command.")
            return
        users = await get_all_users_for_clone(clone_token)
        if not users:
            await message.answer("No users to export.")
            return
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["user_id", "user_name", "refer_count", "referred_by", "joined_at"])
        writer.writeheader()
        for u in users:
            writer.writerow({
                "user_id": u.get("user_id", ""),
                "user_name": u.get("user_name", ""),
                "refer_count": u.get("refer_count", 0),
                "referred_by": u.get("referred_by", ""),
                "joined_at": str(u.get("joined_at", ""))[:19]
            })
        csv_bytes = output.getvalue().encode("utf-8")
        from aiogram.types import BufferedInputFile
        await bot.send_document(
            message.chat.id,
            BufferedInputFile(csv_bytes, filename="users_export.csv"),
            caption=f"📋 Users export — {len(users)} users"
        )

    # ── /banuser (owner) ─────────────────────────────────────

    @router.message(Command("banuser"))
    async def clone_ban_user(message: Message):
        clone = await get_clone_bot(clone_token)
        if not clone or message.from_user.id != clone["owner_id"]:
            await message.answer("❌ Owner only command.")
            return
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer("Usage: /banuser <user_id>")
            return
        try:
            target_id = int(parts[1])
        except ValueError:
            await message.answer("❌ Invalid user ID.")
            return
        from utils.db import get_db, is_mongo, get_sqlite_path
        if is_mongo():
            await get_db().clone_banned.update_one(
                {"clone_token": clone_token, "user_id": target_id},
                {"$set": {"clone_token": clone_token, "user_id": target_id}},
                upsert=True
            )
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                await conn.execute(
                    "CREATE TABLE IF NOT EXISTS clone_banned "
                    "(clone_token TEXT, user_id INTEGER, PRIMARY KEY(clone_token, user_id))"
                )
                await conn.execute(
                    "INSERT OR IGNORE INTO clone_banned (clone_token, user_id) VALUES (?,?)",
                    (clone_token, target_id)
                )
                await conn.commit()
        await message.answer(f"✅ User <code>{target_id}</code> banned from this bot.", parse_mode="HTML")

    # ── /setwelcomeimage (owner) ──────────────────────────────

    @router.message(Command("setwelcomeimage"))
    async def clone_set_welcome_image(message: Message, bot: Bot):
        clone = await get_clone_bot(clone_token)
        if not clone or message.from_user.id != clone["owner_id"]:
            await message.answer("❌ Owner only command.")
            return
        if not message.reply_to_message or not message.reply_to_message.photo:
            await message.answer(
                "📸 Reply to a photo with /setwelcomeimage to set it as the welcome image.\n"
                "Use /clearwelcomeimage to remove it."
            )
            return
        photo = message.reply_to_message.photo[-1]
        await update_clone_bot(clone_token, welcome_image=photo.file_id)
        await message.answer("✅ Welcome image set! Users will see it when they /start.")

    @router.message(Command("clearwelcomeimage"))
    async def clone_clear_welcome_image(message: Message):
        clone = await get_clone_bot(clone_token)
        if not clone or message.from_user.id != clone["owner_id"]:
            await message.answer("❌ Owner only command.")
            return
        await update_clone_bot(clone_token, welcome_image=None)
        await message.answer("✅ Welcome image cleared.")

    # ── /schedulebroadcast (owner) ────────────────────────────

    @router.message(Command("schedulebroadcast"))
    async def clone_schedule_broadcast(message: Message, bot: Bot):
        clone = await get_clone_bot(clone_token)
        if not clone or message.from_user.id != clone["owner_id"]:
            await message.answer("❌ Owner only command.")
            return
        # Format: /schedulebroadcast 2h Your message here
        parts = message.text.split(maxsplit=2)
        if len(parts) < 3:
            await message.answer(
                "Usage: /schedulebroadcast <delay> <message>\n"
                "Example: /schedulebroadcast 2h Hello everyone!"
            )
            return
        delay_str, text = parts[1], parts[2]
        # Parse delay
        import re
        match = re.match(r"(\d+)(h|m|d)", delay_str.lower())
        if not match:
            await message.answer("❌ Invalid delay. Use 2h, 30m, 1d")
            return
        val, unit = int(match.group(1)), match.group(2)
        secs = val * {"h": 3600, "m": 60, "d": 86400}[unit]
        await message.answer(f"✅ Broadcast scheduled in {delay_str}!")
        asyncio.create_task(_delayed_broadcast(bot, clone_token, text, secs))

    async def _delayed_broadcast(bot, token, text, delay_secs):
        await asyncio.sleep(delay_secs)
        users = await get_all_users_for_clone(token)
        sent = failed = 0
        for u in users:
            try:
                await bot.send_message(u["user_id"], f"📢 <b>Announcement</b>\n\n{text}", parse_mode="HTML")
                sent += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.05)

    # ── /broadcast (owner) ────────────────────────────────────

    @router.message(Command("broadcast"))
    async def clone_broadcast(message: Message, bot: Bot):
        clone = await get_clone_bot(clone_token)
        if not clone or message.from_user.id != clone["owner_id"]:
            await message.answer("❌ Owner only command.")
            return
        text = message.text.partition(" ")[2].strip()
        if not text:
            await message.answer("Usage: /broadcast <message>")
            return
        all_users = await get_all_users_for_clone(clone_token)
        sent, failed = 0, 0
        for u in all_users:
            try:
                await bot.send_message(u["user_id"], f"📢 <b>Announcement</b>\n\n{text}", parse_mode="HTML")
                sent += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.05)
        await message.answer(f"✅ Done!\n📤 Sent: {sent} | ❌ Failed: {failed}")

    return router


async def get_daily_joins_local(clone_token):
    from models.referral import get_daily_joins
    return await get_daily_joins(clone_token)


# ══════════════════════════════════════════════════════════════
# CLONE MANAGER
# ══════════════════════════════════════════════════════════════

class CloneManager:
    def __init__(self):
        self.running_clones: dict = {}

    async def start_all_clones(self):
        from utils.db import get_db, is_mongo
        if is_mongo():
            db = get_db()
            clones = await db.clone_bots.find({"is_active": True, "is_banned": False}).to_list(length=None)
        else:
            import aiosqlite
            from utils.db import get_sqlite_path
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(
                    "SELECT * FROM clone_bots WHERE is_active=1 AND is_banned=0"
                ) as cur:
                    rows = await cur.fetchall()
            clones = [dict(r) for r in rows]

        logger.info(f"Starting {len(clones)} clone bot(s)...")
        for clone in clones:
            await self.start_clone(clone["token"])

    async def start_clone(self, token: str):
        if token in self.running_clones:
            return
        try:
            bot = Bot(token=token)
            me = await bot.get_me()
            main_uname = MAIN_BOT_USERNAME or "MainBot"

            # Clear webhook and pending updates to avoid TelegramConflictError
            try:
                await bot.delete_webhook(drop_pending_updates=True)
            except Exception:
                pass

            storage = MemoryStorage()
            dp = Dispatcher(storage=storage)
            dp.include_router(build_clone_router(token, main_uname))

            task = asyncio.create_task(
                dp.start_polling(bot, allowed_updates=["message", "callback_query"], drop_pending_updates=True)
            )
            self.running_clones[token] = (bot, dp, task)
            logger.info(f"✅ Clone bot started: @{me.username}")
        except Exception as e:
            logger.error(f"❌ Failed to start clone {token[:10]}...: {e}")

    async def stop_clone(self, token: str):
        if token not in self.running_clones:
            return
        bot, dp, task = self.running_clones.pop(token)
        task.cancel()
        try:
            await bot.session.close()
        except Exception:
            pass
        logger.info(f"🛑 Clone bot stopped: {token[:10]}...")


_clone_manager = CloneManager()


def get_clone_manager() -> CloneManager:
    return _clone_manager
