from aiogram import Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

router = Router()

MAIN_BOT_WELCOME = (
    "👑 <b>Welcome to RoyalityGiveawayBot!</b>\n\n"
    "🎁 <b>What can I do for you?</b>\n"
    "• Run live Poll Giveaways in your channel\n"
    "• Host Referral Giveaways to grow your audience\n"
    "• Manage winners, prizes & more — all automated\n\n"
    "🔥 Upgrade to <b>Premium</b> for unlimited giveaways & no bot stamp!\n\n"
    "Use the buttons below to get started 👇"
)


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Create Giveaway", callback_data="menu:create_giveaway_type")],
        [InlineKeyboardButton(text="📋 My Giveaways", callback_data="menu:my_giveaways")],
        [
            InlineKeyboardButton(text="❓ Help", callback_data="menu:help"),
            InlineKeyboardButton(text="💎 Buy Premium", callback_data="menu:buy_premium"),
        ],
        [
            InlineKeyboardButton(text="🛠 Support", callback_data="menu:support"),
            InlineKeyboardButton(text="📢 Channel", callback_data="menu:channel"),
        ],
    ])


def _force_join_keyboard(channel_link: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Join Channel", url=channel_link)],
        [InlineKeyboardButton(text="✅ I've Joined", callback_data="menu:create_giveaway")],
    ])


@router.message(CommandStart())
async def cmd_start(message: Message):
    user = message.from_user
    try:
        from utils.log_utils import log_new_user, get_main_bot
        bot = get_main_bot()
        if bot:
            me = await bot.get_me()
            reported_by = f"@{me.username}"
        else:
            reported_by = "MainBot"
        dc_id = getattr(user, "dc_id", None)
        await log_new_user(
            user_id=user.id,
            first_name=user.first_name or "",
            last_name=user.last_name,
            username=user.username,
            dc_id=dc_id,
            reported_by=reported_by,
        )
    except Exception:
        pass

    await message.answer(MAIN_BOT_WELCOME, reply_markup=main_menu_keyboard(), parse_mode="HTML")


@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "❓ <b>Help Guide</b>\n\n"
        "🗳 <b>Poll Giveaway</b>\n"
        "• Add this bot as <b>admin</b> in your channel\n"
        "• Tap <b>Create Giveaway → Poll Giveaway</b>\n"
        "• Set your channel, title, prizes & options\n"
        "• The poll goes live with real-time vote bars\n"
        "• Voters must join your channel to participate\n\n"
        "🔗 <b>Refer Giveaway</b>\n"
        "• Tap <b>Create Giveaway → Refer Giveaway</b>\n"
        "• Participants earn entries by referring friends\n"
        "• Winner is picked automatically at end time\n\n"
        "📋 <b>My Giveaways</b>\n"
        "• View all your active & past giveaways\n"
        "• Check vote counts, status & winner history\n\n"
        "💎 <b>Premium</b>\n"
        "• No bot stamp on your giveaways\n"
        "• Unlimited giveaways at once\n"
        "• Priority support\n"
        "• Tap <b>Buy Premium</b> to upgrade\n\n"
        "⚙️ <b>Commands</b>\n"
        "/creategiveaway — Start a new giveaway\n"
        "/mygiveaways — View your giveaways\n"
        "/mystats — Your personal stats\n"
        "/mypanel — Get your panel links\n"
        "/start — Return to main menu",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


@router.callback_query(F.data == "menu:help")
async def menu_help_callback(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "❓ <b>Help Guide</b>\n\n"
        "🗳 <b>Poll Giveaway</b>\n"
        "• Add this bot as <b>admin</b> in your channel\n"
        "• Tap <b>Create Giveaway → Poll Giveaway</b>\n"
        "• Set your channel, title, prizes & options\n"
        "• The poll goes live with real-time vote bars\n"
        "• Voters must join your channel to participate\n\n"
        "🔗 <b>Refer Giveaway</b>\n"
        "• Tap <b>Create Giveaway → Refer Giveaway</b>\n"
        "• Participants earn entries by referring friends\n"
        "• Winner is picked automatically at end time\n\n"
        "📋 <b>My Giveaways</b>\n"
        "• View all your active & past giveaways\n"
        "• Check vote counts, status & winner history\n\n"
        "💎 <b>Premium</b>\n"
        "• No bot stamp on your giveaways\n"
        "• Unlimited giveaways at once\n"
        "• Priority support\n"
        "• Tap <b>Buy Premium</b> to upgrade\n\n"
        "⚙️ <b>Commands</b>\n"
        "/creategiveaway — Start a new giveaway\n"
        "/mygiveaways — View your giveaways\n"
        "/mystats — Your personal stats\n"
        "/mypanel — Get your panel links\n"
        "/start — Return to main menu",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


@router.callback_query(F.data == "menu:buy_premium")
async def menu_buy_premium(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "💎 <b>Buy Premium</b>\n\n"
        "Tap the button below to contact our admin and purchase a premium plan.\n\n"
        "You'll be redirected to <b>@Codesfevers</b> with your interest message ready to send.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="💎 Buy Premium",
                url="https://t.me/Codesfevers?text=I am intrested in buy premium"
            )],
            [InlineKeyboardButton(text="🔙 Back", callback_data="menu:back")],
        ])
    )


@router.callback_query(F.data == "menu:support")
async def menu_support(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "🛠 <b>Support</b>\n\n"
        "Need help? Join our support group and our team will assist you as soon as possible!",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛠 Join Support Group", url="https://t.me/RoyalityDiscussion")],
            [InlineKeyboardButton(text="🔙 Back", callback_data="menu:back")],
        ])
    )


@router.callback_query(F.data == "menu:channel")
async def menu_channel(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "📢 <b>Official Channel</b>\n\n"
        "Stay updated with the latest news, updates & announcements from us!",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Join @royalitybots", url="https://t.me/royalitybots")],
            [InlineKeyboardButton(text="🔙 Back", callback_data="menu:back")],
        ])
    )


@router.callback_query(F.data == "menu:back")
async def menu_back(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(MAIN_BOT_WELCOME, reply_markup=main_menu_keyboard(), parse_mode="HTML")


@router.message(Command("mystats"))
async def cmd_mystats(message: Message):
    """Show personal stats for the current user in the main bot."""
    user_id = message.from_user.id
    from utils.db import get_db, is_mongo, get_sqlite_path

    try:
        if is_mongo():
            db = get_db()
            total_giveaways = await db.giveaways.count_documents({"owner_id": user_id})
            active_giveaways = await db.giveaways.count_documents({"owner_id": user_id, "is_active": True})
            closed_giveaways = await db.giveaways.count_documents({"owner_id": user_id, "is_active": False})
            # Count total votes across user's giveaways
            total_votes = 0
            async for g in db.giveaways.find({"owner_id": user_id}, {"total_votes": 1}):
                total_votes += g.get("total_votes", 0)
            # Clone bot info
            clone = await db.clone_bots.find_one({"owner_id": user_id, "is_active": True})
            clone_username = f"@{clone['bot_username']}" if clone else "None"
            # Referral count
            total_referrals = await db.referrals.count_documents({"referred_by": user_id})
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                async def cnt(q, p=()):
                    async with conn.execute(q, p) as c:
                        return (await c.fetchone())[0]
                total_giveaways  = await cnt("SELECT COUNT(*) FROM giveaways WHERE owner_id=?", (user_id,))
                active_giveaways = await cnt("SELECT COUNT(*) FROM giveaways WHERE owner_id=? AND is_active=1", (user_id,))
                closed_giveaways = await cnt("SELECT COUNT(*) FROM giveaways WHERE owner_id=? AND is_active=0", (user_id,))
                try:
                    total_votes = await cnt("SELECT COALESCE(SUM(total_votes),0) FROM giveaways WHERE owner_id=?", (user_id,))
                except Exception:
                    total_votes = 0
                try:
                    conn.row_factory = aiosqlite.Row
                    async with conn.execute(
                        "SELECT bot_username FROM clone_bots WHERE owner_id=? AND is_active=1 LIMIT 1", (user_id,)
                    ) as cur:
                        row = await cur.fetchone()
                    clone_username = f"@{row['bot_username']}" if row else "None"
                except Exception:
                    clone_username = "None"
                try:
                    total_referrals = await cnt("SELECT COUNT(*) FROM referrals WHERE referred_by=?", (user_id,))
                except Exception:
                    total_referrals = 0

        # Check premium status
        from utils.premium import is_premium, get_premium_info
        premium = await is_premium(user_id)
        prem_info = await get_premium_info(user_id)
        prem_status = "⭐ Active" if premium else "❌ Not Premium"
        prem_expiry = ""
        if prem_info:
            prem_expiry = f"\n⏳ Expires: <b>{str(prem_info.get('expires_at',''))[:10]}</b>"

        await message.answer(
            "📊 <b>Your Stats</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 <b>User ID:</b> <code>{user_id}</code>\n"
            f"🎁 <b>Total Giveaways:</b> <code>{total_giveaways}</code>\n"
            f"🟢 <b>Active Giveaways:</b> <code>{active_giveaways}</code>\n"
            f"🔒 <b>Closed Giveaways:</b> <code>{closed_giveaways}</code>\n"
            f"🗳 <b>Total Votes Received:</b> <code>{total_votes}</code>\n"
            f"🤖 <b>Clone Bot:</b> {clone_username}\n"
            f"🔗 <b>Referrals Made:</b> <code>{total_referrals}</code>\n"
            f"💎 <b>Premium:</b> {prem_status}{prem_expiry}",
            parse_mode="HTML",
        )
    except Exception as e:
        await message.answer(f"❌ Error fetching stats: <code>{e}</code>", parse_mode="HTML")


@router.message(Command("mypanel"))
async def cmd_mypanel_main(message: Message):
    """Retrieve all panel URLs for the current user."""
    user_id = message.from_user.id
    from utils.db import get_db, is_mongo, get_sqlite_path
    from config.settings import settings as _settings
    domain = getattr(_settings, "WEB_DOMAIN", "")

    try:
        if is_mongo():
            db = get_db()
            panels = await db.panels.find({"owner_id": user_id, "is_deleted": False}).to_list(None)
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                try:
                    async with conn.execute(
                        "SELECT * FROM panels WHERE owner_id=? AND is_deleted=0", (user_id,)
                    ) as cur:
                        rows = await cur.fetchall()
                    panels = [dict(r) for r in rows]
                except Exception:
                    panels = []

        if not panels:
            await message.answer(
                "📋 You have no active panels yet.\n\n"
                "Create a giveaway or referral campaign to get your panel link!",
                parse_mode="HTML"
            )
            return

        lines = [f"🔗 <b>Your Panels ({len(panels)}):</b>\n"]
        for p in panels[:10]:
            token = p.get("token", "")
            title = p.get("channel_title") or p.get("channel_username") or "Panel"
            ptype = p.get("panel_type", "referral")
            url = f"{domain}/panel/{token}" if domain else f"/panel/{token}"
            lines.append(f"• <b>{title}</b> [{ptype}]\n  🔗 <code>{url}</code>")

        await message.answer("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Error: <code>{e}</code>", parse_mode="HTML")


@router.callback_query(F.data == "menu:create_giveaway_type")
async def menu_create_giveaway_type(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "🎁 <b>Create Giveaway</b>\n\n"
        "Choose the type of giveaway you want to run:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗳 Poll Giveaway", callback_data="menu:create_giveaway")],
            [InlineKeyboardButton(text="🔗 Refer Giveaway", callback_data="menu:refer_giveaway")],
            [InlineKeyboardButton(text="🔙 Back", callback_data="menu:back")],
        ])
    )
