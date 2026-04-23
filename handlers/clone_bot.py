import asyncio
from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from models.referral import (
    create_clone_bot, get_clone_bot_by_owner, delete_clone_bot,
    update_clone_bot, DEFAULT_COMMANDS
)
from utils.clone_manager import get_clone_manager

router = Router()

ALL_USER_COMMANDS = ["refer", "mystats", "leaderboard", "myreferrals"]


class CloneBotForm(StatesGroup):
    token = State()
    channel_link = State()
    welcome_message = State()
    referral_caption = State()


def commands_keyboard(enabled: list) -> InlineKeyboardMarkup:
    buttons = []
    for cmd in ALL_USER_COMMANDS:
        icon = "✅" if cmd in enabled else "❌"
        buttons.append([InlineKeyboardButton(
            text=f"{icon} /{cmd}",
            callback_data=f"toggle_cmd:{cmd}"
        )])
    buttons.append([InlineKeyboardButton(text="💾 Save Settings", callback_data="save_cmd_settings")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.message(Command("clonebot"))
@router.callback_query(F.data == "menu:clone_bot")
async def start_clone_setup(event, state: FSMContext):
    msg = event if isinstance(event, Message) else event.message
    user_id = event.from_user.id
    if isinstance(event, CallbackQuery):
        await event.answer()

    existing = await get_clone_bot_by_owner(user_id)
    if existing:
        await msg.answer(
            f"⚠️ You already have a clone bot: @{existing.get('bot_username', 'unknown')}\n\n"
            f"Use /deleteclone to remove it first."
        )
        return

    await state.set_state(CloneBotForm.token)
    await msg.answer(
        "🤖 <b>Clone Bot Setup — Step 1/4</b>\n\n"
        "Create a new bot via @BotFather and paste the <b>bot token</b> here.\n\n"
        "⚠️ Never share your token with anyone!",
        parse_mode="HTML"
    )


@router.message(CloneBotForm.token)
async def form_clone_token(message: Message, state: FSMContext):
    token = message.text.strip()
    if ":" not in token or len(token) < 30:
        await message.answer("❌ That doesn't look like a valid bot token. Try again.")
        return
    try:
        test_bot = Bot(token=token)
        me = await test_bot.get_me()
        await test_bot.session.close()
    except Exception as e:
        await message.answer(f"❌ Invalid token or bot not accessible: {e}")
        return

    await state.update_data(token=token, bot_username=me.username)
    await state.set_state(CloneBotForm.channel_link)
    await message.answer(
        f"✅ Bot verified: @{me.username}\n\n"
        f"<b>Step 2/4 — Channel Link</b>\n\n"
        f"Enter your channel username (e.g. <code>@mychannel</code>).\n"
        f"Users must join this channel before using your bot.\n\n"
        f"Tap <b>Skip</b> if you don't want a join gate.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Skip", callback_data="skip_channel")]
        ])
    )


@router.callback_query(F.data == "skip_channel")
async def skip_channel(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.update_data(channel_link="")
    await _ask_welcome(callback.message, state)


@router.message(CloneBotForm.channel_link)
async def form_channel_link(message: Message, state: FSMContext):
    link = message.text.strip()
    if not link.startswith("@") and not link.startswith("https://t.me/"):
        await message.answer("❌ Please enter a valid channel like @mychannel or https://t.me/mychannel")
        return
    await state.update_data(channel_link=link)
    await _ask_welcome(message, state)


async def _ask_welcome(msg, state: FSMContext):
    await state.set_state(CloneBotForm.welcome_message)
    await msg.answer(
        "<b>Step 3/4 — Welcome Message</b>\n\n"
        "Enter the welcome message shown when users /start your bot.\n\n"
        "Tap <b>Use Default</b> to keep the standard message.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Use Default", callback_data="use_default_welcome")]
        ])
    )


@router.callback_query(F.data == "use_default_welcome")
async def use_default_welcome(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.update_data(welcome_message="")
    await _ask_caption(callback.message, state)


@router.message(CloneBotForm.welcome_message)
async def form_welcome(message: Message, state: FSMContext):
    await state.update_data(welcome_message=message.text.strip())
    await _ask_caption(message, state)


async def _ask_caption(msg, state: FSMContext):
    await state.set_state(CloneBotForm.referral_caption)
    await msg.answer(
        "<b>Step 4/4 — Referral Caption</b>\n\n"
        "Enter custom text shown with users' referral links.\n"
        "Example: <i>Invite friends and win prizes!</i>\n\n"
        "Tap <b>Use Default</b> to skip.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Use Default", callback_data="use_default_caption")]
        ])
    )


@router.callback_query(F.data == "use_default_caption")
async def use_default_caption(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.update_data(referral_caption="")
    await _finish_clone_setup(callback.message, state, callback.from_user.id)


@router.message(CloneBotForm.referral_caption)
async def form_caption(message: Message, state: FSMContext):
    await state.update_data(referral_caption=message.text.strip())
    await _finish_clone_setup(message, state, message.from_user.id)


async def _finish_clone_setup(msg, state: FSMContext, owner_id: int):
    data = await state.get_data()
    await state.clear()

    await create_clone_bot(
        owner_id=owner_id,
        token=data["token"],
        bot_username=data["bot_username"],
        welcome_message=data.get("welcome_message", ""),
        channel_link=data.get("channel_link", ""),
        referral_caption=data.get("referral_caption", ""),
        enabled_commands=DEFAULT_COMMANDS[:]
    )

    manager = get_clone_manager()
    await manager.start_clone(data["token"])

    # Create analytics panel
    channel_link = data.get("channel_link","")
    member_count = 0
    channel_title = ""
    if channel_link:
        try:
            from aiogram import Bot as _Bot
            tmp = _Bot(token=data["token"])
            chat = await tmp.get_chat(channel_link)
            member_count = await tmp.get_chat_member_count(channel_link)
            channel_title = chat.title or channel_link
            await tmp.session.close()
        except Exception:
            channel_title = channel_link

    from models.referral import get_clone_bot
    clone = await get_clone_bot(data["token"])
    from models.panel import create_panel
    panel = await create_panel(
        owner_id=owner_id,
        panel_type="refer",
        ref_id=data["token"],
        channel_id=channel_link,
        channel_username=channel_link,
        channel_title=channel_title,
        member_count_start=member_count
    )
    from config.settings import settings as _s
    panel_url = f"{_s.WEB_DOMAIN}/panel/{panel['token']}"

    await msg.answer(
        f"🎉 <b>Clone bot launched!</b>\n\n"
        f"🤖 Bot: @{data['bot_username']}\n"
        f"📢 Channel gate: {data.get('channel_link') or 'None (disabled)'}\n\n"
        f"📊 <b>Your Analytics Panel:</b>\n"
        f"<code>{panel_url}</code>\n\n"
        f"<b>User commands:</b>\n"
        f"/start — Welcome + join check\n"
        f"/refer — Personal referral link\n"
        f"/mystats — Personal stats\n"
        f"/leaderboard — Top referrers\n"
        f"/myreferrals — Who they referred\n\n"
        f"<b>Owner commands:</b>\n"
        f"/all — Full participant list\n"
        f"/broadcast — Message all users\n"
        f"/resetreferral — Reset a user's count\n"
        f"/exportusers — Download CSV\n"
        f"/botstats — Stats & daily joins\n"
        f"/togglecommands — Enable/disable user commands\n\n"
        f"🔗 Share: t.me/{data['bot_username']}",
        parse_mode="HTML"
    )


# ─── Toggle Commands ──────────────────────────────────────────

@router.message(Command("togglecommands"))
async def toggle_commands(message: Message):
    clone = await get_clone_bot_by_owner(message.from_user.id)
    if not clone:
        await message.answer("❌ You don't have an active clone bot.")
        return
    enabled = clone.get("enabled_commands", DEFAULT_COMMANDS)
    await message.answer(
        "⚙️ <b>Command Settings</b>\n\nToggle which commands users can access:",
        parse_mode="HTML",
        reply_markup=commands_keyboard(enabled)
    )


@router.callback_query(F.data.startswith("toggle_cmd:"))
async def handle_toggle_cmd(callback: CallbackQuery):
    cmd = callback.data.split(":")[1]
    clone = await get_clone_bot_by_owner(callback.from_user.id)
    if not clone:
        await callback.answer("No clone bot found.", show_alert=True)
        return
    enabled = list(clone.get("enabled_commands", DEFAULT_COMMANDS))
    if cmd in enabled:
        enabled.remove(cmd)
    else:
        enabled.append(cmd)
    await update_clone_bot(clone["token"], enabled_commands=enabled)
    await callback.message.edit_reply_markup(reply_markup=commands_keyboard(enabled))
    await callback.answer(f"{'✅ Enabled' if cmd in enabled else '❌ Disabled'}: /{cmd}")


@router.callback_query(F.data == "save_cmd_settings")
async def save_cmd_settings(callback: CallbackQuery):
    await callback.answer("✅ Settings saved!", show_alert=True)
    await callback.message.edit_reply_markup(reply_markup=None)


# ─── Delete Clone ─────────────────────────────────────────────

@router.message(Command("deleteclone"))
async def delete_clone(message: Message):
    existing = await get_clone_bot_by_owner(message.from_user.id)
    if not existing:
        await message.answer("You don't have an active clone bot.")
        return
    manager = get_clone_manager()
    await manager.stop_clone(existing["token"])
    await delete_clone_bot(message.from_user.id)
    await message.answer(f"✅ Clone bot @{existing.get('bot_username', '')} has been stopped and removed.")
