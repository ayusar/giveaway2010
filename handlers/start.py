from aiogram import Router
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

router = Router()

MAIN_BOT_WELCOME = (
    "👋 <b>Welcome to GiveawayBot!</b>\n\n"
    "🎁 <b>What can I do?</b>\n"
    "• Create live giveaway polls in your channel\n"
    "• Manage votes with channel-join verification\n"
    "• Clone me to run your own referral bot\n\n"
    "Use the buttons below to get started!"
)


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗳 Create Giveaway", callback_data="menu:create_giveaway")],
        [InlineKeyboardButton(text="🤖 Clone Refer Bot", callback_data="menu:clone_bot")],
        [InlineKeyboardButton(text="📊 My Giveaways", callback_data="menu:my_giveaways")],
        [InlineKeyboardButton(text="❓ Help", callback_data="menu:help")],
    ])


@router.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer(MAIN_BOT_WELCOME, reply_markup=main_menu_keyboard(), parse_mode="HTML")


@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "📖 <b>Help Guide</b>\n\n"
        "<b>Giveaway Poll:</b>\n"
        "1. Add this bot as admin in your channel\n"
        "2. Use /creategiveaway and follow the setup steps\n"
        "3. The poll posts in your channel with live vote bars\n"
        "4. Users must join your channel to vote\n\n"
        "<b>Clone Refer Bot:</b>\n"
        "1. Create a new bot via @BotFather\n"
        "2. Use /clonebot and paste your bot token\n"
        "3. Your bot is now live and tracks referrals!\n\n"
        "<b>Commands:</b>\n"
        "/creategiveaway — Start a new giveaway\n"
        "/mygiveaways — View your active giveaways\n"
        "/clonebot — Set up your referral clone bot\n",
        parse_mode="HTML"
    )
