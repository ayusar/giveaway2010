from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery

router = Router()


@router.message(Command("mygiveaways"))
@router.callback_query(F.data == "menu:my_giveaways")
async def my_giveaways(event):
    msg = event if isinstance(event, Message) else event.message
    if isinstance(event, CallbackQuery):
        await event.answer()

    from utils.db import get_db
    db = get_db()
    user_id = event.from_user.id
    cursor = db.giveaways.find({"creator_id": user_id}).sort("created_at", -1).limit(10)
    giveaways = await cursor.to_list(length=10)

    if not giveaways:
        await msg.answer("You haven't created any giveaways yet.\nUse /creategiveaway to start one!")
        return

    lines = ["📋 <b>Your Giveaways</b>\n"]
    for g in giveaways:
        status = "✅ Active" if g["is_active"] else "🔒 Closed"
        lines.append(
            f"• <code>{g['giveaway_id']}</code> — {g['title']}\n"
            f"  {status} | 👥 {g['total_votes']} votes"
        )

    await msg.answer("\n".join(lines), parse_mode="HTML")


@router.callback_query(F.data == "menu:help")
async def menu_help(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "📖 <b>Help Guide</b>\n\n"
        "<b>Giveaway Poll:</b>\n"
        "1. Add this bot as admin in your channel\n"
        "2. Use /creategiveaway and follow the steps\n"
        "3. The poll posts live with vote bars\n"
        "4. Users must join your channel to vote\n\n"
        "<b>Clone Refer Bot:</b>\n"
        "1. Create a bot via @BotFather\n"
        "2. Use /clonebot and paste your token\n"
        "3. Share your bot — it tracks referrals!\n\n"
        "<b>Commands:</b>\n"
        "/creategiveaway — New giveaway poll\n"
        "/mygiveaways — Your giveaways\n"
        "/clonebot — Set up referral bot\n"
        "/deleteclone — Remove your clone bot",
        parse_mode="HTML"
    )
