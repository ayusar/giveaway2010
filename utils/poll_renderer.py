from typing import List, Dict, Optional
from datetime import datetime
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def render_poll_bar(percent: float, width: int = 8) -> str:
    filled = round((percent / 100) * width)
    return "█" * filled + "░" * (width - filled)


def render_giveaway_message(
    title: str,
    prizes: List[str],
    options: List[str],
    votes: Dict[int, int],
    total_votes: int,
    is_active: bool = True,
    end_time: Optional[datetime] = None,
    hide_stamp: bool = False,
) -> str:
    medals = ["🥇", "🥈", "🥉"]
    prizes_text = "\n".join(
        f"  {medals[i] if i < 3 else f'{i+1}.'} {prize}"
        for i, prize in enumerate(prizes)
    )

    lines = [f"🗳 <b>{title}</b>\n", "<b>Prizes:</b>", prizes_text, ""]

    for i, option in enumerate(options):
        count = votes.get(i, 0)
        percent = round(count / total_votes * 100) if total_votes > 0 else 0
        bar = render_poll_bar(percent)
        lines.append(f"{bar} {option} — {percent}% ({count})")

    lines.append(f"\n👥 <b>{total_votes}</b> votes cast")

    if end_time and is_active:
        lines.append(f"⏰ Ends: {end_time.strftime('%Y-%m-%d %H:%M')} UTC")

    lines.append("🔒 Poll closed" if not is_active else "✅ Poll active — tap an option to vote!")

    # Show bot stamp unless creator has premium
    if not hide_stamp:
        try:
            import utils.clone_manager as _cm
            main_bot = getattr(_cm, "MAIN_BOT_USERNAME", None)
            if main_bot:
                lines.append(f"\n🤖 <i>This poll was created via @{main_bot}</i>")
        except Exception:
            pass

    return "\n".join(lines)


def build_vote_keyboard(giveaway_id: str, options: List[str], is_active: bool) -> InlineKeyboardMarkup:
    if not is_active:
        return InlineKeyboardMarkup(inline_keyboard=[])
    buttons = [
        [InlineKeyboardButton(
            text=f"Vote: {option[:30]}",
            callback_data=f"vote:{giveaway_id}:{i}"
        )]
        for i, option in enumerate(options)
    ]
    buttons.append([
        InlineKeyboardButton(text="🔒 Close Poll", callback_data=f"close_poll:{giveaway_id}")
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_verify_join_keyboard(giveaway_id: str, channel_username: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Join Channel", url=f"https://t.me/{channel_username.lstrip('@')}")],
        [InlineKeyboardButton(text="✅ I've Joined — Verify", callback_data=f"verify_join:{giveaway_id}")]
    ])
