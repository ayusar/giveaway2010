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


def build_vote_keyboard(
    giveaway_id: str,
    options: List[str],
    is_active: bool,
    bot_username: Optional[str] = None,
) -> InlineKeyboardMarkup:
    """
    Build the poll's vote keyboard.

    When `bot_username` is provided, each vote button is a URL deep-link
    (t.me/<bot_username>?start=vote_<giveaway_id>_<index>) that opens a DM
    with the bot. The bot then checks channel membership and records the
    vote in DM — nothing is ever posted back into the channel, so voters
    who haven't joined don't cause any spam there.

    If `bot_username` is not supplied (legacy/back-compat callers only),
    falls back to the old in-channel callback_data buttons.
    """
    if not is_active:
        return InlineKeyboardMarkup(inline_keyboard=[])

    if bot_username:
        buttons = [
            [InlineKeyboardButton(
                text=f"Vote: {option[:30]}",
                url=f"https://t.me/{bot_username}?start=vote_{giveaway_id}_{i}"
            )]
            for i, option in enumerate(options)
        ]
    else:
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


def build_dm_join_vote_keyboard(giveaway_id: str, channel_username: str, option_index: int) -> InlineKeyboardMarkup:
    """
    Sent in the voter's DM (never in the channel) when they tap a vote
    button before joining the required channel. After joining, they tap
    "I've Joined — Vote Now" here to complete the same vote.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Join Channel", url=f"https://t.me/{channel_username.lstrip('@')}")],
        [InlineKeyboardButton(
            text="✅ I've Joined — Vote Now",
            callback_data=f"verify_vote:{giveaway_id}:{option_index}"
        )]
    ])
