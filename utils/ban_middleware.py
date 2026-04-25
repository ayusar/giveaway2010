# utils/ban_middleware.py
"""
utils/ban_middleware.py

Aiogram middleware that blocks banned users from interacting with the bot.
Checks main_bot_users.is_banned / banned_users table on every update.
"""
from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery

logger = logging.getLogger(__name__)


async def _is_banned(user_id: int) -> bool:
    """Return True if the user is banned."""
    try:
        from utils.db import get_db, is_mongo, get_sqlite_path
        if is_mongo():
            db = get_db()
            if db is None:
                return False
            doc = await db.main_bot_users.find_one(
                {"user_id": user_id}, {"is_banned": 1}
            )
            if doc and doc.get("is_banned"):
                return True
            ban_doc = await db.banned_users.find_one({"user_id": user_id})
            return bool(ban_doc)
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                async with conn.execute(
                    "SELECT is_banned FROM main_bot_users WHERE user_id=?", (user_id,)
                ) as cur:
                    row = await cur.fetchone()
                if row and row[0]:
                    return True
                async with conn.execute(
                    "SELECT banned FROM banned_users WHERE user_id=?", (user_id,)
                ) as cur:
                    row2 = await cur.fetchone()
                return bool(row2 and row2[0])
    except Exception as e:
        logger.debug(f"ban_middleware check failed for {user_id}: {e}")
        return False


class BanMiddleware(BaseMiddleware):
    """Drops updates from banned users silently."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        # Extract user_id from the event
        user_id: int | None = None
        if isinstance(event, Message) and event.from_user:
            user_id = event.from_user.id
        elif isinstance(event, CallbackQuery) and event.from_user:
            user_id = event.from_user.id
        else:
            # Try generic access
            from_user = getattr(event, "from_user", None)
            if from_user:
                user_id = from_user.id

        if user_id and await _is_banned(user_id):
            # Silently ignore — optionally send one message
            if isinstance(event, Message):
                try:
                    await event.answer(
                        "🚫 You have been banned from using this bot.",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
            elif isinstance(event, CallbackQuery):
                try:
                    await event.answer("🚫 You are banned.", show_alert=True)
                except Exception:
                    pass
            return  # Do NOT call handler

        return await handler(event, data)
