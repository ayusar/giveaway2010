"""
utils/premium_reminder.py

Background task that DMs premium users 3 days before their premium expires.
Runs a check every 6 hours to avoid spamming.
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

_bot = None
_already_notified: set = set()  # (user_id, expiry_date_str) pairs — resets on restart


def set_bot(bot):
    global _bot
    _bot = bot


async def premium_expiry_reminder_loop():
    """Runs forever, checking every 6 hours for expiring premium users."""
    while True:
        try:
            await _check_expiring_premiums()
        except Exception as e:
            logger.error(f"Premium reminder loop error: {e}")
        await asyncio.sleep(6 * 3600)  # 6 hours


async def _check_expiring_premiums():
    if not _bot:
        return

    from utils.db import get_db, is_mongo, get_sqlite_path

    now = datetime.now(timezone.utc)
    warn_before = now + timedelta(days=3)

    users_to_notify = []

    try:
        if is_mongo():
            db = get_db()
            async for doc in db.premium_users.find({}):
                user_id = doc.get("user_id")
                exp_str = doc.get("expires_at", "")
                try:
                    exp_dt = datetime.fromisoformat(str(exp_str).replace("Z", ""))
                    if exp_dt.tzinfo is None:
                        exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                    days_left = (exp_dt - now).days
                    if 0 <= days_left <= 3:
                        key = (user_id, exp_dt.strftime("%Y-%m-%d"))
                        if key not in _already_notified:
                            users_to_notify.append((user_id, days_left, exp_dt))
                            _already_notified.add(key)
                except Exception:
                    pass
        else:
            import aiosqlite
            async with aiosqlite.connect(get_sqlite_path()) as conn:
                conn.row_factory = aiosqlite.Row
                try:
                    async with conn.execute("SELECT user_id, expires_at FROM premium_users") as cur:
                        rows = await cur.fetchall()
                except Exception:
                    rows = []
                for r in rows:
                    user_id = r["user_id"]
                    exp_str = r["expires_at"] or ""
                    try:
                        exp_dt = datetime.fromisoformat(str(exp_str))
                        if exp_dt.tzinfo is None:
                            exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                        days_left = (exp_dt - now).days
                        if 0 <= days_left <= 3:
                            key = (user_id, exp_dt.strftime("%Y-%m-%d"))
                            if key not in _already_notified:
                                users_to_notify.append((user_id, days_left, exp_dt))
                                _already_notified.add(key)
                    except Exception:
                        pass
    except Exception as e:
        logger.error(f"_check_expiring_premiums DB error: {e}")
        return

    for user_id, days_left, exp_dt in users_to_notify:
        try:
            if days_left == 0:
                day_text = "today"
                emoji = "🔴"
            elif days_left == 1:
                day_text = "tomorrow"
                emoji = "🟠"
            else:
                day_text = f"in {days_left} days"
                emoji = "🟡"

            await _bot.send_message(
                chat_id=user_id,
                text=(
                    f"⚠️ <b>Premium Expiry Reminder</b>\n\n"
                    f"{emoji} Your premium expires <b>{day_text}</b> "
                    f"({exp_dt.strftime('%Y-%m-%d')}).\n\n"
                    f"Renew now to keep your benefits:\n"
                    f"✦ No watermark on giveaways\n"
                    f"✦ Unlimited giveaways\n"
                    f"✦ Priority support\n\n"
                    f"Contact admin to renew your subscription! 💎"
                ),
                parse_mode="HTML",
            )
            logger.info(f"✅ Expiry reminder sent to user {user_id} (expires {exp_dt.date()})")
            await asyncio.sleep(0.05)  # rate limit
        except Exception as e:
            logger.debug(f"Could not send reminder to {user_id}: {e}")
