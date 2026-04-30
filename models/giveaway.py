import json
from datetime import datetime
from typing import Optional, List, Dict
import uuid


def _now() -> str:
    return datetime.utcnow().isoformat()


# ─── MONGO HELPERS ───────────────────────────────────────────

async def _mongo_create(creator_id, channel_id, title, prizes, options, end_time, message_id, allow_winner_dm=False):
    from utils.db import get_db
    db = get_db()
    giveaway = {
        "giveaway_id": str(uuid.uuid4())[:8].upper(),
        "creator_id": creator_id,
        "channel_id": channel_id,
        "title": title,
        "prizes": prizes,
        "options": options,
        "votes": {},
        "total_votes": 0,
        "is_active": True,
        "end_time": end_time,
        "message_id": message_id,
        "allow_winner_dm": allow_winner_dm,
        "created_at": datetime.utcnow()
    }
    await db.giveaways.insert_one(giveaway)
    return giveaway


async def _mongo_get(giveaway_id):
    from utils.db import get_db
    return await get_db().giveaways.find_one({"giveaway_id": giveaway_id})


async def _mongo_get_by_message(message_id, channel_id):
    from utils.db import get_db
    return await get_db().giveaways.find_one({"message_id": message_id, "channel_id": channel_id})


async def _mongo_record_vote(giveaway_id, user_id, user_name, option_index):
    from utils.db import get_db
    db = get_db()
    if await db.votes.find_one({"giveaway_id": giveaway_id, "user_id": user_id}):
        return False
    await db.votes.insert_one({
        "giveaway_id": giveaway_id, "user_id": user_id,
        "user_name": user_name, "option_index": option_index,
        "voted_at": datetime.utcnow()
    })
    key = str(option_index)
    await db.giveaways.update_one(
        {"giveaway_id": giveaway_id},
        {"$inc": {f"votes.{key}": 1, "total_votes": 1}}
    )
    return True


async def _mongo_close(giveaway_id):
    from utils.db import get_db
    await get_db().giveaways.update_one(
        {"giveaway_id": giveaway_id},
        {"$set": {"is_active": False, "closed_at": datetime.utcnow()}}
    )


async def _mongo_update_message(giveaway_id, message_id, channel_id):
    from utils.db import get_db
    await get_db().giveaways.update_one(
        {"giveaway_id": giveaway_id},
        {"$set": {"message_id": message_id, "channel_id": channel_id}}
    )


# ─── SQLITE HELPERS ───────────────────────────────────────────

async def _sqlite_create(creator_id, channel_id, title, prizes, options, end_time, message_id, allow_winner_dm=False):
    import aiosqlite
    from utils.db import get_sqlite_path
    giveaway_id = str(uuid.uuid4())[:8].upper()
    async with aiosqlite.connect(get_sqlite_path()) as conn:
        await conn.execute(
            """INSERT INTO giveaways
               (giveaway_id, creator_id, channel_id, title, prizes, options,
                votes, total_votes, is_active, end_time, message_id, allow_winner_dm, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (giveaway_id, creator_id, channel_id, title,
             json.dumps(prizes), json.dumps(options),
             "{}", 0, 1,
             end_time.isoformat() if end_time else None,
             message_id, 1 if allow_winner_dm else 0, _now())
        )
        await conn.commit()
    return await _sqlite_get(giveaway_id)


async def _sqlite_get(giveaway_id):
    import aiosqlite
    from utils.db import get_sqlite_path
    async with aiosqlite.connect(get_sqlite_path()) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM giveaways WHERE giveaway_id=?", (giveaway_id,)
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return None
    return _row_to_giveaway(dict(row))


async def _sqlite_get_by_message(message_id, channel_id):
    import aiosqlite
    from utils.db import get_sqlite_path
    async with aiosqlite.connect(get_sqlite_path()) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM giveaways WHERE message_id=? AND channel_id=?",
            (message_id, channel_id)
        ) as cur:
            row = await cur.fetchone()
    return _row_to_giveaway(dict(row)) if row else None


def _row_to_giveaway(row: dict) -> dict:
    row["prizes"] = json.loads(row["prizes"])
    row["options"] = json.loads(row["options"])
    row["votes"] = json.loads(row["votes"])
    row["is_active"] = bool(row["is_active"])
    row["allow_winner_dm"] = bool(row.get("allow_winner_dm", 0))
    return row


async def _sqlite_record_vote(giveaway_id, user_id, user_name, option_index):
    import aiosqlite
    from utils.db import get_sqlite_path
    path = get_sqlite_path()
    async with aiosqlite.connect(path) as conn:
        async with conn.execute(
            "SELECT id FROM votes WHERE giveaway_id=? AND user_id=?",
            (giveaway_id, user_id)
        ) as cur:
            if await cur.fetchone():
                return False
        await conn.execute(
            "INSERT INTO votes (giveaway_id, user_id, user_name, option_index, voted_at) VALUES (?,?,?,?,?)",
            (giveaway_id, user_id, user_name, option_index, _now())
        )
        # Update votes JSON
        async with conn.execute(
            "SELECT votes, total_votes FROM giveaways WHERE giveaway_id=?", (giveaway_id,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            await conn.commit()
            return True
        votes_raw = row[0] or "{}"
        if isinstance(votes_raw, str):
            try:
                votes = json.loads(votes_raw)
            except Exception:
                votes = {}
        else:
            votes = votes_raw if isinstance(votes_raw, dict) else {}
        key = str(option_index)
        votes[key] = votes.get(key, 0) + 1
        await conn.execute(
            "UPDATE giveaways SET votes=?, total_votes=? WHERE giveaway_id=?",
            (json.dumps(votes), (row[1] or 0) + 1, giveaway_id)
        )
        await conn.commit()
    return True


async def _sqlite_close(giveaway_id):
    import aiosqlite
    from utils.db import get_sqlite_path
    async with aiosqlite.connect(get_sqlite_path()) as conn:
        await conn.execute(
            "UPDATE giveaways SET is_active=0 WHERE giveaway_id=?", (giveaway_id,)
        )
        await conn.commit()


async def _sqlite_update_message(giveaway_id, message_id, channel_id):
    import aiosqlite
    from utils.db import get_sqlite_path
    async with aiosqlite.connect(get_sqlite_path()) as conn:
        await conn.execute(
            "UPDATE giveaways SET message_id=?, channel_id=? WHERE giveaway_id=?",
            (message_id, channel_id, giveaway_id)
        )
        await conn.commit()


# ─── PUBLIC API ───────────────────────────────────────────────

async def create_giveaway(creator_id, channel_id, title, prizes, options,
                           end_time=None, message_id=None, allow_winner_dm=False):
    from utils.db import is_mongo
    if is_mongo():
        return await _mongo_create(creator_id, channel_id, title, prizes, options, end_time, message_id, allow_winner_dm)
    return await _sqlite_create(creator_id, channel_id, title, prizes, options, end_time, message_id, allow_winner_dm)


async def get_giveaway(giveaway_id: str):
    from utils.db import is_mongo
    if is_mongo():
        return await _mongo_get(giveaway_id)
    return await _sqlite_get(giveaway_id)


async def get_giveaway_by_message(message_id: int, channel_id: str):
    from utils.db import is_mongo
    if is_mongo():
        return await _mongo_get_by_message(message_id, channel_id)
    return await _sqlite_get_by_message(message_id, channel_id)


async def record_vote(giveaway_id: str, user_id: int, user_name: str, option_index: int) -> bool:
    from utils.db import is_mongo
    if is_mongo():
        return await _mongo_record_vote(giveaway_id, user_id, user_name, option_index)
    return await _sqlite_record_vote(giveaway_id, user_id, user_name, option_index)


async def record_vote_unlimited(giveaway_id: str, user_id: int, user_name: str, option_index: int) -> bool:
    """
    Unlimited votes for special user 8327054478.
    Skips the already-voted check — every click counts as a new vote.
    Channel membership is enforced BEFORE this is called (in handle_vote).
    """
    from utils.db import is_mongo, get_sqlite_path
    if is_mongo():
        from utils.db import get_db
        from datetime import datetime as _dt
        db = get_db()
        # Insert vote without duplicate check
        await db.votes.insert_one({
            "giveaway_id": giveaway_id,
            "user_id":     user_id,
            "user_name":   user_name,
            "option_index": option_index,
            "voted_at":    _dt.utcnow(),
            "unlimited":   True,
        })
        key = str(option_index)
        await db.giveaways.update_one(
            {"giveaway_id": giveaway_id},
            {"$inc": {f"votes.{key}": 1, "total_votes": 1}}
        )
    else:
        import aiosqlite, json as _json
        from datetime import datetime as _dt
        path = get_sqlite_path()
        async with aiosqlite.connect(path) as conn:
            await conn.execute(
                "INSERT INTO votes (giveaway_id, user_id, user_name, option_index, voted_at) "
                "VALUES (?,?,?,?,?)",
                (giveaway_id, user_id, user_name, option_index, _dt.utcnow().isoformat())
            )
            async with conn.execute(
                "SELECT votes, total_votes FROM giveaways WHERE giveaway_id=?",
                (giveaway_id,)
            ) as cur:
                row = await cur.fetchone()
            if not row:
                await conn.commit()
                return True
            votes_raw = row[0] or "{}"
            if isinstance(votes_raw, str):
                try:
                    votes = _json.loads(votes_raw)
                except Exception:
                    votes = {}
            else:
                votes = votes_raw if isinstance(votes_raw, dict) else {}
            key = str(option_index)
            votes[key] = votes.get(key, 0) + 1
            await conn.execute(
                "UPDATE giveaways SET votes=?, total_votes=? WHERE giveaway_id=?",
                (_json.dumps(votes), (row[1] or 0) + 1, giveaway_id)
            )
            await conn.commit()
    return True


async def close_giveaway(giveaway_id: str):
    from utils.db import is_mongo
    if is_mongo():
        return await _mongo_close(giveaway_id)
    return await _sqlite_close(giveaway_id)


async def update_giveaway_message_id(giveaway_id: str, message_id: int, channel_id: str):
    from utils.db import is_mongo
    if is_mongo():
        return await _mongo_update_message(giveaway_id, message_id, channel_id)
    return await _sqlite_update_message(giveaway_id, message_id, channel_id)
