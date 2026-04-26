from config.settings import settings

client = None
db = None
sqlite_path = "bot_data.db"


async def _init_mongo():
    global client, db
    from motor.motor_asyncio import AsyncIOMotorClient
    client = AsyncIOMotorClient(
        settings.MONGO_URI,
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
        socketTimeoutMS=20000,
    )
    db = client.get_default_database()
    await db.giveaways.create_index("giveaway_id", unique=True)
    await db.giveaways.create_index("channel_id")
    await db.votes.create_index([("giveaway_id", 1), ("user_id", 1)], unique=True)
    await db.clone_bots.create_index("token", unique=True)
    await db.clone_bots.create_index("owner_id")
    await db.referrals.create_index([("clone_token", 1), ("user_id", 1)], unique=True)
    await db.referrals.create_index("referred_by")
    await db.main_bot_users.create_index("user_id", unique=True)
    await db.premium_users.create_index("user_id", unique=True)
    print("✅ MongoDB connected")


async def _init_sqlite():
    import aiosqlite
    async with aiosqlite.connect(sqlite_path) as conn:
        await conn.executescript("""
            CREATE TABLE IF NOT EXISTS giveaways (
                giveaway_id TEXT PRIMARY KEY,
                creator_id INTEGER,
                channel_id TEXT,
                title TEXT,
                prizes TEXT,
                options TEXT,
                votes TEXT DEFAULT '{}',
                total_votes INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                end_time TEXT,
                message_id INTEGER,
                allow_winner_dm INTEGER DEFAULT 0,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS votes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                giveaway_id TEXT,
                user_id INTEGER,
                user_name TEXT,
                option_index INTEGER,
                voted_at TEXT,
                UNIQUE(giveaway_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS clone_bots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER,
                token TEXT UNIQUE,
                bot_username TEXT,
                welcome_message TEXT,
                channel_link TEXT DEFAULT '',
                referral_caption TEXT DEFAULT '',
                enabled_commands TEXT DEFAULT '["refer","mystats","leaderboard","myreferrals"]',
                is_active INTEGER DEFAULT 1,
                is_banned INTEGER DEFAULT 0,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                clone_token TEXT,
                user_id INTEGER,
                user_name TEXT,
                referred_by INTEGER,
                refer_count INTEGER DEFAULT 0,
                lang TEXT DEFAULT 'en',
                joined_at TEXT,
                UNIQUE(clone_token, user_id)
            );
            CREATE TABLE IF NOT EXISTS main_bot_users (
                user_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                is_banned INTEGER DEFAULT 0,
                joined_at TEXT
            );
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id INTEGER PRIMARY KEY,
                banned INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS panel_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE,
                password TEXT
            );
            CREATE TABLE IF NOT EXISTS premium_users (
                user_id INTEGER PRIMARY KEY,
                granted_by INTEGER,
                expires_at TEXT,
                granted_at TEXT
            );
        """)
        await conn.commit()
        # Migration: add allow_winner_dm column if missing (for existing DBs)
        try:
            await conn.execute("ALTER TABLE giveaways ADD COLUMN allow_winner_dm INTEGER DEFAULT 0")
            await conn.commit()
        except Exception:
            pass  # Column already exists
    print("✅ SQLite initialised at", sqlite_path)


async def init_db():
    import logging
    log = logging.getLogger(__name__)
    if settings.MONGO:
        # Retry up to 5 times — Render cold starts can be slow
        for attempt in range(1, 6):
            try:
                await _init_mongo()
                if db is not None:
                    return
                raise RuntimeError("Motor client is None after init")
            except Exception as e:
                log.warning(f"MongoDB init attempt {attempt}/5 failed: {e}")
                if attempt < 5:
                    await __import__("asyncio").sleep(3)
        raise RuntimeError("MongoDB failed to initialise after 5 attempts — check MONGO_URI")
    else:
        await _init_sqlite()


def get_db():
    return db


def is_mongo() -> bool:
    return settings.MONGO


def get_sqlite_path() -> str:
    return sqlite_path
