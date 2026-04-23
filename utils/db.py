from config.settings import settings

client = None
db = None
sqlite_path = "bot_data.db"


async def _init_mongo():
    global client, db
    from motor.motor_asyncio import AsyncIOMotorClient
    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client.get_default_database()
    await db.giveaways.create_index("giveaway_id", unique=True)
    await db.giveaways.create_index("channel_id")
    await db.votes.create_index([("giveaway_id", 1), ("user_id", 1)], unique=True)
    await db.clone_bots.create_index("token", unique=True)
    await db.clone_bots.create_index("owner_id")
    await db.referrals.create_index([("clone_token", 1), ("user_id", 1)], unique=True)
    await db.referrals.create_index("referred_by")
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
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id INTEGER PRIMARY KEY,
                banned INTEGER DEFAULT 1
            );
        """)
        await conn.commit()
    print("✅ SQLite initialised at", sqlite_path)


async def init_db():
    if settings.MONGO:
        await _init_mongo()
    else:
        await _init_sqlite()


def get_db():
    return db


def is_mongo() -> bool:
    return settings.MONGO


def get_sqlite_path() -> str:
    return sqlite_path
