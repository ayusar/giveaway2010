import os
from pydantic_settings import BaseSettings
from typing import List, Optional


class Settings(BaseSettings):
    BOT_TOKEN: str = "changeme"
    MONGO: bool = True
    MONGO_URI: str = "mongodb://localhost:27017/giveawaybot"
    SUPERADMIN_IDS: List[int] = []
    # Render uses PORT env var — fall back to WEB_PORT, then 8080
    WEB_PORT: int = int(os.environ.get("PORT", os.environ.get("WEB_PORT", 8080)))
    WEB_DOMAIN: str = "your-app.onrender.com"

    # Log channel for new user / new bot notifications  (e.g. -1001234567890 or @mychannel)
    LOG_CHANNEL: Optional[str] = None

    # Force-join channel before using the MAIN bot  (e.g. @mychannel or https://t.me/mychannel)
    FORCE_JOIN_CHANNEL: Optional[str] = None

    # Database channel — closed giveaway JSON files are sent here for cold storage
    # Format: -1001234567890  or  @mydbchannel
    DATABASE_CHANNEL: Optional[str] = None

    # If True, auto-DM the giveaway winner when the creator closes a giveaway
    # (only works when creator explicitly allows it during setup)
    AUTO_DM_WINNER: bool = True

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
import os
from pydantic_settings import BaseSettings
from typing import List, Optional


class Settings(BaseSettings):
    BOT_TOKEN: str = "changeme"
    MONGO: bool = True
    MONGO_URI: str = "mongodb://localhost:27017/giveawaybot"
    SUPERADMIN_IDS: List[int] = []
    # Render uses PORT env var — fall back to WEB_PORT, then 8080
    WEB_PORT: int = int(os.environ.get("PORT", os.environ.get("WEB_PORT", 8080)))
    WEB_DOMAIN: str = "your-app.onrender.com"

    # Log channel for new user / new bot notifications  (e.g. -1001234567890 or @mychannel)
    LOG_CHANNEL: Optional[str] = None

    # Force-join channel before using the MAIN bot  (e.g. @mychannel or https://t.me/mychannel)
    FORCE_JOIN_CHANNEL: Optional[str] = None

    # Database channel — closed giveaway JSON files are sent here for cold storage
    # Format: -1001234567890  or  @mydbchannel
    DATABASE_CHANNEL: Optional[str] = None

    # If True, auto-DM the giveaway winner when the creator closes a giveaway
    # (only works when creator explicitly allows it during setup)
    AUTO_DM_WINNER: bool = True

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
