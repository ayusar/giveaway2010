import os
import json
from pydantic_settings import BaseSettings
from typing import List, Optional


class Settings(BaseSettings):
    BOT_TOKEN: str = "changeme"
    MONGO: bool = True
    MONGO_URI: str = "mongodb://localhost:27017/giveawaybot"
    SUPERADMIN_IDS: List[int] = []
    WEB_PORT: int = int(os.environ.get("PORT", os.environ.get("WEB_PORT", 8080)))
    WEB_DOMAIN: str = "your-app.onrender.com"
    LOG_CHANNEL: Optional[str] = None
    FORCE_JOIN_CHANNEL: Optional[str] = None
    DATABASE_CHANNEL: Optional[int] = None
    AUTO_DM_WINNER: bool = True

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


def _load_settings() -> Settings:
    raw = os.environ.get("SUPERADMIN_IDS", "").strip()
    if raw and not raw.startswith("["):
        # Plain number or comma-separated: "8420494874" or "123,456"
        ids = [int(x.strip()) for x in raw.split(",") if x.strip()]
        os.environ["SUPERADMIN_IDS"] = json.dumps(ids)
    return Settings()


settings = _load_settings()
