import os
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    BOT_TOKEN: str = "changeme"
    MONGO: bool = True
    MONGO_URI: str = "mongodb://localhost:27017/giveawaybot"
    SUPERADMIN_IDS: List[int] = []
    # Render uses PORT env var — fall back to WEB_PORT, then 8080
    WEB_PORT: int = int(os.environ.get("PORT", os.environ.get("WEB_PORT", 8080)))
    WEB_DOMAIN: str = "your-app.onrender.com"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
