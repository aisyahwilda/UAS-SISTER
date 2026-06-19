"""
Configuration settings dari environment variables.
"""

import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql://user:pass@localhost:5432/db"
    )
    BROKER_URL: str = os.getenv("BROKER_URL", "redis://localhost:6379")
    REDIS_QUEUE_KEY: str = "event_queue"
    WORKER_COUNT: int = int(os.getenv("WORKER_COUNT", "4"))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    class Config:
        env_file = ".env"


settings = Settings()
