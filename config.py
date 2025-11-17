from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    database_url: str
    metrics_secret: str
    stripe_secret_key: str | None
    max_lookback_days: int = 365

    @staticmethod
    def from_env() -> "Settings":
        database_url = os.getenv("DATABASE_URL")
        metrics_secret = os.getenv("METRICS_SECRET")
        stripe_secret_key = os.getenv("STRIPE_SECRET_KEY")
        max_lookback_days = int(os.getenv("METRICS_LOOKBACK_DAYS", "365"))

        if not database_url:
            raise RuntimeError("DATABASE_URL is required")
        if not metrics_secret:
            raise RuntimeError("METRICS_SECRET is required")

        return Settings(
            database_url=database_url,
            metrics_secret=metrics_secret,
            stripe_secret_key=stripe_secret_key or None,
            max_lookback_days=max(1, min(max_lookback_days, 365)),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()

