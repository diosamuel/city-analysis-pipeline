"""Superset configuration overrides for the Smart City stack.

Picks up SQLAlchemy metadata DB + Redis cache via env vars, so the same
image can be reused in dev / staging / prod without editing this file.
"""

from __future__ import annotations

import os


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


# ---- metadata DB (Postgres) -------------------------------------------------
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{_env('DATABASE_USER')}:{_env('DATABASE_PASSWORD')}"
    f"@{_env('DATABASE_HOST')}:{_env('DATABASE_PORT', '5432')}"
    f"/{_env('DATABASE_DB')}"
)

# ---- secret key --------------------------------------------------------------
SECRET_KEY = _env("SUPERSET_SECRET_KEY", "please-change-me-in-production")

# ---- caching (Redis) --------------------------------------------------------
_REDIS_HOST = os.environ.get("REDIS_HOST", "superset-redis")
_REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": _REDIS_HOST,
    "CACHE_REDIS_PORT": _REDIS_PORT,
    "CACHE_REDIS_DB": 1,
}

DATA_CACHE_CONFIG = {**CACHE_CONFIG, "CACHE_KEY_PREFIX": "superset_data_"}
FILTER_STATE_CACHE_CONFIG = {**CACHE_CONFIG, "CACHE_KEY_PREFIX": "superset_filter_"}
EXPLORE_FORM_DATA_CACHE_CONFIG = {**CACHE_CONFIG, "CACHE_KEY_PREFIX": "superset_explore_"}

# ---- feature flags ----------------------------------------------------------
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# ---- web server -------------------------------------------------------------
SUPERSET_WEBSERVER_TIMEOUT = 300
ROW_LIMIT = 50_000
