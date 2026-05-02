#!/usr/bin/env bash
# Superset container entrypoint: install DuckDB driver, migrate the
# metadata DB, create the admin user, initialize roles, then start
# gunicorn on :8088.
set -euo pipefail

ADMIN_USERNAME="${ADMIN_USERNAME:-admin}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin}"
ADMIN_EMAIL="${ADMIN_EMAIL:-admin@example.com}"

echo "[superset] installing DuckDB sqlalchemy driver..."
pip install --no-cache-dir \
    "duckdb-engine>=0.13.0" \
    "duckdb>=1.0.0" \
    || true

echo "[superset] running db upgrade..."
superset db upgrade

echo "[superset] creating admin user (${ADMIN_USERNAME}) if missing..."
superset fab create-admin \
    --username "${ADMIN_USERNAME}" \
    --firstname Super \
    --lastname Admin \
    --email "${ADMIN_EMAIL}" \
    --password "${ADMIN_PASSWORD}" || true

echo "[superset] initializing roles & permissions..."
superset init

echo "[superset] starting server on :8088..."
exec /usr/bin/run-server.sh
