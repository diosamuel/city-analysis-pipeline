# Apache Superset — Smart City dashboards

Superset runs alongside the pipeline so analysts can build dashboards on
top of the local DuckDB warehouse produced by dbt-duckdb.

## Start the stack

```bash
docker compose up -d superset-db superset-redis superset
```

Then open <http://localhost:8088>. Default credentials (override in `.env`):

```
username: admin
password: admin
```

## Connect Superset to the DuckDB warehouse

The repo's `medallion/` folder is mounted read-only inside the container
at `/warehouse/medallion/` so Superset can open the DuckDB file in-place.

1. In Superset → **Settings → Database Connections → + Database → DuckDB**.
2. SQLAlchemy URI:

   ```
   duckdb:////warehouse/medallion/warehouse.duckdb
   ```

   (four slashes — DuckDB absolute-path URI inside the container.)

3. Save. You can now explore views like `main.silver_vehicle_speed`, etc.,
   which dbt-duckdb registers pointing at `medallion/silver/*.parquet`.

## Files

- `superset_config.py` — metadata DB, cache, feature flags.
- `bootstrap.sh` — container entrypoint that installs `duckdb-engine`,
  runs `superset db upgrade`, creates the admin user, and boots gunicorn.

## Environment variables

| Variable                   | Purpose                                     |
|----------------------------|---------------------------------------------|
| `SUPERSET_SECRET_KEY`      | Flask secret key — **must** be overridden. |
| `SUPERSET_ADMIN_USERNAME`  | Initial admin username (default `admin`).  |
| `SUPERSET_ADMIN_PASSWORD`  | Initial admin password (default `admin`).  |
| `SUPERSET_ADMIN_EMAIL`     | Initial admin email.                        |
