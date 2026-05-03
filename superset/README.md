# Apache Superset — Smart City dashboards

Superset runs alongside the pipeline so analysts can build dashboards on
top of the local DuckDB warehouse produced by dbt-duckdb.

## Start the stack

From the **repository root**, Kestra and Superset run together:

```bash
docker compose up -d
```

- Kestra UI: <http://localhost:8080> — see [Kestra Docker docs](https://kestra.io/docs/installation/docker).
- Superset: <http://localhost:8088> — see [Superset Docker Compose](https://superset.apache.org/admin-docs/installation/docker-compose/).

First run builds the Superset image (`docker/superset/Dockerfile`). Override secrets in `.env` (copy from `.env.example`).

Then open Superset at <http://localhost:8088>. Default credentials (override in `.env`):

```
username: admin
password: admin
```

## Connect Superset to the DuckDB warehouse

The repo's `medallion/` folder is mounted read-only inside the container
at `/warehouse/medallion/` so Superset can open the DuckDB file in-place.

1. In Superset → **Settings → Database Connections → + Database → DuckDB**.
2. SQLAlchemy URI (example for dbt output `gold.duckdb`):

   ```
   duckdb:////warehouse/medallion/gold.duckdb
   ```

   (four slashes — DuckDB absolute-path URI inside the container.)

3. Save. You can explore relations in the `main` schema (or your dbt target schema) created by `dbt run`.

## Connect Superset to Google BigQuery (service account)

The Superset image installs **`sqlalchemy-bigquery`** and mounts `./.gcp` at **`/etc/gcp`** in the container. Set **`GOOGLE_APPLICATION_CREDENTIALS`** to your key path (default: `/etc/gcp/bigquery-sa.json` via `GCP_SA_FILENAME` in `.env`).

1. Put the JSON key on the host, e.g. **`.gcp/bigquery-sa.json`** (never commit it; only `.gcp/.gitkeep` is tracked).
2. Rebuild/restart so the variable is applied:
   ```bash
   docker compose up -d --build superset
   ```
3. In GCP, grant the service account at least **BigQuery Data Viewer** on datasets you query and **BigQuery Job User** on the project (so it can run queries).
4. In Superset → **Settings → Database connections → + Database**:
   - **Display name:** e.g. `BigQuery (prod)`
   - **SQLAlchemy URI:** `bigquery://YOUR_PROJECT_ID/YOUR_DEFAULT_DATASET`  
     Example (matches a typical dbt prod target): `bigquery://learngcp-461809/dbt_dio`
   - Leave **user/password** empty when using the JSON key via ADC.

If the connection test fails, open **Advanced → Other → ENGINE PARAMETERS** and add (path is **inside the container**):

```json
{
  "connect_args": {
    "credentials_path": "/etc/gcp/bigquery-sa.json"
  }
}
```

Use the same filename as `GCP_SA_FILENAME` if you changed it.

## Files

- `superset_config.py` — metadata DB, cache, feature flags.
- `bootstrap.sh` — runs `superset db upgrade`, creates the admin user, `superset init`, then `run-server.sh` (drivers are baked in the image).

## Environment variables

| Variable                   | Purpose                                     |
|----------------------------|---------------------------------------------|
| `SUPERSET_SECRET_KEY`      | Flask secret key — **must** be overridden. |
| `SUPERSET_ADMIN_USERNAME`  | Initial admin username (default `admin`).  |
| `SUPERSET_ADMIN_PASSWORD`  | Initial admin password (default `admin`).  |
| `SUPERSET_ADMIN_EMAIL`     | Initial admin email.                        |
| `GCP_SA_FILENAME`          | Service account JSON filename under `.gcp/` (default `bigquery-sa.json`). |
| `GCP_PROJECT_ID`           | Documentation hint for BigQuery URI. |
| `GCP_BIGQUERY_DATASET`     | Documentation hint for default dataset in URI. |
