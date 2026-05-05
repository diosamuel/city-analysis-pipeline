# Smart city data engineering

This repository collects and models **Indonesian smart-city style data**: **CCTV traffic** (speed and volumes from KemenPUPR-style feeds), **BMKG weather**, and **ISPU air quality** (KemenLH). It combines **Python ingestion**, a **DuckDB** bronze store, **dbt** (medallion layering), optional **BigQuery** as a production warehouse, and Docker services for **Kestra** (orchestration) and **Apache Superset** (BI).

---

## Repository layout

| Path | Role |
|------|------|
| [`ingest/crawl`](ingest/crawl/) | Scripts to fetch CCTV, weather, AQI data and load bronze tables |
| [`source_data/raw_data.duckdb`](source_data/) | Attached read-only DuckDB catalog **`cctv`** for dbt sources (bronze); create/populate via ingest |
| [`medallion`](medallion/) | **dbt** project (silver staging + gold dimensions/facts) |
| [`schema`](schema/) | Reference DDL: operational tables (`schema.sql`), semantic DWH sketch (`dwh.sql`, `schema.dbdiagram`) |
| [`infra`](infra/) | Terraform scaffolding (GCP / infra as code) |
| [`docker-compose.yml`](docker-compose.yml) | Kestra + Superset (+ Postgres + Redis) |
| [`superset`](superset/) | Superset bootstrap and config hints |

Nested READMEs: [`ingest/crawl/cctv`](ingest/crawl/cctv/README.md), [`ingest/crawl/aqi`](ingest/crawl/aqi/README.md), [`ingest/crawl/weather`](ingest/crawl/weather/README.md), [`superset/README.md`](superset/README.md).

---

## Prerequisites

- **Python** ≥ 3.13 (see [`pyproject.toml`](pyproject.toml))
- **uv** or **pip** for dependencies (`dbt-core`, `dbt-duckdb`, `dbt-bigquery`, `duckdb`, etc.)
- **Docker Desktop** (optional, for Compose stack; WSL2 on Windows recommended)

---

## Environment variables

Copy [`.env.example`](.env.example) to `.env` and adjust paths and secrets.

Important for local dbt (run from **`medallion/`**):

- **`DBT_DUCKDB_PATH`** — DuckDB file where dbt builds dev models (defaults described in `.env.example`)
- **`DBT_BRONZE_DUCKDB_PATH`** — Must point at the bronze file dbt attaches as catalog **`cctv`**. Repo convention: `source_data/raw_data.duckdb` (paths are relative to the **current working directory** when you invoke `dbt`)

If dbt errors with **“catalog cctv does not exist”**, the attach path is wrong for your shell’s working directory—fix `DBT_BRONZE_DUCKDB_PATH` or always run commands from [`medallion`](medallion/) as documented in [`profiles.yml`](medallion/profiles.yml).

---

## Ingest bronze data

Bronze tables expected by dbt sources (catalog `cctv`, schema `main`) include `cctv_list_final`, `vehicle_speed`, `hourly_vehicle_speed`, `all_timeseries_vehicle_speed`, `bmkg_weather`, and `air_quality`. See [`medallion/models/sources/_sources.yml`](medallion/models/sources/_sources.yml).

Use the crawl/insert pipelines under [`ingest/crawl`](ingest/crawl/) (per-domain README files). Populate `source_data/raw_data.duckdb` before running transformations.

[`medallion/init.sql`](medallion/init.sql) shows a manual **`ATTACH`** for ad hoc DuckDB clients; dbt attaches the same bronze file via **`attach`** in the dev profile.

---

## dbt (medallion)

```bash
cd medallion

# Ensure DBT_* env vars are set (see .env.example)
export DBT_PROFILES_DIR=.   # optional: use repo-local profiles.yml

dbt deps        # if you add packages later
dbt debug
dbt run
dbt test        # when you add tests
```

[`medallion/dbt_project.yml`](medallion/dbt_project.yml) defines model defaults. Layers:

- **Silver (`models/silver/`)** — `stg_*` models sourced from **`source('sources', …)`**
- **Gold (`models/gold/`)** — dimensions and facts; gold models **`ref`** each other where appropriate (`dim_camera`, `dim_date`, `fact_cctv`, etc.)

[`medallion/profiles.yml`](medallion/profiles.yml):

- **`dev`** — DuckDB (+ read-only bronze attach). Default target is **`dev`**
- **`prod`** — BigQuery (requires a valid **service-account key** and dataset; paths in the committed example are placeholders you must replace for your GCP project)

To use BigQuery: set `target: prod` only after aligning `keyfile`, `project`, and `dataset`; raw ingests are still DuckDB-centric in this repo, so production warehousing may need a separate load/sync story.

---

## Logical warehouse schema

- [`schema/dwh.sql`](schema/dwh.sql) — Intended **star-schema** DDL (SQLite-flavored sketch): `dim_*`, `fact_cctv_daily_snapshot`, indexes, and FK notes
- [`schema/schema.dbdiagram`](schema/schema.dbdiagram) — dbdiagram sketch for the same design
- [`schema/schema.sql`](schema/schema.sql) — Broader bronze/operational table definitions aligned with ingestion

Gold dbt SQL is modeled to mirror `dwh.sql` semantics (grain and column names evolve with the codebase).

---

## Docker Compose (Kestra + Superset)

From the repo root:

```bash
docker compose up -d
```

- **Kestra** — http://localhost:8080  
- **Superset** — http://localhost:8088 (default admin credentials from `.env.example`; change for production)

The Superset image mounts `./medallion` and `./source_data` read-only under `/warehouse/…`; see [`docker-compose.yml`](docker-compose.yml) and [`docker/superset/Dockerfile`](docker/superset/Dockerfile).

---

## Contributing and operations

1. Prefer **thin, focused commits** aligned with one pipeline or model change  
2. **Do not commit** GCP keys, `.env`, or personal `profiles.yml` secrets—use `.env.example` and local overrides  
3. After changing ingestion or DDL, rerun **`dbt run`** from `medallion/` and reconcile with `schema/dwh.sql`

---

## License / attribution

Data sources include **BMKG**, **KemenLH ISPU**, OpenStreetMap / **Nominatim** (reverse geocoding), and CCTV feed endpoints configured in `.env.example`. Respect each provider’s **terms of use** and rate limits when running crawlers.

If you rely on **`apace-ai.com`** chart APIs for CCTV payloads, verify licensing and suitability for your use case before production ingestion.
