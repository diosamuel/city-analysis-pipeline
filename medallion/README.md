### Medallion dbt project (smart city)

Run from this directory (`medallion/`) so paths in `profiles.yml` / `.env` resolve:

```bash
cd medallion
dbt run
```

### Layout

| Path | Role |
|------|------|
| `models/staging/` | Source definitions (`_sources.yml`); optional `stg_*` models later |
| `models/silver/` | Cleansed / typed models over `{{ source('bronze', ...) }}` |
| `models/gold/` | Analytics-ready tables (aggregates, KPIs for BI) |

Standard dbt dirs: `analyses/`, `macros/`, `seeds/`, `snapshots/`, `tests/`.

### References

- [dbt docs](https://docs.getdbt.com/docs/introduction)
