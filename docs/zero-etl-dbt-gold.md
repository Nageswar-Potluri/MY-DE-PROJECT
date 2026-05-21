# Session Context: "Zero ETL dbt Gold Design"
## Racing Analytics — Architecture Decisions Log

> **Recall name:** `zero-etl-dbt-gold`
> Use this name at the start of any future session to resume context.

---

## What Was Decided

### 1. Overall Architecture
Zero ETL pipeline — platform-native processing, no hand-written ETL orchestration code.

```
Betfair/FormFav APIs
        │
Python Ingestion Scripts (existing)
        │
AWS S3 — Bronze (raw JSON, partitioned by snapshot_type)
        │
Databricks — Silver (Delta tables: harness + Betfair)
        │
dbt Gold Layer  ──→  AWS S3 Gold (Parquet / Iceberg)
        │
Power BI (DirectQuery via Athena ODBC)
```

---

### 2. dbt Gold Layer Design
Full enterprise architecture aligned to **dbt Analytics Engineering Certification v1.11**.

**Adapter strategy (dual-target):**
| Environment | Adapter | Engine |
|---|---|---|
| Dev (VS Code) | dbt-duckdb | Local DuckDB reads S3 directly |
| Prod | dbt-athena | AWS Athena serverless, writes Gold Parquet/Iceberg to S3 |

**Project layer structure:**
```
models/
  sources/       → declares Silver Delta tables via Glue Catalog
  staging/       → view, private, 1:1 with silver (harness/ + betfair/)
  intermediate/  → ephemeral, complex joins + business logic
  marts/
    core/        → table, public, contract enforced (dim_tracks, dim_horses, dim_dates, dim_markets)
    racing/      → incremental merge (fct_races, fct_runner_performance)
    betting/     → microbatch (fct_market_prices), incremental (fct_price_movements)
```

**All 7 certification exam domains covered:**
1. Materializations: view / ephemeral / table / incremental merge / microbatch
2. Governance: contracts + constraints + model versions
3. Debugging: compile workflow, behavior flags
4. Pipeline: dbt clone for CI, warn_if/error_if thresholds
5. Testing: generic + singular + custom generic + unit tests + `where` param
6. External deps: exposures (Power BI) + source freshness
7. State: state:modified+ selector for slim CI runs

---

### 3. Silver → Gold Bridge (Open Decision)
**Options discussed — not yet decided:**
| Option | Approach | Complexity |
|---|---|---|
| A | Enable Delta symlink manifest on Databricks → Athena reads via Glue | Low |
| B | Register Silver Delta tables in Glue Catalog as external tables | Medium |
| C | Rewrite Silver to output Iceberg instead of Delta | High (cleanest long-term) |

---

### 4. Open Questions (to resolve next session)
- [ ] Keep Databricks for Silver, or replace with AWS-native (Glue)?
- [ ] Gold format: Parquet, Iceberg, or Delta?
- [ ] Power BI connection method: Athena ODBC, or S3 direct connector?

---

### 5. What Exists in the Repo Today
- `Databricks/Silver_Races_Final.py` — Silver harness layer (dim_tracks, harness_meetings, harness_races, harness_runners, harness_runner_stats, dim_horse_form)
- `Databricks/Silver_Betfair_Final.py` — Silver Betfair layer (betfair_catalogue, betfair_market_book, betfair_runner_odds)
- `betfair_ingestion_pipeline_to_s3.py` — Live Betfair polling → S3 Bronze
- `formfav_ingestion_pipeline_to_s3.py` — FormFav ingestion → S3 Bronze
- `bronze_autoloader.py` — Databricks AutoLoader S3 → Delta Bronze
- `dbt-project/` — empty scaffold (ready to build)

---

### 6. dbt Plan File
Full architecture plan saved at:
`/root/.claude/plans/d-bt-ana-lyt-piped-brook.md`

---

### 7. Study Guide Reference
Official: dbt Analytics Engineering Certification Study Guide v1.11
URL: https://www.getdbt.com/dbt-assets/certifications/dbt-certificate-study-guide-version-1-11
