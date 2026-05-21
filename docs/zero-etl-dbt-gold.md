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

---

## Update — Market Showcase Discussion

### 8. Study Guide Intent Clarified
The dbt v1.11 study guide was shared as a **feature reference syllabus** — not for exam
preparation. The goal is to showcase enterprise-level dbt skills to the market
(employers, clients). Every feature included must serve that purpose.

---

### 9. dbt Mesh — Reinstated (Full Multi-Project Topology)
Initially excluded because it's on the Architect cert, not the Analytics Engineering cert.
**Reinstated** because it is the biggest market differentiator in dbt right now.
Showing a proper Mesh topology signals senior/architect-level thinking.

**Revised project topology:**
```
dbt-project/
├── racing_core/              ← Producer project (harness racing domain)
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/          ← private
│   │   ├── intermediate/     ← ephemeral, private
│   │   └── marts/
│   │       ├── core/         ← public, contracted (dim_tracks, dim_horses, dim_dates)
│   │       └── racing/       ← public, contracted (fct_races, fct_runner_performance)
│   └── semantic_models/      ← MetricFlow metrics on racing facts
│
└── betting_analytics/        ← Consumer project (Betfair domain)
    ├── dbt_project.yml
    ├── dependencies.yml      ← declares racing_core as upstream project
    ├── models/
    │   ├── staging/          ← private
    │   └── marts/
    │       └── betting/      ← public, contracted (dim_markets, fct_market_prices, fct_price_movements)
    └── semantic_models/      ← MetricFlow metrics on Betfair facts
```

Cross-project reference example:
```sql
-- inside betting_analytics project
select * from {{ ref('racing_core', 'fct_races') }}
```

---

### 10. Full Feature Showcase List (Final)

| Feature | Market Signal | Status |
|---|---|---|
| dbt Mesh (multi-project cross-refs) | Senior / Architect level | Include |
| Semantic Layer / MetricFlow | Enterprise analytics engineering | Include |
| Microbatch incremental (dbt 1.9+) | Cutting edge | Include |
| Unit tests (dbt 1.8+) | Modern testing practices | Include |
| Model contracts | Data governance | Include |
| Model versions + deprecation | Breaking change management | Include |
| Custom generic tests | Reusable test patterns | Include |
| Snapshots in YAML (dbt 1.9+) | Modern SCD Type 2 | Include |
| dbt clone | CI/CD maturity | Include |
| State-based selection | Advanced CI | Include |
| Exposures | Lineage to Power BI | Include |
| Source freshness | Data reliability | Include |
| Grants | Security / access control | Include |
| dbt-utils + dbt-expectations | Package ecosystem knowledge | Include |

---

### 11. What This Showcases to the Market
1. **Data product thinking** — producer/consumer separation via Mesh
2. **Multi-team architecture** — not just solo analytics
3. **Latest dbt features** — Mesh, microbatch, semantic layer
4. **Engineering rigour** — contracts, versions, unit tests, CI/CD
5. **Governance mindset** — access controls, grants, model access levels

---

## Update — Gold Storage Decisions

### 12. Gold Format — CONFIRMED: Apache Iceberg
Format: **Apache Iceberg** on AWS S3

Rationale:
- Native Athena v2 support (no manifest tricks)
- ACID transactions + time travel + schema evolution
- Readable by both Databricks (Variant 1) and Glue/native (Variant 2)
- Only format that works cleanly across both architecture variants
- dbt-athena supports Iceberg materialization natively

**S3 structure:**
```
s3://racing-analytics-prod/gold/
├── racing_core/
│   ├── dim_tracks/
│   ├── dim_horses/
│   ├── dim_dates/
│   ├── fct_races/
│   └── fct_runner_performance/
└── betting_analytics/
    ├── dim_markets/
    ├── fct_market_prices/
    └── fct_price_movements/
```

**Glue Data Catalog:**
- Database: `racing_gold`
- Schema per project: `racing_core` / `betting_analytics`

**dbt config:**
```yaml
# dbt_project.yml
models:
  betting_analytics:
    marts:
      +file_format:    iceberg
      +table_type:     iceberg
      +partitioned_by: ['event_date']   # fct_market_prices + fct_price_movements
```

**profiles.yml (prod target):**
```yaml
prod:
  type:           athena
  s3_staging_dir: s3://racing-analytics-prod/athena-staging/
  s3_data_dir:    s3://racing-analytics-prod/gold/
  database:       racing_gold
  schema:         betting_analytics
  region_name:    ap-southeast-2
```

### 13. Open Questions (remaining)
- [ ] S3 bucket name — existing bucket or new one?
- [ ] AWS region confirmed as ap-southeast-2 (Sydney)?
- [ ] Power BI connection method — Athena ODBC or S3 direct connector?
