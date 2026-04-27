# Databricks Setup Documentation

**Project:** Racing Data Engineering Pipeline
**Date:** 2026-04-02
**Author:** Nageswara Chowdary Potluri

---

## Overview

This document covers all steps taken to set up Databricks and connect dbt to the Databricks SQL Warehouse as part of the Bronze → Silver → Gold pipeline.

---

## Platform Decision

### Options Evaluated

| Platform | URL | Clusters | S3 Access | Best For |
|---|---|---|---|---|
| Databricks Free Edition | app.databricks.com | Serverless only | Restricted | SQL Warehouse + dbt |
| Databricks Community Edition | community.cloud.databricks.com | Manual (single node) | Not available anymore | - |
| Databricks 14-day Trial | databricks.com | Full (multi-node) | Full access | S3 mount, Auto Loader |

### Decision
- **Free Edition** → Used for SQL Warehouse (Step 4) + dbt setup (Step 5 prep)
- **14-day Trial on AWS (ap-southeast-2)** → Required for Steps 1-3 (S3 mount, Auto Loader, Bronze Delta tables)

**Reason:** Free Edition restricts outbound internet access, blocking direct S3 connectivity. The Trial runs on AWS so S3 access works natively.

---

## Step 4 — SQL Warehouse (Free Edition)

### What is a SQL Warehouse?
A SQL Warehouse is the compute engine in Databricks that:
- Runs SQL queries on your Delta tables
- Powers dbt transformations (Silver + Gold models)
- Serves Power BI dashboards via a connection string

### What We Found
Databricks Free Edition automatically provisions a SQL Warehouse — no manual creation needed.

**Navigation:**
Left sidebar → **Compute** → **SQL Warehouses**

**Warehouse Details:**
| Field | Value |
|---|---|
| Name | Serverless Starter Warehouse |
| Type | Serverless |
| Size | 2X-Small |
| Status | Ready (0/1 active) |
| Created by | ncpotluri95@gmail.com |

No action required — warehouse was already provisioned and ready.

---

## Step 5 Prep — dbt Setup on Mac

### What is dbt?
dbt (data build tool) is a transformation framework that:
- Connects to your SQL Warehouse
- Runs SQL models to build Silver and Gold Delta tables
- Manages table dependencies, lineage, and documentation

### 5.1 — Install dbt-databricks

**Prerequisite:** Activate your virtual environment first.

```bash
source /Users/nageswarchowdarypotluri/my-de-project/.venv/bin/activate
```

**Why activate venv?**
Keeps dbt installed inside the project environment, not globally on your Mac. Prevents version conflicts with other Python projects.

**Install command:**
```bash
pip install dbt-databricks
```

**Verify installation:**
```bash
dbt --version
```

**Expected output:**
```
Core:
  - installed: 1.11.6

Plugins:
  - databricks: 1.11.6 - Up to date!
  - spark:      1.10.1 - Up to date!
```

---

### 5.2 — Get Databricks Connection Details

Before running `dbt init`, collect the following from Databricks:

**Navigation:**
SQL Warehouses → Serverless Starter Warehouse → **Connection details** tab

| Field | Value |
|---|---|
| Server hostname | `dbc-2b1dc14c-9aed.cloud.databricks.com` |
| HTTP path | `/sql/1.0/warehouses/3f3297dd1da19ad2` |

---

### 5.3 — Generate Personal Access Token

dbt authenticates to Databricks using a Personal Access Token (PAT).

**Navigation:**
Profile icon (top right) → **Settings** → **Developer** → **Access tokens** → **Generate new token**

**Settings used:**
| Field | Value |
|---|---|
| Description | `dbt-connection` |
| Lifetime | 90 days |
| Scope | SQL |

> **Important:** Copy the token immediately after generating — it is shown only once.
> Token format: starts with `dapi` followed by a long alphanumeric string.

---

### 5.4 — Initialise dbt Project

```bash
cd /Users/nageswarchowdarypotluri/my-de-project
dbt init racing_dbt
```

**What this does:**
Creates a new dbt project folder called `racing_dbt` with the standard folder structure (models, tests, macros, etc.) and writes your connection profile to `~/.dbt/profiles.yml`.

**Prompts and answers during `dbt init`:**

| Prompt | Answer | Why |
|---|---|---|
| Which database? | `databricks` (select number) | Our warehouse is Databricks |
| host | `dbc-------------.cloud.databricks.com` | Server hostname from Connection details |
| http_path | `/sql/1.0/warehouses/12345------` | HTTP path from Connection details |
| Desired access token option | `1` (token auth) | Simplest auth method for local dev |
| token | `Your_Token` (your PAT) | Authentication to Databricks |
| Scope | `SQL` | dbt only needs SQL access |
| Unity Catalog | `1` (use Unity Catalog) | Matches our plan — proper data governance |
| catalog | `main` | Default Unity Catalog in Free Edition |
| schema | `bronze` | dbt will read from Bronze layer |
| threads | `1` (default) | Fine for single-node/serverless |

---

### 5.5 — Fix profiles.yml

**Problem encountered:**
During `dbt init`, the host and token values were accidentally entered in the wrong fields. The terminal arrow key inputs also corrupted the token field.


** profiles.yml:**
```yaml
racing_dbt:
  outputs:
    dev:
      catalog: main
      host: dbc-2b1dc14c-9aed.cloud.databricks.com   # ← correct hostname
      http_path: /sql/1.0/warehouses/3f3297dd1da19ad2
      schema: bronze
      threads: 1
      token: 'Your_Token'     # ← correct token
      type: databricks
  target: dev
```

**File location:** `/Users/nageswarchowdarypotluri/.dbt/profiles.yml`

**Fix applied:** Manually edited the file to swap host and token to their correct fields.

---

### 5.6 — Validate Connection

```bash
cd /Users/nageswarchowdarypotluri/my-de-project/racing_dbt
dbt debug
```

**What this does:**
Checks that:
- `profiles.yml` is valid
- `dbt_project.yml` is valid
- Git is installed
- Connection to Databricks SQL Warehouse is successful

**Expected output (success):**
```
Configuration:
  profiles.yml file [OK found and valid]
  dbt_project.yml file [OK found and valid]
Required dependencies:
  - git [OK found]
Connection:
  host: dbc-2b1dc14c-9aed.cloud.databricks.com
  http_path: /sql/1.0/warehouses/3f3297dd1da19ad2
  catalog: main
  schema: bronze
Connection test: [OK connection ok]

All checks passed!
```

✅ dbt is successfully connected to Databricks SQL Warehouse.

---

## dbt Project Structure

```
racing_dbt/
├── models/             ← SQL transformation models (Silver + Gold)
├── tests/              ← Data quality tests
├── macros/             ← Reusable SQL snippets
├── seeds/              ← Static CSV data (lookup tables)
├── snapshots/          ← SCD Type 2 history tracking
├── analyses/           ← Ad hoc SQL (not materialised)
├── dbt_project.yml     ← Project configuration
└── README.md
```

---

## Current Status

| Step | Description | Status |
|---|---|---|
| 1 | Mount S3 Bronze to Databricks | ⏳ Needs 14-day Trial |
| 2 | Auto Loader (S3 → Bronze Delta) | ⏳ Needs 14-day Trial |
| 3 | Create Bronze Delta tables | ⏳ Needs 14-day Trial |
| 4 | SQL Warehouse | ✅ Done (Free Edition) |
| 5 | Run dbt models (Silver + Gold) | ⏳ Needs Bronze tables first |
| 6 | Register Silver + Gold Delta tables | ⏳ Needs dbt models |
| 7 | Unity Catalog | ⏳ Needs 14-day Trial |
| 8 | Serve to Power BI | ⏳ Needs Gold tables |

---

## Before Starting the Trial

Complete these Bronze loading tasks first:

- [ ] Finish `harness_race` table in PostgreSQL → upload to S3 Bronze
- [ ] Build `race_runners` table (FormFav API) → upload to S3 Bronze
- [ ] Build `race_info` table → upload to S3 Bronze
- [ ] Build `openmeteo_api_to_s3_bronze.py` → upload to S3 Bronze

---

## Key File Locations

| File | Path |
|---|---|
| dbt project | `/Users/nageswarchowdarypotluri/my-de-project/racing_dbt/` |
| dbt profile | `/Users/nageswarchowdarypotluri/.dbt/profiles.yml` |
| Python venv | `/Users/nageswarchowdarypotluri/my-de-project/.venv/` |



can i set it as a reamefile in git or what