# Databricks notebook source
# Silver Layer — Betfair Tables
# Tables: betfair_market_book | betfair_runner_odds | betfair_catalogue
# Run order: this notebook runs AFTER Silver_Races_Final (dim_tracks must exist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 1 — Table Properties (run once on first setup)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE harness_stream.silver.betfair_catalogue
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true',
# MAGIC     'delta.enableChangeDataFeed'       = 'true'
# MAGIC );
# MAGIC ALTER TABLE harness_stream.silver.betfair_market_book
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true',
# MAGIC     'delta.enableChangeDataFeed'       = 'true'
# MAGIC );
# MAGIC ALTER TABLE harness_stream.silver.betfair_runner_odds
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true',
# MAGIC     'delta.enableChangeDataFeed'       = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 2 — Shared Dedup View (must run before market_book and runner_odds)

# COMMAND ----------

# %python
latest_bronze_df = spark.sql("""
    WITH ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY marketId, run_time
                ORDER BY _bronze_loaded_at DESC
            ) AS _rank
        FROM harness_stream.bronze.betfair_market_book
    )
    SELECT * FROM ranked WHERE _rank = 1
""")
latest_bronze_df.createOrReplaceTempView("latest_snapshot_view")
print("✅ latest_snapshot_view ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 3 — betfair_market_book MERGE

# COMMAND ----------

market_df = spark.sql("""
    SELECT
        marketId                                                                    AS market_id,
        CASE
            WHEN status = 'CLOSED'                               THEN 'POST_RACE'
            WHEN status IN ('OPEN','SUSPENDED') AND inplay = TRUE  THEN 'INPLAY'
            WHEN status IN ('OPEN','SUSPENDED') AND inplay = FALSE THEN 'PRE_RACE'
            ELSE snapshot_type
        END                                                                         AS snapshot_type,
        status,
        inplay,
        complete,
        extracted_date,
        bspReconciled                                                               AS bsp_reconciled,
        betDelay                                                                    AS bet_delay,
        isMarketDataDelayed                                                         AS is_market_data_delayed,
        numberOfRunners                                                             AS number_of_runners,
        numberOfActiveRunners                                                       AS number_of_active_runners,
        numberOfWinners                                                             AS number_of_winners,
        ROUND(CAST(totalMatched   AS DOUBLE), 2)                                    AS total_matched,
        ROUND(CAST(totalAvailable AS DOUBLE), 2)                                    AS total_available,
        CONVERT_TIMEZONE('UTC','Australia/Sydney',CAST(lastMatchTime AS TIMESTAMP)) AS last_match_time,
        version,
        run_time,
        CONVERT_TIMEZONE('UTC','Australia/Sydney', _bronze_loaded_at)              AS _bronze_loaded_at,
        CONVERT_TIMEZONE('UTC','Australia/Sydney', current_timestamp())            AS _silver_loaded_at
    FROM latest_snapshot_view
""")

market_df.createOrReplaceTempView("market_updates")

spark.sql("""
    MERGE INTO harness_stream.silver.betfair_market_book target
    USING market_updates source
    ON  target.market_id = source.market_id
    AND target.run_time IS NOT DISTINCT FROM source.run_time
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("✅ betfair_market_book updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 4 — betfair_runner_odds MERGE

# COMMAND ----------

runners_df = spark.sql("""
    SELECT
        r.marketId                                                                  AS market_id,
        CASE
            WHEN r.status = 'CLOSED'                               THEN 'POST_RACE'
            WHEN r.status IN ('OPEN','SUSPENDED') AND r.inplay = TRUE  THEN 'INPLAY'
            WHEN r.status IN ('OPEN','SUSPENDED') AND r.inplay = FALSE THEN 'PRE_RACE'
            ELSE r.snapshot_type
        END                                                                         AS snapshot_type,
        r.extracted_date,
        r.run_time,
        r.status,
        r.inplay,
        r.complete,
        r.bspReconciled                                                             AS bsp_reconciled,
        r.betDelay                                                                  AS bet_delay,
        r.isMarketDataDelayed                                                       AS is_market_data_delayed,
        r.numberOfRunners                                                           AS number_of_runners,
        r.numberOfActiveRunners                                                     AS number_of_active_runners,
        r.numberOfWinners                                                           AS number_of_winners,
        ROUND(CAST(r.totalMatched   AS DOUBLE), 2)                                  AS total_matched,
        ROUND(CAST(r.totalAvailable AS DOUBLE), 2)                                  AS total_available,
        CONVERT_TIMEZONE('UTC','Australia/Sydney',CAST(r.lastMatchTime AS TIMESTAMP)) AS last_match_time,
        r.version,
        CAST(ra.value:selectionId AS INT)                                           AS selection_id,
        ra.value:status::string                                                     AS runner_status,
        ra.value:handicap::double                                                   AS handicap,
        ra.value:adjustmentFactor::double                                           AS adjustment_factor,
        ra.value:lastPriceTraded::double                                            AS last_price_traded,
        ROUND(ra.value:totalMatched::double, 2)                                     AS runner_total_matched,
        CONVERT_TIMEZONE('UTC','Australia/Sydney',
            CAST(ra.value:removalDate::string AS TIMESTAMP))                        AS removal_date,
        ra.value:ex.availableToBack[0].price::double                               AS best_back_price,
        ra.value:ex.availableToBack[0].size::double                                AS best_back_size,
        ra.value:ex.availableToLay[0].price::double                                AS best_lay_price,
        ra.value:ex.availableToLay[0].size::double                                 AS best_lay_size,
        ra.value:sp.actualSP::double                                                AS bsp,
        ra.value:sp.nearPrice::double                                               AS bsp_near_price,
        ra.value:sp.farPrice::double                                                AS bsp_far_price,
        CONVERT_TIMEZONE('UTC','Australia/Sydney', r._bronze_loaded_at)            AS _bronze_loaded_at,
        CONVERT_TIMEZONE('UTC','Australia/Sydney', current_timestamp())            AS _silver_loaded_at
    FROM latest_snapshot_view r
    CROSS JOIN LATERAL variant_explode(parse_json(r.runners)) AS ra
""")

runners_df.createOrReplaceTempView("runner_updates")

spark.sql("""
    MERGE INTO harness_stream.silver.betfair_runner_odds target
    USING runner_updates source
    ON  target.market_id    = source.market_id
    AND target.run_time     IS NOT DISTINCT FROM source.run_time
    AND target.selection_id = source.selection_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("✅ betfair_runner_odds updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 5 — betfair_catalogue MERGE (with race_code)

# COMMAND ----------

try:
    spark.sql("ALTER TABLE harness_stream.silver.betfair_catalogue ADD COLUMNS (race_code STRING)")
    print("race_code column added.")
except Exception:
    print("race_code column already exists, skipping.")

catalogue_updates = spark.sql("""
    WITH ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY market_id
                ORDER BY _bronze_loaded_at DESC
            ) AS _rank
        FROM harness_stream.bronze.betfair_catalogue
        WHERE event_country = 'AU'
          AND (market_name ILIKE '%Pace%' OR market_name ILIKE '%Trot%')
    )
    SELECT
        r.event_id,
        r.market_id,
        r.event_name,
        r.market_name,
        CONVERT_TIMEZONE('UTC','Australia/Sydney',
            CAST(r.market_start_time AS TIMESTAMP))                                 AS market_start_time,
        r.event_venue,
        r.event_country,
        CAST(CONVERT_TIMEZONE('UTC','Australia/Sydney',
            CAST(r.event_open_date AS TIMESTAMP)) AS DATE)                          AS event_date,
        CONVERT_TIMEZONE('UTC','Australia/Sydney',
            CAST(r.event_open_date AS TIMESTAMP))                                   AS event_open_date,
        ROUND(CAST(r.total_matched AS DOUBLE), 2)                                   AS total_matched,
        ra.value:handicap::double                                                   AS runner_handicap,
        CAST(split(ra.value:runnerName::string, '\\\\. ')[0] AS INT)                AS barrier_number,
        trim(split(ra.value:runnerName::string, '\\\\. ')[1])                       AS runner_name,
        ra.value:selectionId::int                                                   AS selection_id,
        ra.value:sortPriority::int                                                  AS sort_priority,
        CASE
            WHEN t.track_code IS NOT NULL
             AND REGEXP_EXTRACT(r.market_name, 'R([0-9]+)', 1) IS NOT NULL
             AND REGEXP_EXTRACT(r.market_name, 'R([0-9]+)', 1) != ''
            THEN CONCAT(
                t.track_code,
                DATE_FORMAT(
                    CAST(CONVERT_TIMEZONE('UTC','Australia/Sydney',
                        CAST(r.event_open_date AS TIMESTAMP)) AS DATE),
                    'ddMMyyyy'
                ),
                LPAD(REGEXP_EXTRACT(r.market_name, 'R([0-9]+)', 1), 2, '0')
            )
            ELSE NULL
        END                                                                         AS race_code,
        CONVERT_TIMEZONE('UTC','Australia/Sydney', r._bronze_loaded_at)            AS _bronze_loaded_at,
        CONVERT_TIMEZONE('UTC','Australia/Sydney', current_timestamp())            AS _silver_loaded_at
    FROM ranked r
    CROSS JOIN LATERAL variant_explode(parse_json(r.runners)) AS ra
    LEFT JOIN harness_stream.silver.dim_tracks t
        ON LOWER(TRIM(r.event_venue)) = LOWER(TRIM(t.track_name))
    WHERE r._rank = 1
""")

catalogue_updates.createOrReplaceTempView("cat_updates_view")

spark.sql("""
    MERGE INTO harness_stream.silver.betfair_catalogue target
    USING cat_updates_view source
    ON  target.market_id    = source.market_id
    AND target.selection_id = source.selection_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("✅ betfair_catalogue updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 6 — Data Quality Checks

# COMMAND ----------

def quality_check(table, unique_keys, not_null_cols):
    print(f"\n── {table} ──")
    checks = ", ".join([
        f"COUNT(CASE WHEN {c} IS NULL THEN 1 END) AS null_{c}"
        for c in not_null_cols
    ])
    spark.sql(f"SELECT COUNT(*) AS total_rows, {checks} FROM {table}").show()
    key_str = ", ".join(unique_keys)
    dupes = spark.sql(f"""
        SELECT COUNT(*) AS dupes FROM (
            SELECT {key_str}, COUNT(*) AS cnt
            FROM {table}
            GROUP BY {key_str}
            HAVING cnt > 1
        )
    """).collect()[0]['dupes']
    if dupes > 0:
        raise ValueError(f"❌ {table} — {dupes} duplicate keys on ({key_str})")
    print(f"   ✅ No duplicates on ({key_str})")

quality_check(
    "harness_stream.silver.betfair_market_book",
    ["market_id", "run_time"],
    ["market_id", "status"]
)
quality_check(
    "harness_stream.silver.betfair_runner_odds",
    ["market_id", "run_time", "selection_id"],
    ["market_id", "selection_id", "runner_status"]
)
quality_check(
    "harness_stream.silver.betfair_catalogue",
    ["market_id", "selection_id"],
    ["market_id", "selection_id", "runner_name"]
)
print("\n✅ All quality checks passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 7 — OPTIMIZE + ZORDER

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE harness_stream.silver.betfair_market_book  ZORDER BY (market_id);
# MAGIC OPTIMIZE harness_stream.silver.betfair_runner_odds  ZORDER BY (market_id, selection_id);
# MAGIC OPTIMIZE harness_stream.silver.betfair_catalogue    ZORDER BY (race_code, selection_id);

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE harness_stream.silver.betfair_market_book
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * EXCEPT (_rn) FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (
# MAGIC         PARTITION BY market_id, run_time
# MAGIC         ORDER BY _bronze_loaded_at DESC
# MAGIC     ) AS _rn
# MAGIC     FROM harness_stream.silver.betfair_market_book
# MAGIC ) WHERE _rn = 1;
# MAGIC
# MAGIC ALTER TABLE harness_stream.silver.betfair_market_book
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true',
# MAGIC     'delta.enableChangeDataFeed'       = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE harness_stream.silver.betfair_runner_odds
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * EXCEPT (_rn) FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (
# MAGIC         PARTITION BY market_id, run_time, selection_id
# MAGIC         ORDER BY _bronze_loaded_at DESC
# MAGIC     ) AS _rn
# MAGIC     FROM harness_stream.silver.betfair_runner_odds
# MAGIC ) WHERE _rn = 1;
# MAGIC
# MAGIC ALTER TABLE harness_stream.silver.betfair_runner_odds
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true',
# MAGIC     'delta.enableChangeDataFeed'       = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE harness_stream.silver.betfair_catalogue
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * EXCEPT (_rn) FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (
# MAGIC         PARTITION BY market_id, selection_id
# MAGIC         ORDER BY _bronze_loaded_at DESC
# MAGIC     ) AS _rn
# MAGIC     FROM harness_stream.silver.betfair_catalogue
# MAGIC ) WHERE _rn = 1;
# MAGIC
# MAGIC ALTER TABLE harness_stream.silver.betfair_catalogue
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true',
# MAGIC     'delta.enableChangeDataFeed'       = 'true'
# MAGIC );
