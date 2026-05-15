# Databricks notebook source
# Silver Layer — Harness & Reference Tables
# Tables: dim_tracks | harness_meetings | harness_races | harness_runners | harness_runner_stats | dim_horse_form
# Run this notebook BEFORE Silver_Betfair_Final (betfair_catalogue depends on dim_tracks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 1 — dim_tracks MERGE (foundation — run first)
# MAGIC NOTE: After first run, manually verify track_code uniqueness.
# MAGIC marburg and maryborough both auto-generate MAR — fix one manually if both appear.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO harness_stream.silver.dim_tracks t
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         track_slug,
# MAGIC         MAX(state)   AS state,
# MAGIC         MAX(country) AS country
# MAGIC     FROM (
# MAGIC         SELECT track_slug, state, country FROM harness_stream.bronze.harness_meetings
# MAGIC         WHERE country = 'au' AND track_slug IS NOT NULL
# MAGIC         UNION ALL
# MAGIC         SELECT track_slug, state, country FROM harness_stream.bronze.harness_races
# MAGIC         WHERE country = 'au' AND track_slug IS NOT NULL
# MAGIC     )
# MAGIC     GROUP BY track_slug
# MAGIC ) s
# MAGIC ON t.track_slug = s.track_slug
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (track_slug, track_code, track_name, state, country)
# MAGIC     VALUES (
# MAGIC         s.track_slug,
# MAGIC         UPPER(LEFT(REPLACE(s.track_slug, '-', ''), 3)),
# MAGIC         INITCAP(REPLACE(s.track_slug, '-', ' ')),
# MAGIC         s.state,
# MAGIC         s.country
# MAGIC     );

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 2 — harness_meetings
# MAGIC meeting_id format: ddMMyyyy — matches meeting_code in harness_races

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE harness_stream.silver.harness_meetings
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH ranked AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY track_slug, meeting_date
# MAGIC             ORDER BY _bronze_loaded_at DESC
# MAGIC         ) AS _rank
# MAGIC     FROM harness_stream.bronze.harness_meetings
# MAGIC     WHERE country = 'au'
# MAGIC )
# MAGIC SELECT
# MAGIC     CONCAT(t.track_code, DATE_FORMAT(CAST(m.meeting_date AS DATE), 'ddMMyyyy')) AS meeting_id,
# MAGIC     m.track_slug,
# MAGIC     CAST(m.meeting_date AS DATE)                                                 AS meeting_date,
# MAGIC     m.race_code                                                                  AS racing_code,
# MAGIC     t.state,
# MAGIC     m.country,
# MAGIC     m.country_name,
# MAGIC     m.name                                                                       AS meeting_name,
# MAGIC     CAST(m.number_of_races AS INT)                                               AS number_of_races,
# MAGIC     m.track_condition,
# MAGIC     m.weather,
# MAGIC     CAST(m._ingested_at AS TIMESTAMP)                                            AS _ingested_at,
# MAGIC     m._bronze_loaded_at,
# MAGIC     current_timestamp()                                                          AS _silver_loaded_at
# MAGIC FROM ranked m
# MAGIC LEFT JOIN harness_stream.silver.dim_tracks t ON t.track_slug = m.track_slug
# MAGIC WHERE m._rank = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 3 — harness_races

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE harness_stream.silver.harness_races
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH ranked AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY track_slug, race_date, race_number
# MAGIC             ORDER BY _bronze_loaded_at DESC
# MAGIC         ) AS _rank
# MAGIC     FROM harness_stream.bronze.harness_races
# MAGIC     WHERE country = 'au'
# MAGIC )
# MAGIC SELECT
# MAGIC     CONCAT(t.track_code,
# MAGIC            DATE_FORMAT(CAST(r.race_date AS DATE), 'ddMMyyyy'),
# MAGIC            LPAD(CAST(r.race_number AS STRING), 2, '0'))                         AS race_code,
# MAGIC     CONCAT(t.track_code,
# MAGIC            DATE_FORMAT(CAST(r.race_date AS DATE), 'ddMMyyyy'))                  AS meeting_code,
# MAGIC     CAST(r.race_date AS DATE)                                                   AS race_date,
# MAGIC     t.track_name,
# MAGIC     r.race_number,
# MAGIC     r.race_name,
# MAGIC     r.number_of_runners                                                         AS total_runners,
# MAGIC     r.race_class                                                                AS class,
# MAGIC     r.race_code                                                                 AS race_type,
# MAGIC     r.track_slug                                                                AS track,
# MAGIC     r.distance_raw                                                              AS distance,
# MAGIC     CAST(REGEXP_EXTRACT(COALESCE(r.distance_raw, '0'), '([0-9]+)', 1) AS INT)  AS distance_metres,
# MAGIC     r._source_system                                                            AS source,
# MAGIC     r.condition,
# MAGIC     r.country,
# MAGIC     r.country_name,
# MAGIC     CAST(REGEXP_EXTRACT(COALESCE(r.prize_money_raw, '0'), '([0-9]+)', 1) AS INT) AS prize_money,
# MAGIC     CAST(r._ingested_at AS TIMESTAMP)                                           AS _ingested_at,
# MAGIC     CAST(r._bronze_loaded_at AS TIMESTAMP)                                      AS _bronze_loaded_at,
# MAGIC     current_timestamp()                                                         AS _silver_loaded_at
# MAGIC FROM ranked r
# MAGIC LEFT JOIN harness_stream.silver.dim_tracks t ON t.track_slug = r.track_slug
# MAGIC WHERE r._rank = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 4 — harness_runners

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE harness_stream.silver.harness_runners
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH ranked AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY track_slug, race_date, race_number
# MAGIC             ORDER BY _bronze_loaded_at DESC
# MAGIC         ) AS _rank
# MAGIC     FROM harness_stream.bronze.harness_races
# MAGIC     WHERE country = 'au'
# MAGIC )
# MAGIC SELECT
# MAGIC     CONCAT(t.track_code,
# MAGIC            DATE_FORMAT(CAST(r.race_date AS DATE), 'ddMMyyyy'),
# MAGIC            LPAD(CAST(r.race_number AS STRING), 2, '0'))                         AS race_code,
# MAGIC     CONCAT(t.track_code,
# MAGIC            DATE_FORMAT(CAST(r.race_date AS DATE), 'ddMMyyyy'))                  AS meeting_code,
# MAGIC     runner.name                                                                 AS horse_name,
# MAGIC     runner.number                                                               AS horse_number,
# MAGIC     runner.barrier                                                              AS barrier,
# MAGIC     runner.jockey                                                               AS jockey,
# MAGIC     runner.trainer                                                              AS trainer,
# MAGIC     runner.age                                                                  AS age,
# MAGIC     runner.sex                                                                  AS sex,
# MAGIC     runner.country                                                              AS horse_country,
# MAGIC     CAST(NULL AS STRING)                                                        AS gear,
# MAGIC     runner.racingColours                                                        AS racing_colours,
# MAGIC     runner.classProfile                                                         AS class_profile,
# MAGIC     runner.form                                                                 AS recent_form,
# MAGIC     runner.last20Starts                                                         AS last_20,
# MAGIC     runner.gearChange                                                           AS gear_changes,
# MAGIC     runner.careerPrizeMoney                                                     AS prize_money,
# MAGIC     CAST(runner.weight AS STRING)                                               AS weight,
# MAGIC     runner.sire                                                                 AS sire,
# MAGIC     runner.dam                                                                  AS dam,
# MAGIC     runner.scratched                                                            AS is_scratched,
# MAGIC     CONVERT_TIMEZONE('UTC','Australia/Sydney',CAST(r._ingested_at AS TIMESTAMP)) AS _ingested_at,
# MAGIC     CONVERT_TIMEZONE('UTC','Australia/Sydney', r._bronze_loaded_at)            AS _bronze_loaded_at,
# MAGIC     CONVERT_TIMEZONE('UTC','Australia/Sydney', current_timestamp())            AS _silver_loaded_at
# MAGIC FROM ranked r
# MAGIC LEFT JOIN harness_stream.silver.dim_tracks t ON t.track_slug = r.track_slug
# MAGIC LATERAL VIEW EXPLODE(r.runners) AS runner
# MAGIC WHERE r._rank = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 5 — harness_runner_stats
# MAGIC 7 stat groups: overall | track | distance | trackDistance | condition | firstUp | secondUp

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE harness_stream.silver.harness_runner_stats
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH ranked AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY track_slug, race_date, race_number
# MAGIC             ORDER BY _bronze_loaded_at DESC
# MAGIC         ) AS _rank
# MAGIC     FROM harness_stream.bronze.harness_races
# MAGIC     WHERE country = 'au'
# MAGIC ),
# MAGIC base AS (
# MAGIC     SELECT
# MAGIC         CONCAT(t.track_code,
# MAGIC                DATE_FORMAT(CAST(r.race_date AS DATE), 'ddMMyyyy'),
# MAGIC                LPAD(CAST(r.race_number AS STRING), 2, '0'))  AS race_id,
# MAGIC         runner.name                                          AS horse_name,
# MAGIC         runner.number                                        AS horse_number,
# MAGIC         runner.stats                                         AS stats
# MAGIC     FROM ranked r
# MAGIC     LEFT JOIN harness_stream.silver.dim_tracks t ON t.track_slug = r.track_slug
# MAGIC     LATERAL VIEW EXPLODE(r.runners) AS runner
# MAGIC     WHERE r._rank = 1
# MAGIC )
# MAGIC SELECT race_id, horse_name, horse_number, 'overall' AS stat_group,
# MAGIC     stats.overall.starts        AS starts,
# MAGIC     stats.overall.wins          AS wins,
# MAGIC     stats.overall.places        AS places,
# MAGIC     stats.overall.seconds       AS seconds,
# MAGIC     stats.overall.thirds        AS thirds,
# MAGIC     stats.overall.winPercent    AS win_pct,
# MAGIC     stats.overall.placePercent  AS place_pct
# MAGIC FROM base WHERE stats.overall IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT race_id, horse_name, horse_number, 'track',
# MAGIC     stats.track.starts, stats.track.wins, stats.track.places,
# MAGIC     stats.track.seconds, stats.track.thirds,
# MAGIC     stats.track.winPercent, stats.track.placePercent
# MAGIC FROM base WHERE stats.track IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT race_id, horse_name, horse_number, 'distance',
# MAGIC     stats.distance.starts, stats.distance.wins, stats.distance.places,
# MAGIC     stats.distance.seconds, stats.distance.thirds,
# MAGIC     stats.distance.winPercent, stats.distance.placePercent
# MAGIC FROM base WHERE stats.distance IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT race_id, horse_name, horse_number, 'trackDistance',
# MAGIC     stats.trackDistance.starts, stats.trackDistance.wins, stats.trackDistance.places,
# MAGIC     stats.trackDistance.seconds, stats.trackDistance.thirds,
# MAGIC     stats.trackDistance.winPercent, stats.trackDistance.placePercent
# MAGIC FROM base WHERE stats.trackDistance IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT race_id, horse_name, horse_number, 'condition',
# MAGIC     stats.condition.starts, stats.condition.wins, stats.condition.places,
# MAGIC     stats.condition.seconds, stats.condition.thirds,
# MAGIC     stats.condition.winPercent, stats.condition.placePercent
# MAGIC FROM base WHERE stats.condition IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT race_id, horse_name, horse_number, 'firstUp',
# MAGIC     stats.firstUp.starts, stats.firstUp.wins, stats.firstUp.places,
# MAGIC     stats.firstUp.seconds, stats.firstUp.thirds,
# MAGIC     stats.firstUp.winPercent, stats.firstUp.placePercent
# MAGIC FROM base WHERE stats.firstUp IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT race_id, horse_name, horse_number, 'secondUp',
# MAGIC     stats.secondUp.starts, stats.secondUp.wins, stats.secondUp.places,
# MAGIC     stats.secondUp.seconds, stats.secondUp.thirds,
# MAGIC     stats.secondUp.winPercent, stats.secondUp.placePercent
# MAGIC FROM base WHERE stats.secondUp IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 6 — dim_horse_form (derived from harness_runners)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE harness_stream.silver.dim_horse_form
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH latest_per_horse AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY horse_name
# MAGIC             ORDER BY _bronze_loaded_at DESC
# MAGIC         ) AS rn
# MAGIC     FROM harness_stream.silver.harness_runners
# MAGIC     WHERE last_20 IS NOT NULL
# MAGIC ),
# MAGIC most_recent AS (SELECT * FROM latest_per_horse WHERE rn = 1),
# MAGIC exploded AS (
# MAGIC     SELECT
# MAGIC         race_code,
# MAGIC         horse_number,
# MAGIC         horse_name,
# MAGIC         pos,
# MAGIC         SUBSTR(last_20, pos + 1, 1) AS result_code,
# MAGIC         LENGTH(last_20) - pos        AS races_ago
# MAGIC     FROM most_recent
# MAGIC     LATERAL VIEW POSEXPLODE(SPLIT(last_20, '')) t AS pos, val
# MAGIC     WHERE SUBSTR(last_20, pos + 1, 1) != ''
# MAGIC )
# MAGIC SELECT
# MAGIC     horse_number,
# MAGIC     horse_name,
# MAGIC     races_ago,
# MAGIC     result_code,
# MAGIC     CASE result_code
# MAGIC         WHEN '1' THEN 'Won'           WHEN '2' THEN '2nd'
# MAGIC         WHEN '3' THEN '3rd'           WHEN '4' THEN '4th'
# MAGIC         WHEN '5' THEN '5th'           WHEN '6' THEN '6th'
# MAGIC         WHEN '7' THEN '7th'           WHEN '8' THEN '8th'
# MAGIC         WHEN '9' THEN '9th'           WHEN '0' THEN '10th or worse'
# MAGIC         WHEN 'X' THEN 'Scratched'     WHEN 'S' THEN 'Standing Start'
# MAGIC         ELSE 'Unknown'
# MAGIC     END AS meaning,
# MAGIC     SUM(CASE WHEN result_code = '1' THEN 1 ELSE 0 END)
# MAGIC         OVER (PARTITION BY horse_name) AS total_wins_last_20,
# MAGIC     SUM(CASE WHEN result_code = '2' THEN 1 ELSE 0 END)
# MAGIC         OVER (PARTITION BY horse_name) AS total_seconds_last_20,
# MAGIC     SUM(CASE WHEN result_code = '3' THEN 1 ELSE 0 END)
# MAGIC         OVER (PARTITION BY horse_name) AS total_thirds_last_20
# MAGIC FROM exploded;

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 7 — Data Quality Checks

# COMMAND ----------

checks = [
    ("harness_stream.silver.harness_meetings",     ["meeting_id"],                        ["meeting_id", "track_slug"]),
    ("harness_stream.silver.harness_races",        ["race_code"],                         ["race_code", "meeting_code", "race_date"]),
    ("harness_stream.silver.harness_runners",      ["race_code", "horse_name"],           ["horse_name", "race_code"]),
    ("harness_stream.silver.harness_runner_stats", ["race_id", "horse_name", "stat_group"],  ["race_id", "horse_name", "stat_group"]),
]

for table, unique_keys, not_null_cols in checks:
    print(f"\n── {table} ──")
    checks_sql = ", ".join([
        f"COUNT(CASE WHEN {c} IS NULL THEN 1 END) AS null_{c}"
        for c in not_null_cols
    ])
    spark.sql(f"SELECT COUNT(*) AS total_rows, {checks_sql} FROM {table}").show()
    key_str = ", ".join(unique_keys)
    dupes = spark.sql(f"""
        SELECT COUNT(*) AS d FROM (
            SELECT {key_str}, COUNT(*) AS cnt
            FROM {table}
            GROUP BY {key_str}
            HAVING cnt > 1
        )
    """).collect()[0]['d']
    if dupes > 0:
        raise ValueError(f"❌ {table} — {dupes} duplicates on ({key_str})")
    print(f"   ✅ No duplicates on ({key_str})")

print("\n✅ All harness quality checks passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CELL 8 — OPTIMIZE + ZORDER

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE harness_stream.silver.harness_meetings     ZORDER BY (meeting_id);
# MAGIC OPTIMIZE harness_stream.silver.harness_races        ZORDER BY (race_code, race_date);
# MAGIC OPTIMIZE harness_stream.silver.harness_runners      ZORDER BY (race_code, horse_name);
# MAGIC OPTIMIZE harness_stream.silver.harness_runner_stats ZORDER BY (race_id, stat_group);
# MAGIC OPTIMIZE harness_stream.silver.dim_horse_form       ZORDER BY (horse_name);

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC SELECT meeting_id, COUNT(*) AS cnt, COLLECT_LIST(track_slug) AS slugs
# MAGIC FROM harness_stream.silver.harness_meetings
# MAGIC GROUP BY meeting_id
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY meeting_id

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC -- Example: rename maryborough to MBR to avoid the MAR collision
# MAGIC UPDATE harness_stream.silver.dim_tracks
# MAGIC SET track_code = 'MBR'
# MAGIC WHERE track_slug = 'maryborough';
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC -- MOR collision: mornington vs morphettville
# MAGIC UPDATE harness_stream.silver.dim_tracks SET track_code = 'MOV' WHERE track_slug = 'morphettville';
# MAGIC
# MAGIC -- POR collision: port-augusta vs port-macquarie
# MAGIC UPDATE harness_stream.silver.dim_tracks SET track_code = 'PAU' WHERE track_slug = 'port-augusta';
# MAGIC UPDATE harness_stream.silver.dim_tracks SET track_code = 'PMQ' WHERE track_slug = 'port-macquarie';
# MAGIC
# MAGIC -- SUN collision: sunshine-coast vs sunshine-coast-polytrack
# MAGIC UPDATE harness_stream.silver.dim_tracks SET track_code = 'SUP' WHERE track_slug = 'sunshine-coast-polytrack';
# MAGIC
# MAGIC -- WAR collision: warrnambool vs warwick-farm vs warwick
# MAGIC UPDATE harness_stream.silver.dim_tracks SET track_code = 'WFM' WHERE track_slug = 'warwick-farm';
# MAGIC UPDATE harness_stream.silver.dim_tracks SET track_code = 'WRW' WHERE track_slug = 'warwick';

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC SELECT race_code, COUNT(*) AS cnt, COLLECT_LIST(track_name) AS slugs
# MAGIC FROM harness_stream.silver.harness_races
# MAGIC GROUP BY race_code
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY race_code

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC SELECT race_code, horse_number, COUNT(*) AS cnt, COLLECT_LIST(horse_name) AS names
# MAGIC FROM harness_stream.silver.harness_runners
# MAGIC GROUP BY race_code, horse_number
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY race_code
# MAGIC LIMIT 10
