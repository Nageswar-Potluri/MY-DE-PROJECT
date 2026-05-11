# ==============================================================================
# bronze_autoloader.py
# Auto Loader: FormFav & Betfair → S3 Bronze → Delta Tables (harness_stream.bronze)
#
# Modes:
#   all      — run all streams once with availableNow=True  (scheduled daily)
#   dynamic  — run betfair_market_book dynamically based on today's race schedule
#              reads catalogue from S3, triggers at T-5 min, polls until CLOSED
#
# Catalog  : harness_stream.bronze
# ==============================================================================


# COMMAND ----------
# SECTION 1 — IMPORTS & SPARK CONFIG
# ==============================================================================
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Runs in both environments:
#   - Inside Databricks notebook → spark is pre-injected, this block is skipped
#   - Local VS Code via Databricks Connect → creates a serverless session
try:
    spark
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder \
        .serverless(True) \
        .profile("VS_Databricks_connect") \
        .getOrCreate()

# optimizeWrite and autoCompact are managed automatically by Serverless


# COMMAND ----------
# SECTION 1b — OVERWRITE PARAMETER
# ==============================================================================
# When triggered from Airflow with overwrite=true, clears S3 checkpoints and
# schema locations so Auto Loader reprocesses all files from scratch.
# Default is false — normal incremental run.
try:
    dbutils.widgets.text("overwrite", "false")
    overwrite = dbutils.widgets.get("overwrite").lower() == "true"
except Exception:
    overwrite = False  # running locally via Databricks Connect

if overwrite:
    print("[Overwrite] Mode ON — checkpoints and schema locations will be cleared")


# COMMAND ----------
# SECTION 2 — STREAM REGISTRY
# Add a new entry here to onboard a new feed. Nothing else needs to change.
# ==============================================================================
BUCKET  = "s3://project-racing-bronze"
CATALOG = "harness_stream.bronze"

STREAMS = {

    "harness_meetings": {
        "source_path":     f"{BUCKET}/formfav/meetings/",
        "schema_location": f"{BUCKET}/_schema/formfav_meetings/",
        "checkpoint":      f"{BUCKET}/_checkpoints/formfav_meetings/",
        "target_table":    f"{CATALOG}.harness_meetings",
        "dlo_table":       f"{CATALOG}.dlo_harness_meetings",
        "schema_hints":    None,
        "merge_keys":      ["track_slug", "meeting_date", "race_code"],
        "race_code_filter": "harness",
        "max_files":       50,
    },

    "harness_races": {
        "source_path":     f"{BUCKET}/formfav/races/",
        "schema_location": f"{BUCKET}/_schema/formfav_races/",
        "checkpoint":      f"{BUCKET}/_checkpoints/formfav_races/",
        "target_table":    f"{CATALOG}.harness_races",
        "dlo_table":       f"{CATALOG}.dlo_harness_races",
        "race_code_filter": "harness",
        # runners is a deep array of structs — force to STRING to avoid schema explosions
        "schema_hints":    "runners STRING",
        "merge_keys":      ["track_slug", "race_date", "race_number", "race_code"],
        "max_files":       50,
    },

    "betfair_catalogue": {
        "source_path":     f"{BUCKET}/betfair/market_catalogue/",
        "schema_location": f"{BUCKET}/_schema/betfair_catalogue/",
        "checkpoint":      f"{BUCKET}/_checkpoints/betfair_catalogue/",
        "target_table":    f"{CATALOG}.betfair_catalogue",
        "dlo_table":       f"{CATALOG}.dlo_betfair_catalogue",
        # runners = array of structs, event/eventType = structs
        "schema_hints":    "runners STRING, event STRING, eventType STRING",
        "merge_keys":      ["market_id"],
        "max_files":       50,
    },

    "betfair_market_book": {
        "source_path":     f"{BUCKET}/betfair/market_book/",
        "schema_location": f"{BUCKET}/_schema/betfair_market_book/",
        "checkpoint":      f"{BUCKET}/_checkpoints/betfair_market_book/",
        "target_table":    f"{CATALOG}.betfair_market_book",
        "dlo_table":       f"{CATALOG}.dlo_betfair_market_book",
        # runners = deeply nested array, betDelayModels = string array
        "schema_hints":    "runners STRING, betDelayModels STRING",
        # marketId is camelCase — MarketBook Pydantic model preserves the raw API field name
        # run_time is the poll timestamp from the S3 path — keeps every snapshot as a separate row
        "merge_keys":      ["marketId", "snapshot_type", "run_time"],
        "max_files":       100,
    },

    "betfair_historical_bsp": {
        "source_path":     f"{BUCKET}/betfair/historical_bsp/",
        "schema_location": f"{BUCKET}/_schema/betfair_historical_bsp/",
        "checkpoint":      f"{BUCKET}/_checkpoints/betfair_historical_bsp/",
        "target_table":    f"{CATALOG}.betfair_historical_bsp",
        "dlo_table":       f"{CATALOG}.dlo_betfair_historical_bsp",
        "schema_hints":    "runners STRING",
        "merge_keys":      ["market_id"],
        "max_files":       50,
    },
}


# COMMAND ----------
# SECTION 3 — AUTO LOADER READER
# ==============================================================================
def build_stream(source_path, schema_location, schema_hints, max_files, use_notifications=False):

    # Base schema hints always applied — prevents partition folder names from
    # being mis-inferred as dates or integers
    base_hints = "extracted_date STRING, race_code STRING, run_time STRING, snapshot_type STRING"
    full_hints = f"{base_hints}, {schema_hints}" if schema_hints else base_hints

    reader = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",              "json")
            .option("cloudFiles.schemaLocation",      schema_location)
            .option("cloudFiles.inferColumnTypes",    "true")
            # New columns added by the API are absorbed automatically
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            # Type mismatches / unexpected fields go here instead of being dropped
            .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
            .option("cloudFiles.schemaHints",         full_hints)
            .option("pathGlobFilter",                 "*.json")
            .option("cloudFiles.includeExistingFiles","true")
            # Throttle: process in chunks so the cluster doesn't OOM on large backfills
            .option("maxFilesPerTrigger",             str(max_files))
            # Reads all files recursively regardless of folder depth or structure.
            # Required here because old paths (race_code=.../extracted_date=...)
            # and new paths (extracted_date=.../race_code=...) have different
            # folder orderings and would cause a partition conflict with normal reads.
            .option("recursiveFileLookup",            "true")
    )

    if use_notifications:
        reader = reader \
            .option("cloudFiles.useNotifications", "true") \
            .option("cloudFiles.region",           "ap-southeast-2")

    df = (
        reader.load(source_path)

            # Audit columns
            # _metadata.file_path is the Unity Catalog equivalent of input_file_name()
            .withColumn("_source_file",      F.col("_metadata.file_path"))
            .withColumn("_bronze_loaded_at", F.from_utc_timestamp(F.current_timestamp(), "Australia/Sydney"))

            # Derive snapshot_type from the S3 folder path
            # New files: .../snapshot_type=PRE_RACE/... → extracted
            # Old files: no snapshot_type folder        → null (not guessed)
            .withColumn(
                "snapshot_type",
                F.when(
                    F.col("_metadata.file_path").contains("snapshot_type="),
                    F.regexp_extract(F.col("_metadata.file_path"), r"snapshot_type=([^/]+)", 1)
                ).otherwise(F.lit(None).cast("string"))
            )
    )
    return df


# COMMAND ----------
# SECTION 4 — FOREACHBATCH WRITER  (DLO + MERGE)
# ==============================================================================
def make_writer(target_table, dlo_table, merge_keys):

    def write_batch(batch_df, batch_id):

        # Step 1: drop intra-batch duplicates
        deduped = batch_df.dropDuplicates(merge_keys)

        # Step 2: Dead Letter Office
        # Catches two types of bad records:
        #   _rescued_data   — record parsed but had fields that didn't fit the schema
        #   _corrupt_record — record was so malformed Spark couldn't parse it at all
        dlo_filter = F.col("_rescued_data").isNotNull()
        if "_corrupt_record" in deduped.columns:
            dlo_filter = dlo_filter | F.col("_corrupt_record").isNotNull()

        bad_rows = deduped.filter(dlo_filter)
        if not bad_rows.isEmpty():
            (
                bad_rows
                    .write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .saveAsTable(dlo_table)
            )
            print(f"⚠  [{target_table}] batch {batch_id}: "
                  f"{bad_rows.count()} bad row(s) → {dlo_table}")

        # Step 3: clean rows only
        clean = deduped.filter(~dlo_filter)

        if clean.isEmpty():
            print(f"ℹ  [{target_table}] batch {batch_id}: no clean rows to write.")
            return

        # Step 4: MERGE INTO (upsert)
        # First run: table doesn't exist → create it
        # Every run after: update changed rows, insert new ones
        # Re-running the job never creates duplicates
        on_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in merge_keys]
        )

        try:
            delta_tbl = DeltaTable.forName(spark, target_table)
            (
                delta_tbl.alias("target")
                    .merge(clean.alias("source"), on_condition)
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
            )
        except Exception:
            # First run — table doesn't exist yet, create it
            (
                clean
                    .write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .partitionBy("extracted_date")
                    .saveAsTable(target_table)
            )

        print(f"✅ [{target_table}] batch {batch_id}: {clean.count()} row(s) merged.")

    return write_batch


# COMMAND ----------
# SECTION 4b — OVERWRITE: CLEAR CHECKPOINTS & SCHEMA
# ==============================================================================
# Uses dbutils.fs.rm to delete S3 checkpoint and schema folders for every stream.
# This forces Auto Loader to start fresh and reprocess all files from the source.
# Only runs when overwrite=true is passed from Airflow.
def clear_checkpoints():
    for name, cfg in STREAMS.items():
        print(f"[Overwrite] Clearing → {name}")
        for key in ("checkpoint", "schema_location"):
            path = cfg.get(key)
            if path:
                try:
                    dbutils.fs.rm(path, recurse=True)
                    print(f"  ✅ Cleared {key}: {path}")
                except Exception as e:
                    print(f"  ⚠  {key} not found or already empty: {e}")
    print("[Overwrite] All checkpoints and schema locations cleared\n")


# COMMAND ----------
# SECTION 5 — MAIN RUNNER
# ==============================================================================
def run_all_streams():
    if overwrite:
        clear_checkpoints()

    queries = []

    for name, cfg in STREAMS.items():
        print(f"[Auto Loader] Starting → {name}")

        df = build_stream(
            source_path      = cfg["source_path"],
            schema_location  = cfg["schema_location"],
            schema_hints     = cfg["schema_hints"],
            max_files        = cfg["max_files"],
            use_notifications= False,
        )

        if cfg.get("race_code_filter"):
            df = df.filter(F.col("race_code") == cfg["race_code_filter"])

        query = (
            df.writeStream
                .foreachBatch(
                    make_writer(
                        target_table = cfg["target_table"],
                        dlo_table    = cfg["dlo_table"],
                        merge_keys   = cfg["merge_keys"],
                    )
                )
                .option("checkpointLocation", cfg["checkpoint"])
                .trigger(availableNow=True)
                .queryName(f"bronze_{name}")
                .start()
        )
        queries.append((name, query))

    for name, q in queries:
        q.awaitTermination()
        print(f"[Auto Loader] Done → {name}")

    print("\n[Auto Loader] All streams complete.")



# # COMMAND ----------
# # SECTION 6 — DYNAMIC MODE
# # ==============================================================================
# # Reads today's catalogue directly from S3, builds a race schedule (same logic
# # as the ingestion pipeline's run_dynamic), and triggers the betfair_market_book
# # autoloader at T-5 min before each race. Polls every 10 seconds until all
# # markets in the group are CLOSED in the Delta table, then moves to the next race.
# # Run this right after the morning catalogue run — it stays alive all day.
# # ==============================================================================
# def run_dynamic(today: str = None):

#     SYDNEY          = ZoneInfo("Australia/Sydney")
#     TRIGGER_OFFSET  = timedelta(minutes=5)
#     GRACE_PERIOD    = timedelta(minutes=2)
#     POLL_INTERVAL   = 10   # seconds between autoloader triggers
#     MAX_POLL_MINS   = 15   # safety cap per race group

#     if today is None:
#         today = datetime.now(SYDNEY).strftime("%Y-%m-%d")

#     print(f"[Dynamic] Loading catalogue for {today}")

#     # ── Step 1: Read catalogue from S3 using Spark ────────────────────────────
#     # Reads all NDJSON files under today's catalogue partition directly from S3.
#     # Does NOT require the catalogue to be loaded into Delta first.
#     catalogue_path = f"s3://project-racing-bronze/betfair/market_catalogue/extracted_date={today}/"
#     try:
#         cat_df = (
#             spark.read
#                 .option("recursiveFileLookup", "true")
#                 .json(catalogue_path)
#                 .select("market_id", "market_start_time")
#                 .dropDuplicates(["market_id"])
#         )
#         markets = cat_df.collect()
#     except Exception as e:
#         print(f"[Dynamic] Could not read catalogue from S3: {e}")
#         return

#     if not markets:
#         print(f"[Dynamic] No catalogue data found for {today} — run catalogue ingestion first")
#         return

#     print(f"[Dynamic] {len(markets)} unique markets loaded from catalogue")

#     # ── Step 2: Group by start time ───────────────────────────────────────────
#     groups: dict = defaultdict(list)
#     for row in markets:
#         mst = row["market_start_time"]
#         if mst:
#             start_dt = datetime.fromisoformat(mst.replace("Z", "+00:00"))
#             groups[start_dt].append(row["market_id"])

#     # ── Step 3: Build schedule — skip already-passed trigger times ────────────
#     now_utc  = datetime.now(timezone.utc)
#     schedule = []
#     skipped  = 0

#     for start_time in sorted(groups.keys()):
#         trigger_time = start_time - TRIGGER_OFFSET
#         market_ids   = groups[start_time]
#         aest_str     = start_time.astimezone(SYDNEY).strftime("%H:%M")

#         if trigger_time < now_utc - GRACE_PERIOD:
#             print(f"[Dynamic] Skip {aest_str} AEST — trigger already passed ({len(market_ids)} market(s))")
#             skipped += 1
#         else:
#             schedule.append((start_time, trigger_time, market_ids, aest_str))

#     print(f"[Dynamic] Schedule built — {len(schedule)} upcoming, {skipped} skipped")

#     if not schedule:
#         print("[Dynamic] No upcoming races — all trigger times have passed")
#         return

#     # Stream config for betfair_market_book only
#     cfg = STREAMS["betfair_market_book"]

#     # ── Step 4: Sleep → wake → poll until CLOSED ──────────────────────────────
#     for idx, (start_time, trigger_time, market_ids, aest_str) in enumerate(schedule, 1):

#         # Wait until T-5
#         now_utc   = datetime.now(timezone.utc)
#         wait_secs = (trigger_time - now_utc).total_seconds()
#         if wait_secs > 0:
#             print(
#                 f"[{idx}/{len(schedule)}] Waiting {wait_secs / 60:.1f} min "
#                 f"→ race at {aest_str} AEST ({len(market_ids)} market(s))"
#             )
#             time.sleep(wait_secs)

#         # Poll loop — run availableNow stream every 30s until all markets CLOSED
#         poll_start = datetime.now(timezone.utc)
#         poll_num   = 0

#         print(f"[{idx}/{len(schedule)}] Starting poll for {aest_str} AEST — {len(market_ids)} market(s)")

#         while True:
#             try:
#                 df = build_stream(
#                     source_path      = cfg["source_path"],
#                     schema_location  = cfg["schema_location"],
#                     schema_hints     = cfg["schema_hints"],
#                     max_files        = cfg["max_files"],
#                     use_notifications= False,
#                 )

#                 query = (
#                     df.writeStream
#                         .foreachBatch(make_writer(
#                             target_table = cfg["target_table"],
#                             dlo_table    = cfg["dlo_table"],
#                             merge_keys   = cfg["merge_keys"],
#                         ))
#                         .option("checkpointLocation", cfg["checkpoint"])
#                         .trigger(availableNow=True)
#                         .queryName(f"bronze_book_dynamic_{idx}_{poll_num}")
#                         .start()
#                 )
#                 query.awaitTermination()
#                 poll_num += 1

#             except Exception as e:
#                 print(f"[{idx}/{len(schedule)}] Poll {poll_num} error: {e}")

#             # Check how many markets in this group are CLOSED in Delta
#             # marketId is camelCase in the book table (Pydantic preserves raw API name)
#             try:
#                 closed = (
#                     spark.table(cfg["target_table"])
#                         .filter(F.col("marketId").isin(market_ids))
#                         .filter(F.col("status") == "CLOSED")
#                         .select("marketId")
#                         .dropDuplicates()
#                         .count()
#                 )
#             except Exception:
#                 closed = 0   # table may not exist yet on first poll

#             if closed >= len(market_ids):
#                 print(
#                     f"[{idx}/{len(schedule)}] All {len(market_ids)} market(s) CLOSED "
#                     f"after {poll_num} poll(s) — done with {aest_str} AEST"
#                 )
#                 break

#             print(
#                 f"[{idx}/{len(schedule)}] Poll {poll_num} done "
#                 f"— {closed}/{len(market_ids)} CLOSED | next poll in {POLL_INTERVAL}s"
#             )

#             # Safety cap — don't block the next race indefinitely
#             if datetime.now(timezone.utc) - poll_start > timedelta(minutes=MAX_POLL_MINS):
#                 print(
#                     f"[{idx}/{len(schedule)}] Max poll duration ({MAX_POLL_MINS} min) reached "
#                     f"after {poll_num} poll(s) — moving on"
#                 )
#                 break

#             time.sleep(POLL_INTERVAL)

#     print("[Dynamic] Complete — all races processed for today")


# # COMMAND ----------
# # SECTION 7 — ENTRY POINT
# # ==============================================================================
# # MODE controls which function runs:
# #   "all"     → run_all_streams()  : one-shot, processes every feed (scheduled daily)
# #   "dynamic" → run_dynamic()      : race-aware, betfair_market_book only (run race day)
# #
# # In a Databricks Workflow, set MODE via a job parameter widget:
# #   dbutils.widgets.text("MODE", "dynamic")
# #   MODE = dbutils.widgets.get("MODE")
# #
# # When running locally via Databricks Connect, set MODE directly below.
# # ==============================================================================
# try:
#     MODE = dbutils.widgets.get("MODE")
# except Exception:
#     MODE = "dynamic"   # default when no widget is configured

# if MODE == "dynamic":
#     run_dynamic()
# else:
#     run_all_streams()


# # COMMAND ----------
# # SECTION 8 — DLO HEALTH CHECK
# # Run this manually after the job to inspect bad records.
# # Once the schema is corrected, replay from the DLO table by re-inserting rows.
# # ==============================================================================
# def show_dlo_summary():
#     print("=" * 60)
#     print("Dead Letter Office Summary")
#     print("=" * 60)
#     for name, cfg in STREAMS.items():
#         try:
#             count = spark.table(cfg["dlo_table"]).count()
#             status = f"⚠  {count} rescued record(s)" if count > 0 else "✅ clean"
#             print(f"{name:<30} {status}")
#             if count > 0:
#                 spark.table(cfg["dlo_table"]) \
#                     .select("_source_file", "_bronze_loaded_at", "_rescued_data") \
#                     .limit(3) \
#                     .display()
#         except Exception:
#             print(f"{name:<30} — DLO table not created yet (no bad records seen)")

# show_dlo_summary()


# COMMAND ----------
# SECTION 6 — DYNAMIC MODE (RECURSIVE VERSION)
# ==============================================================================
# Revised to handle "rolling" catalogues. After each race group finishes, it 
# re-scans S3 to see if the Python ingestion script has added new evening races.
# ==============================================================================
def run_dynamic(today: str = None):

    SYDNEY          = ZoneInfo("Australia/Sydney")
    TRIGGER_OFFSET  = timedelta(minutes=5)
    GRACE_PERIOD    = timedelta(minutes=20) # allow extra time for late catalogue arrivals
    POLL_INTERVAL   = 10   # seconds between autoloader triggers
    MAX_POLL_MINS   = 15   # safety cap per race group

    if today is None:
        today = datetime.now(SYDNEY).strftime("%Y-%m-%d")

    catalogue_path = f"s3://project-racing-bronze/betfair/market_catalogue/extracted_date={today}/"
    processed_market_ids = set()
    cfg = STREAMS["betfair_market_book"]

    print(f"[Dynamic] Monitoring schedule for {today}...")

    while True:
        # ── Step 1: Refresh S3 View & Build Schedule ──────────────────────────
        # Force Spark to refresh the file listing to see newly uploaded catalogues
        try:
            spark.catalog.refreshByPath(catalogue_path)
            cat_df = (
                spark.read
                    .option("recursiveFileLookup", "true") # Essential to see HH-MM folders
                    .json(catalogue_path)
                    .select("market_id", "market_start_time")
                    .dropDuplicates(["market_id"])
            )
            markets = cat_df.collect()
        except Exception as e:
            print(f"[Dynamic] Waiting for catalogue files to appear in S3... ({e})")
            time.sleep(60)
            continue

        # ── Step 2: Filter and Group ──────────────────────────────────────────
        # Only focus on races that haven't happened and haven't been processed
        now_utc = datetime.now(timezone.utc)
        schedule = []
        
        groups = defaultdict(list)
        for row in markets:
            mst = row["market_start_time"]
            m_id = row["market_id"]
            if mst and m_id not in processed_market_ids:
                start_dt = datetime.fromisoformat(mst.replace("Z", "+00:00"))
                trigger_time = start_dt - TRIGGER_OFFSET
                
                # Only add to schedule if it's in the future
                if trigger_time > now_utc - GRACE_PERIOD:
                    groups[start_dt].append(m_id)

        for start_time in sorted(groups.keys()):
            trigger_time = start_time - TRIGGER_OFFSET
            market_ids   = groups[start_time]
            aest_str     = start_time.astimezone(SYDNEY).strftime("%H:%M")
            schedule.append((start_time, trigger_time, market_ids, aest_str))

        # ── Step 3: Check if we are done for the day ──────────────────────────
        if not schedule:
            print(f"[Dynamic] No upcoming races found. Sleeping 10 mins before next S3 refresh...")
            time.sleep(600)
            continue

        # ── Step 4: Process the Next Immediate Race Group ─────────────────────
        # We process only the FIRST group in the schedule, then loop back to re-scan S3
        start_time, trigger_time, market_ids, aest_str = schedule[0]
        
        wait_secs = (trigger_time - datetime.now(timezone.utc)).total_seconds()
        if wait_secs > 0:
            print(f"[Dynamic] Next race group at {aest_str} AEST. Waiting {wait_secs/60:.1f} min...")
            time.sleep(wait_secs)

        poll_start = datetime.now(timezone.utc)
        poll_num   = 0

        while True:
            try:
                # Trigger Auto Loader to pull the latest snapshots from S3 to Bronze Delta
                df = build_stream(
                    source_path      = cfg["source_path"],
                    schema_location  = cfg["schema_location"],
                    schema_hints     = cfg["schema_hints"],
                    max_files        = cfg["max_files"],
                    use_notifications= False,
                )

                query = (
                    df.writeStream
                        .foreachBatch(make_writer(
                            target_table = cfg["target_table"],
                            dlo_table    = cfg["dlo_table"],
                            merge_keys   = cfg["merge_keys"],
                        ))
                        .option("checkpointLocation", cfg["checkpoint"])
                        .trigger(availableNow=True)
                        .start()
                )
                query.awaitTermination()
                poll_num += 1

                # Check status in the Delta Table
                closed = (spark.table(cfg["target_table"])
                          .filter(F.col("marketId").isin(market_ids))
                          .filter(F.col("status") == "CLOSED")
                          .count())

                if closed >= len(market_ids):
                    print(f"[Dynamic] All markets in {aest_str} group CLOSED.")
                    break

                if datetime.now(timezone.utc) - poll_start > timedelta(minutes=MAX_POLL_MINS):
                    print(f"[Dynamic] Max duration reached for {aest_str}. Moving on.")
                    break

                time.sleep(POLL_INTERVAL)
            except Exception as e:
                print(f"[Dynamic] Error in poll loop: {e}")
                time.sleep(POLL_INTERVAL)

        # Mark as processed so the next catalogue refresh ignores them
        for mid in market_ids:
            processed_market_ids.add(mid)

# COMMAND ----------

# cd /Users/nageswarchowdarypotluri/my-de-project
# source .venv312/bin/activate
# python3 bronze_autoloader.py

# COMMAND ----------




# # General Syntax
# databricks fs cp /path/to/local/file dbfs:/FileStore/scripts/bronze_autoloader.py

# # To overwrite an existing file, add the flag:
# databricks fs cp /path/to/local/file dbfs:/FileStore/scripts/bronze_autoloader.py --overwrite
