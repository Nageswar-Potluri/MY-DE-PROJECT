# ==============================================================================
# bronze_autoloader.py
# Auto Loader: FormFav & Betfair → S3 Bronze → Delta Tables (harness_stream.bronze)
#
# Schedule : Daily after 12:00 AM AEST via Databricks Workflow
# Trigger  : availableNow=True  — processes all new files, then shuts down
# Catalog  : harness_stream.bronze
# ==============================================================================


# COMMAND ----------
# SECTION 1 — IMPORTS & SPARK CONFIG
# ==============================================================================
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
        "merge_keys":      ["marketId"],
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
        # snapshot_type derived from S3 path, not in the JSON payload
        "merge_keys":      ["marketId", "snapshot_type"],
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
def build_stream(source_path, schema_location, schema_hints, max_files):

    # Base schema hints always applied — prevents partition folder names from
    # being mis-inferred as dates or integers
    base_hints = "extracted_date STRING, race_code STRING, run_time STRING, snapshot_type STRING"
    full_hints = f"{base_hints}, {schema_hints}" if schema_hints else base_hints

    df = (
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
            .option("cloudFiles.useNotifications",    "false")
            .option("pathGlobFilter",                 "*.json")
            .option("cloudFiles.includeExistingFiles","true")
            # Throttle: process in chunks so the cluster doesn't OOM on large backfills
            .option("maxFilesPerTrigger",             str(max_files))
            # Reads all files recursively regardless of folder depth or structure.
            # Required here because old paths (race_code=.../extracted_date=...)
            # and new paths (extracted_date=.../race_code=...) have different
            # folder orderings and would cause a partition conflict with normal reads.
            .option("recursiveFileLookup",            "true")
            .load(source_path)

            # Audit columns
            # _metadata.file_path is the Unity Catalog equivalent of input_file_name()
            .withColumn("_source_file",      F.col("_metadata.file_path"))
            .withColumn("_bronze_loaded_at", F.current_timestamp())

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
# SECTION 5 — MAIN RUNNER
# ==============================================================================
def run_all_streams():
    queries = []

    for name, cfg in STREAMS.items():
        print(f"[Auto Loader] Starting → {name}")

        df = build_stream(
            source_path    = cfg["source_path"],
            schema_location= cfg["schema_location"],
            schema_hints   = cfg["schema_hints"],
            max_files      = cfg["max_files"],
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


run_all_streams()


# COMMAND ----------
# SECTION 6 — DLO HEALTH CHECK
# Run this manually after the job to inspect bad records.
# Once the schema is corrected, replay from the DLO table by re-inserting rows.
# ==============================================================================
def show_dlo_summary():
    print("=" * 60)
    print("Dead Letter Office Summary")
    print("=" * 60)
    for name, cfg in STREAMS.items():
        try:
            count = spark.table(cfg["dlo_table"]).count()
            status = f"⚠  {count} rescued record(s)" if count > 0 else "✅ clean"
            print(f"{name:<30} {status}")
            if count > 0:
                spark.table(cfg["dlo_table"]) \
                    .select("_source_file", "_bronze_loaded_at", "_rescued_data") \
                    .limit(3) \
                    .display()
        except Exception:
            print(f"{name:<30} — DLO table not created yet (no bad records seen)")

show_dlo_summary()

# COMMAND ----------
