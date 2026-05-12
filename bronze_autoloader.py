# ==============================================================================
# bronze_autoloader.py — FINAL DYNAMIC RECURSIVE VERSION (HIGH-SPEED)
# ==============================================================================
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# SECTION 1 — SPARK CONFIG
try:
    spark
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless(True).getOrCreate()

# SECTION 2 — STREAM REGISTRY (FULL)
# ==============================================================================
BUCKET  = "s3://project-racing-bronze"
CATALOG = "harness_stream.bronze"

STREAMS = {
    "betfair_catalogue": {
        "source_path":     f"{BUCKET}/betfair/market_catalogue/",
        "schema_location": f"{BUCKET}/_schema/betfair_catalogue/",
        "checkpoint":      f"{BUCKET}/_checkpoints/betfair_catalogue/",
        "target_table":    f"{CATALOG}.betfair_catalogue",
        "dlo_table":       f"{CATALOG}.dlo_betfair_catalogue",
        "schema_hints":    "runners STRING, event STRING, eventType STRING",
        "merge_keys":      ["market_id"],
        "max_files":       100, # Increased for catch-up
    },

    "betfair_market_book": {
        "source_path":     f"{BUCKET}/betfair/market_book/",
        "schema_location": f"{BUCKET}/_schema/betfair_market_book/",
        "checkpoint":      f"{BUCKET}/_checkpoints/betfair_market_book/",
        "target_table":    f"{CATALOG}.betfair_market_book",
        "dlo_table":       f"{CATALOG}.dlo_betfair_market_book",
        "schema_hints":    "runners STRING, betDelayModels STRING",
        "merge_keys":      ["marketId", "snapshot_type", "run_time"],
        "max_files":       2000, # Nitro button for catch-up
    }
}

# SECTION 3 — UTILS
def build_stream(source_path, schema_location, schema_hints, max_files):
    base_hints = "extracted_date STRING, race_code STRING, run_time STRING, snapshot_type STRING"
    full_hints = f"{base_hints}, {schema_hints}" if schema_hints else base_hints
    
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaHints", full_hints)
        .option("cloudFiles.maxFilesPerTrigger", max_files) # Apply dynamic limit
        .option("recursiveFileLookup", "true")
        .load(source_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_bronze_loaded_at", F.from_utc_timestamp(F.current_timestamp(), "Australia/Sydney"))
    )

def make_writer(target_table, dlo_table, merge_keys):
    def write_batch(batch_df, batch_id):
        deduped = batch_df.dropDuplicates(merge_keys)
        on_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        try:
            dt = DeltaTable.forName(spark, target_table)
            dt.alias("target").merge(deduped.alias("source"), on_condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        except:
            deduped.write.format("delta").mode("overwrite").partitionBy("extracted_date").saveAsTable(target_table)
    return write_batch

# SECTION 4 — THE ENGINE (RECURSIVE DYNAMIC)
def run_dynamic(today: str = None):
    SYDNEY = ZoneInfo("Australia/Sydney")
    TRIGGER_OFFSET = timedelta(minutes=5)
    GRACE_PERIOD = timedelta(hours=12) 
    LIVE_POLL_INTERVAL = 5
    MAX_POLL_MINS = 15
    
    if today is None:
        today = datetime.now(SYDNEY).strftime("%Y-%m-%d")

    cat_cfg = STREAMS["betfair_catalogue"]
    book_cfg = STREAMS["betfair_market_book"]
    processed_market_ids = set()

    print(f"🚀 [Dynamic] Monitoring for {today}...")

    while True:
        print(f"\n--- {datetime.now(SYDNEY).strftime('%H:%M:%S')} | Incremental Catalogue Refresh ---")
        
        # ── Step 1: Ingest Catalogue ──
        try:
            cat_df = build_stream(cat_cfg["source_path"], cat_cfg["schema_location"], cat_cfg["schema_hints"], cat_cfg["max_files"])
            cat_df.writeStream \
                .foreachBatch(make_writer(cat_cfg["target_table"], cat_cfg["dlo_table"], cat_cfg["merge_keys"])) \
                .option("checkpointLocation", cat_cfg["checkpoint"]) \
                .trigger(availableNow=True) \
                .start() \
                .awaitTermination()
        except Exception as e:
            print(f"⚠️ Catalogue Ingestion Error: {e}")

        # ── Step 2: Build Schedule ──
        try:
            markets = (spark.table(cat_cfg["target_table"])
                       .filter(F.col("extracted_date") == today)
                       .select("market_id", "market_start_time")
                       .dropDuplicates(["market_id"])
                       .collect())
        except Exception as e:
            print(f"⚠️ Table not ready. Sleeping 60s...")
            time.sleep(60); continue

        now_utc = datetime.now(timezone.utc)
        groups = defaultdict(list)
        for row in markets:
            m_id = row["market_id"]
            if m_id not in processed_market_ids:
                start_dt = datetime.fromisoformat(row["market_start_time"].replace("Z", "+00:00"))
                if (start_dt - TRIGGER_OFFSET) > (now_utc - GRACE_PERIOD):
                    groups[start_dt].append(m_id)

        schedule = sorted([(t, t - TRIGGER_OFFSET, ids) for t, ids in groups.items()])

        if not schedule:
            print("💤 All races processed. Waiting 10m for new afternoon/evening files...")
            time.sleep(600); continue

        # ── Step 3: Process Immediate Race Group ──
        start_time, trigger_time, market_ids = schedule[0]
        aest_str = start_time.astimezone(SYDNEY).strftime("%H:%M")
        
        is_catchup = (start_time < datetime.now(timezone.utc))
        wait_secs = (trigger_time - datetime.now(timezone.utc)).total_seconds()
        
        if wait_secs > 0:
            print(f"⏰ Next group: {aest_str} AEST. Waiting {wait_secs/60:.1f} min...")
            time.sleep(wait_secs)

        # Market Book Polling Loop
        print(f"🏁 Polling {aest_str} AEST {'(CATCH-UP)' if is_catchup else '(LIVE)'}")
        poll_start = datetime.now(timezone.utc)
        
        while True:
            try:
                # Use higher file limit for catch-up, lower for live
                batch_limit = book_cfg["max_files"] if is_catchup else 200
                
                df = build_stream(book_cfg["source_path"], book_cfg["schema_location"], book_cfg["schema_hints"], batch_limit)
                df.writeStream \
                    .foreachBatch(make_writer(book_cfg["target_table"], book_cfg["dlo_table"], book_cfg["merge_keys"])) \
                    .option("checkpointLocation", book_cfg["checkpoint"]) \
                    .trigger(availableNow=True) \
                    .start() \
                    .awaitTermination()
                
                # Verify CLOSED status
                closed = spark.table(book_cfg["target_table"]) \
                    .filter(F.col("marketId").isin(market_ids)) \
                    .filter(F.col("status") == "CLOSED") \
                    .count()
                
                if closed >= len(market_ids):
                    print(f"✅ All {len(market_ids)} markets CLOSED for {aest_str}")
                    break
                
                if (datetime.now(timezone.utc) - poll_start > timedelta(minutes=MAX_POLL_MINS)):
                    print(f"⚠️ Max duration reached for {aest_str}. Skipping to next.")
                    break
                
                # Nitro: skip sleep if we are catching up on old data
                if not is_catchup:
                    time.sleep(LIVE_POLL_INTERVAL)
                else:
                    print(f"⏩ Catch-up in progress: {closed}/{len(market_ids)} closed...")
                    
            except Exception as e:
                print(f"❌ Error: {e}"); time.sleep(LIVE_POLL_INTERVAL)

        for mid in market_ids: processed_market_ids.add(mid)

# SECTION 5 — ENTRY POINT
if __name__ == "__main__":
    run_dynamic()