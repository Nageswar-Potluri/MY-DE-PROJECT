import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ── SECTION 1: SPARK ──────────────────────────────────────────────────────────
try:
    spark
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless(True).getOrCreate()

# ── SECTION 2: REGISTRY ───────────────────────────────────────────────────────
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
        "max_files":       500,
    },
    "betfair_market_book": {
        "source_path":     f"{BUCKET}/betfair/market_book/",
        "schema_location": f"{BUCKET}/_schema/betfair_market_book/",
        "checkpoint":      f"{BUCKET}/_checkpoints/betfair_market_book/",
        "target_table":    f"{CATALOG}.betfair_market_book",
        "dlo_table":       f"{CATALOG}.dlo_betfair_market_book",
        "schema_hints":    "runners STRING, betDelayModels STRING, api_latency_ms BIGINT, ingested_at STRING",
        "merge_keys":      ["marketId", "snapshot_type", "run_time"],
        "max_files":       5000,
    },
}

# ── SECTION 3: STREAM HELPERS ─────────────────────────────────────────────────
def _base_reader(cfg):
    base_hints = "extracted_date STRING, race_code STRING, run_time STRING, snapshot_type STRING"
    full_hints = f"{base_hints}, {cfg['schema_hints']}" if cfg.get("schema_hints") else base_hints
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", cfg["schema_location"])
        .option("cloudFiles.schemaHints", full_hints)
        .option("cloudFiles.maxFilesPerTrigger", cfg["max_files"])
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("recursiveFileLookup", "true")
        .load(cfg["source_path"])
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_bronze_loaded_at", F.from_utc_timestamp(F.current_timestamp(), "Australia/Sydney"))
    )

def _make_writer(cfg):
    merge_keys   = cfg["merge_keys"]
    target_table = cfg["target_table"]

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        deduped   = batch_df.dropDuplicates(merge_keys)
        condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
        try:
            DeltaTable.forName(spark, target_table) \
                .alias("t").merge(deduped.alias("s"), condition) \
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        except Exception:
            deduped.write.format("delta").mode("overwrite") \
                .partitionBy("extracted_date").saveAsTable(target_table)

    return write_batch

def run_one_shot(cfg):
    """Load all pending files then stop. Used for catalog and book polling."""
    (
        _base_reader(cfg)
        .writeStream
        .foreachBatch(_make_writer(cfg))
        .option("checkpointLocation", cfg["checkpoint"])
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )

# ── SECTION 4: SCHEDULE BUILDER ───────────────────────────────────────────────
def build_schedule(cat_cfg, today, processed_ids, grace_period):
    """
    Read catalog JSON directly from S3 — bypasses Delta for low-latency schedule.
    Returns (schedule, latest_race_start_utc).
    schedule = sorted list of (start_dt, trigger_dt, [market_ids])
    """
    SYDNEY         = ZoneInfo("Australia/Sydney")
    TRIGGER_OFFSET = timedelta(minutes=5)

    try:
        rows = (
            spark.read
            .format("json")
            .option("recursiveFileLookup", "true")
            .load(cat_cfg["source_path"])
            .filter(F.col("extracted_date") == today)
            .select("market_id", "market_start_time")
            .dropDuplicates(["market_id"])
            .collect()
        )
    except Exception as e:
        print(f"⚠️  S3 catalog read failed: {e}")
        return [], None

    now_utc      = datetime.now(timezone.utc)
    groups       = defaultdict(list)
    latest_start = None

    for row in rows:
        m_id     = row["market_id"]
        start_dt = datetime.fromisoformat(row["market_start_time"].replace("Z", "+00:00"))

        if latest_start is None or start_dt > latest_start:
            latest_start = start_dt

        if m_id not in processed_ids:
            trigger_dt = start_dt - TRIGGER_OFFSET
            if trigger_dt > now_utc - grace_period:
                groups[start_dt].append(m_id)

    schedule      = sorted([(t, t - TRIGGER_OFFSET, ids) for t, ids in groups.items()])
    total_markets = sum(len(ids) for _, _, ids in schedule)
    last_str      = latest_start.astimezone(SYDNEY).strftime("%H:%M") if latest_start else "none"
    print(f"📋 Schedule: {len(schedule)} groups | {total_markets} markets | last race {last_str} AEST")
    return schedule, latest_start

# ── SECTION 5: ENGINE ─────────────────────────────────────────────────────────
def run_dynamic(today: str = None):
    SYDNEY = ZoneInfo("Australia/Sydney")

    GRACE_PERIOD            = timedelta(hours=12)
    DAY_END_BUFFER          = timedelta(hours=1)   # suspend this long after last race ends
    SHORT_BREAK             = timedelta(minutes=15) # skip catalog refresh for gaps shorter than this
    CAT_REFRESH_BEFORE      = timedelta(minutes=10) # wake up this early before next race to refresh catalog
    BOOK_LOAD_INTERVAL_SECS = 10                    # how often to run_one_shot during active race
    CLOSED_CHECK_EVERY_N    = 3                     # check CLOSED status every N loads (~30s)
    MAX_POLL                = timedelta(minutes=20)
    EMPTY_WAIT_SECS         = 300                   # 5 min retry when no races found yet

    if today is None:
        today = datetime.now(SYDNEY).strftime("%Y-%m-%d")

    cat_cfg           = STREAMS["betfair_catalogue"]
    book_cfg          = STREAMS["betfair_market_book"]
    processed_ids     = set()
    schedule          = []
    latest_race_start = None

    print(f"🚀 AutoLoader starting — {today}")

    def catalog_refresh():
        nonlocal schedule, latest_race_start
        print("🔄 Catalog refresh — loading new files to Delta + rebuilding schedule...")
        try:
            run_one_shot(cat_cfg)
        except Exception as e:
            print(f"⚠️  Catalog stream error: {e}")
        schedule, latest_race_start = build_schedule(cat_cfg, today, processed_ids, GRACE_PERIOD)

    # ── Initial load ──────────────────────────────────────────────────────────
    catalog_refresh()

    # ── Main loop ─────────────────────────────────────────────────────────────
    try:
        while True:
            now_utc = datetime.now(timezone.utc)
            now_syd = datetime.now(SYDNEY)

            # ── Day-end check ─────────────────────────────────────────────────
            if not schedule:
                if latest_race_start and now_utc > latest_race_start + DAY_END_BUFFER:
                    last_str = latest_race_start.astimezone(SYDNEY).strftime("%H:%M")
                    print(f"🏁 Day complete — last race was {last_str} AEST. Suspending.")
                    break
                if not latest_race_start and now_syd.hour >= 21:
                    print("🏁 No catalog data and past 9PM AEST. Suspending.")
                    break
                print(f"📭 No upcoming races. Rechecking in {EMPTY_WAIT_SECS // 60} min...")
                time.sleep(EMPTY_WAIT_SECS)
                catalog_refresh()
                continue

            # ── Next race group ───────────────────────────────────────────────
            start_time, trigger_time, market_ids = schedule[0]
            aest_str   = start_time.astimezone(SYDNEY).strftime("%H:%M")
            wait_secs  = (trigger_time - now_utc).total_seconds()
            is_catchup = start_time < now_utc

            # ── Long break: sleep then refresh catalog near race time ─────────
            if wait_secs > SHORT_BREAK.total_seconds():
                sleep_target = trigger_time - CAT_REFRESH_BEFORE
                sleep_secs   = (sleep_target - now_utc).total_seconds()
                if sleep_secs > 0:
                    print(f"⏸️  Next race {aest_str} AEST in {wait_secs / 60:.0f} min. "
                          f"Sleeping {sleep_secs / 60:.0f} min then refreshing catalog...")
                    time.sleep(sleep_secs)
                catalog_refresh()
                continue  # re-enter with updated schedule

            # ── Short wait or immediate catch-up ──────────────────────────────
            if wait_secs > 0:
                print(f"⏰ {wait_secs:.0f}s until T-5 for {aest_str} AEST...")
                time.sleep(wait_secs)

            print(f"🏁 Monitoring {aest_str} AEST {'(CATCH-UP)' if is_catchup else '(LIVE)'} "
                  f"— {len(market_ids)} markets")

            # ── Load + poll loop (availableNow=True, serverless-compatible) ───
            poll_start = datetime.now(timezone.utc)
            load_count = 0
            while True:
                try:
                    run_one_shot(book_cfg)
                    load_count += 1
                except Exception as e:
                    print(f"❌ Book load error: {e}")

                if is_catchup:
                    print(f"✅ {aest_str} AEST catch-up complete.")
                    break

                if load_count % CLOSED_CHECK_EVERY_N == 0:
                    try:
                        elapsed = datetime.now(timezone.utc) - poll_start
                        closed  = (
                            spark.table(book_cfg["target_table"])
                            .filter(F.col("marketId").isin(market_ids))
                            .filter(F.col("status") == "CLOSED")
                            .count()
                        )
                        if closed >= len(market_ids) or elapsed > MAX_POLL:
                            reason = "all CLOSED" if closed >= len(market_ids) else "timeout"
                            print(f"✅ {aest_str} AEST done ({reason}).")
                            break
                        print(f"   {closed}/{len(market_ids)} CLOSED...")
                    except Exception as e:
                        print(f"❌ Poll error: {e}")

                time.sleep(BOOK_LOAD_INTERVAL_SECS)

            # ── Mark processed, advance schedule ─────────────────────────────
            for mid in market_ids:
                processed_ids.add(mid)
            schedule = schedule[1:]

            # ── Post-race catalog refresh (only on long next break) ───────────
            if schedule:
                next_break_secs = (schedule[0][1] - datetime.now(timezone.utc)).total_seconds()
                if next_break_secs > SHORT_BREAK.total_seconds():
                    catalog_refresh()
            else:
                catalog_refresh()

    finally:
        print("👋 AutoLoader shut down cleanly.")


if __name__ == "__main__":
    run_dynamic()
