"""
betfair_historical_to_s3_bronze.py

Parses Betfair Historical Data BASIC files (bz2 compressed MCM streaming format)
and uploads AU WIN BSP results to S3 Bronze.

What we extract (from the final CLOSED line of each file):
  - market_id, event_id, event_name, venue, market_time (UTC)
  - per runner: selection_id, name, bsp, status (WINNER/LOSER/REMOVED), adjustment_factor

S3 path:
  betfair/historical_bsp/extracted_date=YYYY-MM-DD/YYYY-MM-DD.ndjson

Usage:
  python betfair_historical_to_s3_bronze.py --input-dir /path/to/BASIC
  python betfair_historical_to_s3_bronze.py --input-dir /path/to/BASIC --date 2025-12-20
"""

import bz2
import json
import os
import argparse
import boto3
from collections import defaultdict
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "aws_s3.env"))

BUCKET = "project-racing-bronze"
S3 = boto3.client("s3")


def parse_market_file(filepath):
    """
    Read a bz2 MCM file and extract the final settled state.
    Returns a dict with market + runner BSP/result info, or None if not AU WIN BSP.
    """
    try:
        with bz2.open(filepath, "rt", encoding="utf-8") as f:
            lines = f.read().strip().split("\n")
    except Exception as e:
        return None

    if not lines:
        return None

    # Parse first line to get market definition and filter to AU WIN BSP only
    try:
        first = json.loads(lines[0])
        mc = first.get("mc", [])
        if not mc:
            return None
        md = mc[0].get("marketDefinition", {})
        if (md.get("countryCode") != "AU"
                or md.get("marketType") != "WIN"
                or not md.get("bspMarket")):
            return None
    except Exception:
        return None

    # Find the last line that has a marketDefinition with bspReconciled=True + CLOSED status
    final_md = None
    market_id = mc[0].get("id")

    for line in reversed(lines):
        try:
            d = json.loads(line)
            for market in d.get("mc", []):
                mdef = market.get("marketDefinition", {})
                if (mdef.get("bspReconciled") is True
                        and mdef.get("status") == "CLOSED"):
                    final_md = mdef
                    break
            if final_md:
                break
        except Exception:
            continue

    if not final_md:
        # Market may not have reached CLOSED state — skip
        return None

    # Extract market-level fields
    market_time_str = final_md.get("marketTime", "")
    try:
        market_time_utc = datetime.strptime(market_time_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None

    settled_time_str = final_md.get("settledTime", "")
    settled_time_utc = None
    if settled_time_str:
        try:
            settled_time_utc = datetime.strptime(settled_time_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            pass

    market_date = market_time_utc.strftime("%Y-%m-%d")  # UTC date — close enough for AU

    # Extract runner BSP + result
    runners = []
    for r in final_md.get("runners", []):
        runners.append({
            "selection_id":      r.get("id"),
            "runner_name":       r.get("name"),
            "sort_priority":     r.get("sortPriority"),
            "bsp":               r.get("bsp"),
            "adjustment_factor": r.get("adjustmentFactor"),
            "status":            r.get("status"),   # WINNER / LOSER / REMOVED / HIDDEN
        })

    return {
        "market_id":        market_id,
        "event_id":         final_md.get("eventId"),
        "event_name":       final_md.get("eventName"),
        "venue":            final_md.get("venue", ""),
        "country_code":     final_md.get("countryCode"),
        "market_type":      final_md.get("marketType"),
        "market_time_utc":  market_time_utc.isoformat(),
        "market_date":      market_date,
        "settled_time_utc": settled_time_utc,
        "number_of_winners": final_md.get("numberOfWinners"),
        "runners":          runners,
    }


def upload_records(records, date_str):
    if not records:
        print(f"  No AU WIN BSP records for {date_str} — skipping")
        return
    s3_key = f"betfair/historical_bsp/extracted_date={date_str}/{date_str}.ndjson"
    lines = [json.dumps(r) for r in records]
    body = "\n".join(lines).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=s3_key, Body=body, ContentType="application/json")
    print(f"  Uploaded {len(records)} markets → s3://{BUCKET}/{s3_key}")


def process_directory(input_dir, filter_date=None):
    """
    Walk the BASIC/ directory tree, parse all bz2 files,
    group AU WIN BSP records by market_date, upload per date.
    """
    records_by_date = defaultdict(list)
    total_files = 0
    au_win_found = 0

    for root, dirs, files in os.walk(input_dir):
        for fname in files:
            if not fname.endswith(".bz2"):
                continue
            # Skip event-level summary files (named like "35072547.bz2")
            if not fname.startswith("1."):
                continue

            fpath = os.path.join(root, fname)
            total_files += 1

            if total_files % 1000 == 0:
                print(f"  Processed {total_files} files, {au_win_found} AU WIN BSP found...")

            record = parse_market_file(fpath)
            if record is None:
                continue

            date_str = record["market_date"]

            # Filter to specific date if requested
            if filter_date and date_str != filter_date:
                continue

            records_by_date[date_str].append(record)
            au_win_found += 1

    print(f"\nTotal files scanned: {total_files}")
    print(f"AU WIN BSP markets found: {au_win_found}")
    print(f"Dates covered: {sorted(records_by_date.keys())}\n")

    for date_str in sorted(records_by_date.keys()):
        print(f"\n=== {date_str} ===")
        upload_records(records_by_date[date_str], date_str)

    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True,
                        help="Path to the BASIC/ directory from Betfair Historical download")
    parser.add_argument("--date", default=None,
                        help="Only process this date (YYYY-MM-DD). Omit to process all dates.")
    args = parser.parse_args()

    print(f"Betfair Historical BSP Bronze ingestion")
    print(f"Input dir: {args.input_dir}")
    print(f"Date filter: {args.date or 'all'}\n")

    process_directory(args.input_dir, filter_date=args.date)
