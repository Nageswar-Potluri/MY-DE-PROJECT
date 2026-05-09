import os
import json
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional, List

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

SYDNEY = ZoneInfo("Australia/Sydney")

# -------------------- CONFIG --------------------
load_dotenv(os.path.join(os.path.dirname(__file__), "betfair_credits.env"))

@dataclass(frozen=True)
class BetfairConfig:
    api_key:   str
    login_url: str
    username:  str
    password:  str
    api_url:   str

config = BetfairConfig(
    api_key=os.getenv("BETFAIR_APP_KEY"),
    login_url=os.getenv("BETFAIR_LOGIN_URL"),
    username=os.getenv("BETFAIR_USERNAME"),
    password=os.getenv("BETFAIR_PASSWORD"),
    api_url=os.getenv("BETFAIR_API_URL"),
)

missing = [v for v in ["BETFAIR_APP_KEY", "BETFAIR_LOGIN_URL", "BETFAIR_USERNAME",
                        "BETFAIR_PASSWORD", "BETFAIR_API_URL"] if not os.getenv(v)]
if missing:
    raise ValueError(f"Missing env vars: {missing}")


# -------------------- HELPERS --------------------
def sep(char="─", width=80):
    print(char * width)

def header(title: str):
    sep("═")
    print(f"  {title}")
    sep("═")

def section(title: str):
    sep()
    print(f"  {title}")
    sep()


# -------------------- AUTH --------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def get_token(cfg: BetfairConfig) -> str:
    resp = requests.post(
        cfg.login_url,
        data={"username": cfg.username, "password": cfg.password},
        headers={"X-Application": cfg.api_key, "Accept": "application/json"}
    )
    data = resp.json()
    if data.get("status") != "SUCCESS":
        raise Exception(f"Login failed: {data}")
    print("  ✓ Logged in to Betfair")
    return data["token"]


# -------------------- CATALOGUE --------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def fetch_catalogue(cfg: BetfairConfig, token: str, from_utc: str, to_utc: str) -> List[Dict[str, Any]]:
    headers = {
        "X-Application": cfg.api_key,
        "X-Authentication": token,
        "Content-Type": "application/json",
    }
    payload = {
        "filter": {
            "eventTypeIds": ["7"],
            "marketCountries": ["AU"],
            "marketTypeCodes": ["WIN"],
            "marketStartTime": {"from": from_utc, "to": to_utc}
        },
        "marketProjection": [
            "EVENT", "MARKET_START_TIME", "RUNNER_DESCRIPTION",
            "EVENT_TYPE", "COMPETITION", "MARKET_DESCRIPTION",
        ],
        "maxResults": "200",
        "sort": "FIRST_TO_START"
    }
    resp = requests.post(cfg.api_url + "listMarketCatalogue/", json=payload, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise Exception(f"Catalogue API error {resp.status_code}: {resp.text}")
    return resp.json()


def print_catalogue(markets: List[Dict[str, Any]]):
    header(f"TODAY'S RACE MARKETS  ({len(markets)} markets found)")

    # Group by venue
    by_venue: Dict[str, list] = {}
    for m in markets:
        venue = m.get("event", {}).get("venue") or m.get("event", {}).get("name") or "Unknown"
        by_venue.setdefault(venue, []).append(m)

    for venue, venue_markets in sorted(by_venue.items()):
        section(f"VENUE: {venue}  ({len(venue_markets)} races)")
        for m in venue_markets:
            mid        = m.get("marketId", "?")
            name       = m.get("marketName", "?")
            start_raw  = m.get("marketStartTime", "")
            comp       = m.get("competition", {}).get("name", "—")
            bet_type   = m.get("description", {}).get("bettingType", "—")
            in_play    = m.get("description", {}).get("turnInPlayEnabled", False)
            runners    = m.get("runners", [])

            # Parse start time → AEST
            try:
                start_dt = datetime.fromisoformat(start_raw.replace("Z", "+00:00")).astimezone(SYDNEY)
                start_str = start_dt.strftime("%I:%M %p")
            except Exception:
                start_str = start_raw

            print(f"\n  [{start_str}]  {name}  (ID: {mid})")
            print(f"    Competition : {comp}")
            print(f"    Bet type    : {bet_type}   In-play: {'Yes' if in_play else 'No'}")
            print(f"    Runners     : {len(runners)}")
            for r in runners:
                rname   = r.get("runnerName", "?")
                num     = r.get("metadata", {}).get("CLOTH_NUMBER", r.get("sortPriority", "?"))
                barrier = r.get("metadata", {}).get("STALL_DRAW", "—")
                print(f"      #{num}  {rname}  (barrier {barrier})")
        print()


# -------------------- BOOK --------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def fetch_book(cfg: BetfairConfig, token: str, market_ids: List[str]) -> List[Dict[str, Any]]:
    headers = {
        "X-Application": cfg.api_key,
        "X-Authentication": token,
        "Content-Type": "application/json",
    }
    payload = {
        "marketIds": market_ids,
        "priceProjection": {
            "priceData": ["EX_BEST_OFFERS", "EX_TRADED", "SP_TRADED"],
            "virtualise": False
        }
    }
    resp = requests.post(cfg.api_url + "listMarketBook/", json=payload, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise Exception(f"Book API error {resp.status_code}: {resp.text}")
    return resp.json()


def print_book(books: List[Dict[str, Any]], catalogue_lookup: Dict[str, Dict]):
    header(f"CURRENT ODDS SNAPSHOT  ({len(books)} markets)")

    for book in books:
        mid      = book.get("marketId", "?")
        status   = book.get("status", "?")
        matched  = book.get("totalMatched", 0) or 0
        runners  = book.get("runners", [])

        # Get market name from catalogue lookup
        cat      = catalogue_lookup.get(mid, {})
        mname    = cat.get("marketName", mid)
        venue    = cat.get("event", {}).get("venue") or cat.get("event", {}).get("name") or "?"
        start_raw = cat.get("marketStartTime", "")
        try:
            start_dt = datetime.fromisoformat(start_raw.replace("Z", "+00:00")).astimezone(SYDNEY)
            start_str = start_dt.strftime("%I:%M %p")
        except Exception:
            start_str = "?"

        sep()
        print(f"  {venue} | {start_str} | {mname}  (ID: {mid})")
        print(f"  Status: {status}   Total Matched: ${matched:,.0f}")
        print()

        # Build runner name map from catalogue
        runner_names: Dict[int, str] = {}
        for r in cat.get("runners", []):
            runner_names[r.get("selectionId")] = r.get("runnerName", "?")

        if not runners:
            print("    No runner data available")
            continue

        # Header row
        print(f"    {'#':<3} {'Runner':<25} {'Status':<12} {'Back (best)':<14} {'Lay (best)':<14} {'LTP':<8} {'BSP':<8} {'Traded $'}")
        sep("  ·", 80)

        for r in runners:
            sel_id  = r.get("selectionId")
            rname   = runner_names.get(sel_id, f"ID:{sel_id}")
            rstatus = r.get("status", "?")
            sort    = r.get("sortPriority", "?")
            ltp     = r.get("lastPriceTraded")
            bsp     = r.get("sp", {}).get("actualSP")
            traded  = r.get("totalMatched") or 0

            ex      = r.get("ex", {})
            backs   = ex.get("availableToBack", [])
            lays    = ex.get("availableToLay", [])

            best_back = f"{backs[0]['price']}" if backs else "—"
            best_lay  = f"{lays[0]['price']}"  if lays  else "—"
            ltp_str   = f"{ltp:.2f}"  if ltp  else "—"
            bsp_str   = f"{bsp:.2f}"  if bsp  else "—"

            print(f"    {sort!s:<3} {rname:<25} {rstatus:<12} {best_back:<14} {best_lay:<14} {ltp_str:<8} {bsp_str:<8} ${traded:,.0f}")

        print()


# -------------------- MAIN --------------------
def main():
    parser = argparse.ArgumentParser(description="Betfair terminal viewer — no S3, prints to screen")
    parser.add_argument("--mode", choices=["catalogue", "book", "all"], default="catalogue",
                        help="catalogue=market list | book=odds | all=both")
    parser.add_argument("--date", default=None, metavar="YYYY-MM-DD",
                        help="Date to view (defaults to today AEST)")
    args = parser.parse_args()

    # Resolve target date
    if args.date:
        y, m, d = int(args.date[:4]), int(args.date[5:7]), int(args.date[8:10])
        target = datetime(y, m, d, tzinfo=SYDNEY)
        today  = args.date
    else:
        target = datetime.now(SYDNEY)
        today  = target.strftime("%Y-%m-%d")

    day_start = target.replace(hour=0,  minute=0,  second=0,  microsecond=0).astimezone(timezone.utc)
    day_end   = target.replace(hour=23, minute=59, second=59, microsecond=0).astimezone(timezone.utc)
    from_utc  = day_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_utc    = day_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    print()
    header(f"BETFAIR TERMINAL VIEWER  —  {today} AEST")
    print(f"  Mode: {args.mode}   UTC window: {from_utc} → {to_utc}")

    token = get_token(config)

    if args.mode in ("catalogue", "all"):
        print(f"\n  Fetching market catalogue...")
        markets = fetch_catalogue(config, token, from_utc, to_utc)
        print_catalogue(markets)

    if args.mode in ("book", "all"):
        if args.mode == "all":
            market_ids = [m["marketId"] for m in markets]
        else:
            # book-only: fetch catalogue silently just to get IDs + names
            print(f"\n  Fetching market IDs...")
            markets = fetch_catalogue(config, token, from_utc, to_utc)
            market_ids = [m["marketId"] for m in markets]

        catalogue_lookup = {m["marketId"]: m for m in markets}

        print(f"\n  Fetching odds for {len(market_ids)} markets (batches of 8)...")
        all_books = []
        for i in range(0, len(market_ids), 8):
            batch = market_ids[i:i + 8]
            print(f"    Batch {i // 8 + 1}: markets {i + 1}–{i + len(batch)}")
            all_books.extend(fetch_book(config, token, batch))

        print_book(all_books, catalogue_lookup)

    sep("═")
    print(f"  Done — {datetime.now(SYDNEY).strftime('%I:%M:%S %p AEST')}")
    sep("═")


if __name__ == "__main__":
    main()
