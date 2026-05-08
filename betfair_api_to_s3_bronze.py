# =============================================================================
# betfair_api_to_s3_bronze.py
#
# Extracts Betfair AU horse racing data and uploads raw JSON to S3 Bronze.
#
# CHANGE LOG
# ----------
# 2026-04-08 — Added --mode argument to split catalogue and book into separate
#              scheduled runs:
#
#   --mode catalogue  → Morning run (7:00 AM AEST)
#                       Fetches AU WIN market catalogue for the day.
#                       S3: betfair/market_catalogue/extracted_date=YYYY-MM-DD/
#
#   --mode book       → Evening run (8:30 PM AEST)
#                       Fetches market book (odds + BSP) using market IDs
#                       already stored in S3 from the morning catalogue run.
#                       S3: betfair/market_book/extracted_date=YYYY-MM-DD/
#
#   --mode all        → Run both steps in sequence (manual/testing use).
#                       This was the original behaviour before the split.
#
# CRONTAB (adjust UTC offset: AEST=UTC+10, AEDT=UTC+11 Oct–Apr)
# --------------------------------------------------------------
#   # Morning  — catalogue only — 7:00 AM AEST (9:00 PM UTC previous day)
#   0 21 * * * /path/to/.venv/bin/python betfair_api_to_s3_bronze.py --mode catalogue
#
#   # Evening  — market book  — 8:30 PM AEST (10:30 AM UTC)
#   30 10 * * * /path/to/.venv/bin/python betfair_api_to_s3_bronze.py --mode book
# =============================================================================

        import requests
        import boto3
        import json
        import argparse
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo
        from dotenv import load_dotenv
        import os

        load_dotenv(os.path.join(os.path.dirname(__file__), "betfair_credits.env"))
        load_dotenv(os.path.join(os.path.dirname(__file__), "aws_s3.env"))

        BETFAIR_USERNAME = os.getenv("BETFAIR_USERNAME")
        BETFAIR_PASSWORD = os.getenv("BETFAIR_PASSWORD")
        BETFAIR_APP_KEY  = os.getenv("BETFAIR_APP_KEY")


        BUCKET    = "project-racing-bronze"
        S3        = boto3.client(
            "s3",
            aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name           = os.getenv("AWS_REGION", "ap-southeast-2")
        )
        LOGIN_URL = "https://identitysso.betfair.com/api/login"
        API_URL   = "https://api.betfair.com/exchange/betting/rest/v1.0/"


        # --- AUTH ---
        def get_session_token():
            resp = requests.post(
                LOGIN_URL,
                data={"username": BETFAIR_USERNAME, "password": BETFAIR_PASSWORD},
                headers={"X-Application": BETFAIR_APP_KEY, "Accept": "application/json"}
            )
            body = resp.json()
            if body.get("status") != "SUCCESS":
                raise Exception(f"Betfair login failed: {body}")
            return body["token"]


        # --- API HELPER ---
        def betfair_post(session_token, endpoint, payload):
            headers = {
                "X-Application":  BETFAIR_APP_KEY,
                "X-Authentication": session_token,
                "Content-Type":   "application/json"
            }
            for attempt in range(3):
                try:
                    resp = requests.post(API_URL + endpoint, json=payload, headers=headers, timeout=30)
                    if not resp.ok:
                        raise Exception(f"Betfair API error {resp.status_code}: {resp.text}")
                    return resp.json()
                except requests.exceptions.ReadTimeout:
                    if attempt < 2:
                        wait = 5 * (attempt + 1)
                        print(f"  Timeout on {endpoint}, retrying in {wait}s...")
                        time.sleep(wait)
                    else:
                        raise Exception(f"Betfair API timed out on {endpoint} after 3 attempts")


        # --- STEP 1: LIST TODAY'S AU WIN MARKETS ---
        def list_markets(session_token, from_utc, to_utc):
            payload = {
                "filter": {
                    "eventTypeIds":   ["7"],       # 7 = Horse Racing
                    "marketCountries": ["AU"],
                    "marketTypeCodes": ["WIN"],
                    "marketStartTime": {
                        "from": from_utc,
                        "to":   to_utc
                    }
                },
                "marketProjection": [
                    "EVENT",
                    "MARKET_START_TIME",
                    "RUNNER_DESCRIPTION",
                    "EVENT_TYPE"
                ],
                "maxResults": "200",
                "sort": "FIRST_TO_START"
            }
            return betfair_post(session_token, "listMarketCatalogue/", payload)


        # --- STEP 2: GET ODDS + BSP ---
        def get_market_book(session_token, market_ids):
            payload = {
                "marketIds": market_ids,
                "priceProjection": {
                    "priceData": ["EX_BEST_OFFERS", "SP_TRADED"],
                    "virtualise": False
                }
            }
            return betfair_post(session_token, "listMarketBook/", payload)


        # --- S3 UPLOAD ---
        def upload_json(data, s3_key):
            # Write newline-delimited JSON (NDJSON) — one record per line for Auto Loader compatibility
            lines = [json.dumps(record) for record in data]
            body = "\n".join(lines).encode("utf-8")
            S3.put_object(Bucket=BUCKET, Key=s3_key, Body=body, ContentType="application/json")
            print(f"  Uploaded → s3://{BUCKET}/{s3_key}")


        # --- ARGUMENT PARSING ---
        # --mode catalogue : morning run — fetch and upload market catalogue only
        # --mode book      : evening run — read market IDs from S3 catalogue, fetch market book only
        # --mode all       : run both steps in sequence (original behaviour, useful for testing)
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--mode",
            choices=["catalogue", "book", "all"],
            default="all",
            help="catalogue=morning run, book=evening run, all=both (default)"
        )
        args = parser.parse_args()

        # --- MAIN ---
        SYDNEY = ZoneInfo("Australia/Sydney")   # handles AEST/AEDT automatically
        now_aest = datetime.now(SYDNEY)
        today    = now_aest.strftime("%Y-%m-%d")   # AEST date — used for S3 partition

        # Convert full AEST day to UTC for the Betfair filter
        day_start_utc = now_aest.replace(hour=0,  minute=0,  second=0,  microsecond=0).astimezone(timezone.utc)
        day_end_utc   = now_aest.replace(hour=23, minute=59, second=59, microsecond=0).astimezone(timezone.utc)
        from_utc = day_start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_utc   = day_end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        print(f"Mode: {args.mode}")
        print("Logging in to Betfair...")
        session_token = get_session_token()
        print("  Login successful")

        CATALOGUE_KEY = f"betfair/market_catalogue/extracted_date={today}/market_catalogue.json"
        BOOK_KEY      = f"betfair/market_book/extracted_date={today}/market_book.json"


        def run_catalogue():
            """Morning run — fetch AU WIN market catalogue and upload to S3."""
            print(f"\nFetching AU horse racing WIN markets for {today} AEST ({from_utc} to {to_utc} UTC)...")
            markets = list_markets(session_token, from_utc, to_utc)
            print(f"  {len(markets)} markets found")

            if not markets:
                print("No markets found. Exiting.")
                return

            upload_json(markets, CATALOGUE_KEY)


        def run_book():
            """Evening run — read market IDs from today's S3 catalogue, fetch market book (odds + BSP)."""
            # Read today's catalogue from S3 to get market IDs — avoids a second API call
            print(f"\nReading today's market catalogue from S3: {CATALOGUE_KEY}")
            response = S3.get_object(Bucket=BUCKET, Key=CATALOGUE_KEY)
            lines = response["Body"].read().decode("utf-8").strip().splitlines()
            markets = [json.loads(line) for line in lines]
            market_ids = [m["marketId"] for m in markets]
            print(f"  {len(market_ids)} market IDs loaded")

            if not market_ids:
                print("No market IDs found in catalogue. Exiting.")
                return

            # Fetch market book in batches of 10 (kept small to avoid TOO_MUCH_DATA error)
            all_books  = []
            batch_size = 10

            print(f"\nFetching market book ({len(market_ids)} markets, batches of {batch_size})...")
            for i in range(0, len(market_ids), batch_size):
                batch = market_ids[i:i + batch_size]
                print(f"  Batch {i // batch_size + 1}: markets {i + 1}–{i + len(batch)}")
                books = get_market_book(session_token, batch)
                all_books.extend(books)

            upload_json(all_books, BOOK_KEY)


        # --- RUN BASED ON MODE ---
        if args.mode == "catalogue":
            run_catalogue()
        elif args.mode == "book":
            run_book()
        elif args.mode == "all":
            # Original behaviour — run catalogue first, then book in the same session
            run_catalogue()
            run_book()

        print("\nDone.")
