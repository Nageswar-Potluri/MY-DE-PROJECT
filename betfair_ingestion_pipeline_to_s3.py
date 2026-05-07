import os
import json
import logging
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Iterator, Dict, Any, Optional, List

import requests
import boto3
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential

# -------------------- LOGGING --------------------
# Sets up structured logging for the entire script.
# Every log message will show: timestamp - level - message
# Replaces print() — gives us INFO, WARNING, ERROR levels
# so we can filter logs in production (CloudWatch, terminal, cron logs)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# -------------------- CONFIG --------------------
# Frozen dataclass acts as the single source of truth for all configuration.
# frozen=True means no field can be changed after creation — prevents
# accidental overwrites mid-pipeline (e.g. token refresh changing api_key).
# All credentials come from .env files — never hardcoded in source code.
# sns_topic_arn is optional — when set, pipeline sends failure alerts via AWS SNS.
@dataclass(frozen=True)
class BetfairConfig:
    api_key: str
    login_url: str
    username: str
    password: str
    api_url: str
    s3_bucket: str
    aws_access_key: str
    aws_secret_key: str
    region: str = "ap-southeast-2"
    sns_topic_arn: Optional[str] = None


# Load credentials from two separate .env files:
# betfair_credits.env  → Betfair API username, password, app key
# aws_s3.env           → AWS access key, secret key
# os.path.dirname(__file__) ensures the .env path is relative to this script,
# not wherever the terminal is when we run it
load_dotenv(os.path.join(os.path.dirname(__file__), "betfair_credits.env"))
load_dotenv(os.path.join(os.path.dirname(__file__), "aws_s3.env"))

# Build the config object by reading environment variables loaded above.
# If any required variable is missing, os.getenv() returns None silently here —
# the config validation block below catches this before the pipeline starts.
config = BetfairConfig(
    api_key=os.getenv("BETFAIR_APP_KEY"),
    login_url=os.getenv("BETFAIR_LOGIN_URL"),
    username=os.getenv("BETFAIR_USERNAME"),
    password=os.getenv("BETFAIR_PASSWORD"),
    api_url=os.getenv("BETFAIR_API_URL"),
    s3_bucket="project-racing-bronze",
    aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    sns_topic_arn=os.getenv("SNS_TOPIC_ARN"),
)

# -------------------- CONFIG VALIDATION --------------------
# Fail immediately with a clear message if any required env variable is missing.
# Without this, the script would start, connect to Betfair, then crash mid-run
# with a confusing error like "NoneType has no attribute strip".
# Production standard: validate all inputs at startup, not mid-execution.
required_vars = [
    "BETFAIR_APP_KEY",
    "BETFAIR_LOGIN_URL",
    "BETFAIR_USERNAME",
    "BETFAIR_PASSWORD",
    "BETFAIR_API_URL",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
]
missing = [var for var in required_vars if not os.getenv(var)]
if missing:
    raise ValueError(f"Missing required environment variables: {missing}")


# -------------------- PYDANTIC SCHEMAS --------------------
# MarketCatalogue validates the morning catalogue API response.
# Each record represents one race market (e.g. "R1 1400m Mdn at Randwick").
# ADDED: competition_id, competition_name from COMPETITION projection
# ADDED: betting_type, each_way_divisor, turn_in_play, rules from MARKET_DESCRIPTION projection
# market_id is the key that links catalogue → book → runners downstream.
# extra="ignore" silently drops any fields Betfair adds that we don't need,
# so new API fields never break validation.
class MarketCatalogue(BaseModel):
    model_config = {"extra": "ignore"}

    # market level
    market_id: Optional[str] = None
    market_name: Optional[str] = None
    market_start_time: Optional[str] = None
    total_matched: Optional[float] = None
    runners: Optional[List[Dict[str, Any]]] = None  # nested list of runners, flattened in Silver

    # event type level
    event_type_id: Optional[str] = None
    event_type_name: Optional[str] = None

    # event level
    event_id: Optional[str] = None
    event_name: Optional[str] = None
    event_country: Optional[str] = None
    event_timezone: Optional[str] = None
    event_venue: Optional[str] = None
    event_open_date: Optional[str] = None

    # ADDED: competition level — identifies race series / class (e.g. Group 1, Benchmark 78)
    competition_id: Optional[str] = None
    competition_name: Optional[str] = None

    # ADDED: market description — betting rules and structure from MARKET_DESCRIPTION projection
    betting_type: Optional[str] = None         # WIN, PLACE, EACH_WAY etc.
    each_way_divisor: Optional[float] = None   # place payout divisor for each-way markets
    turn_in_play: Optional[bool] = None        # True if market goes in-play during race
    rules: Optional[str] = None               # full market rules text


# MarketBook validates the book API response (odds + BSP).
# Each record represents the current state of one market's order book.
# ADDED: isMarketDataDelayed, betDelay, crossMatchingEnabled from API docs review
# runners contains nested back/lay prices and BSP data —
# kept as raw nested list here (Bronze layer) for flattening later in Databricks.
# status tells us if the market is OPEN, SUSPENDED, or CLOSED.
class MarketBook(BaseModel):
    model_config = {"extra": "ignore"}

    marketId: Optional[str] = None
    status: Optional[str] = None            # OPEN / SUSPENDED / CLOSED
    totalMatched: Optional[float] = None    # total money matched in this market
    totalAvailable: Optional[float] = None  # money still available to be matched
    runners: Optional[List[Dict[str, Any]]] = None  # nested: back/lay/BSP per horse

    # ADDED: market-level metadata fields discovered in API docs review
    isMarketDataDelayed: Optional[bool] = None  # True if non-premium subscription (3s delay)
    betDelay: Optional[int] = None              # seconds of bet delay when market goes in-play
    crossMatchingEnabled: Optional[bool] = None # True if cross-matching with other exchanges
    runnersVoidable: Optional[bool] = None      # True if runners can be voided (scratchings)
    version: Optional[int] = None              # market version number — increments on changes
    complete: Optional[bool] = None            # True if market data is complete


# -------------------- PIPELINE --------------------
# BetfairPipeline encapsulates the entire extract → validate → upload workflow.
# Using a class (not bare functions) lets us share state across methods:
# - self.token: session token used by all API calls
# - self.s3: single reusable S3 client
# - self.sns: single reusable SNS client (if alerting is configured)
# This avoids passing credentials and clients as arguments to every function.
class BetfairPipeline:

    # Class-level constant — shared across all instances, not duplicated per method
    SYDNEY = ZoneInfo("Australia/Sydney")

    def __init__(self, config: BetfairConfig):
        self.config = config

        # Single S3 client shared across all upload and list operations.
        # Creating it once here avoids re-authenticating with AWS on every batch upload.
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key,
            aws_secret_access_key=config.aws_secret_key,
            region_name=config.region
        )

        # SNS client only created if an SNS topic ARN is configured.
        # If sns_topic_arn is None (no alerting), self.sns is None and
        # _send_alert() will skip silently — no crash.
        self.sns = (
            boto3.client(
                "sns",
                aws_access_key_id=config.aws_access_key,
                aws_secret_access_key=config.aws_secret_key,
                region_name=config.region
            )
            if config.sns_topic_arn else None
        )

        # Log in to Betfair immediately on startup and store the session token.
        # All subsequent API calls (catalogue, book) use this token in their headers.
        self.token = self._get_session_token()

    # -------------------- AUTH --------------------
    # Logs in to Betfair and returns a session token (valid for ~8 hours).
    # @retry retries up to 3 times with exponential backoff (2s → 4s → 8s)
    # if the login fails due to network issues.
    # Raises Exception if status != SUCCESS so tenacity knows to retry.
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=2, max=10),
        before_sleep=lambda rs: logger.warning(
            f"Login failed — retrying... attempt {rs.attempt_number}"
        )
    )
    def _get_session_token(self) -> str:
        response = requests.post(
            self.config.login_url,
            data={"username": self.config.username, "password": self.config.password},
            headers={"X-Application": self.config.api_key, "Accept": "application/json"}
        )
        data = response.json()
        if data.get("status") != "SUCCESS":
            raise Exception(f"Login failed: {data}")
        logger.info("Betfair login successful")
        return data["token"]

    # -------------------- TOKEN REFRESH --------------------
    # Betfair session tokens expire after ~8 hours.
    # If any API call returns 400/401/403, it likely means the token has expired.
    # This method refreshes the token and raises an exception so tenacity
    # retries the original API call with the new token.
    # Called inside every API method before raising the final error.
    def _handle_session_error(self, response: requests.Response):
        if response.status_code in {400, 401, 403}:
            logger.warning(f"Session error ({response.status_code}) — refreshing token and retrying")
            self.token = self._get_session_token()
            raise Exception(f"Token refreshed after {response.status_code} — will retry")

    # -------------------- IDEMPOTENCY --------------------
    # Checks whether any files already exist under a given S3 prefix.
    # Used before both catalogue and book runs to prevent duplicate uploads
    # when the same date is accidentally re-run (e.g. cron fires twice).
    # Returns True if files exist (skip the run), False if safe to proceed.
    # Paginator handles prefixes with more than 1000 files (S3 list limit).
    def _s3_prefix_has_files(self, prefix: str) -> bool:
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=prefix):
            if page.get("Contents"):
                return True
        return False

    # -------------------- ALERTING --------------------
    # Publishes a failure notification to AWS SNS when the pipeline crashes.
    # SNS then forwards the alert to whatever is subscribed — email, Slack, PagerDuty.
    # Silently skips if no SNS topic is configured (sns_topic_arn = None).
    # Wrapped in try/except so an alert failure never masks the original error.
    def _send_alert(self, subject: str, message: str):
        if not self.sns or not self.config.sns_topic_arn:
            logger.info("Alerting not configured — skipping SNS")
            return
        try:
            self.sns.publish(
                TopicArn=self.config.sns_topic_arn,
                Subject=subject,
                Message=message
            )
            logger.info("Alert sent via SNS")
        except Exception as e:
            logger.error(f"Failed to send SNS alert: {e}")

    # -------------------- CATALOGUE API --------------------
    # Calls Betfair's listMarketCatalogue endpoint and returns a list of AU WIN markets.
    # This is a plain method (not a generator) so @retry works correctly —
    # tenacity can only retry a function that raises, not one that yields.
    # The UTC time window (from_utc → to_utc) covers the full AEST race day.
    #
    # ADDED projections vs previous version:
    #   COMPETITION     → competition name and ID (race series / class)
    #   MARKET_DESCRIPTION → betting type, each-way divisor, turn-in-play flag, rules
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=2, max=10),
        before_sleep=lambda rs: logger.warning(
            f"Catalogue API failed — retrying... attempt {rs.attempt_number}"
        )
    )
    def _call_market_catalogue_api(self, from_utc: str, to_utc: str) -> List[Dict[str, Any]]:
        headers = {
            "X-Application": self.config.api_key,
            "X-Authentication": self.token,
            "Content-Type": "application/json",
        }
        payload = {
            "filter": {
                "eventTypeIds": ["7"],          # 7 = Horse Racing
                "marketCountries": ["AU"],       # Australian markets only
                "marketTypeCodes": ["WIN"],      # WIN markets only (not PLACE, EACH_WAY etc.)
                "marketStartTime": {"from": from_utc, "to": to_utc}
            },
            "marketProjection": [
                "EVENT",                # event name, id, country, timezone, venue, openDate
                "MARKET_START_TIME",    # scheduled race start time
                "RUNNER_DESCRIPTION",   # runner names, selection IDs, sort priority
                "EVENT_TYPE",           # event type name and id (Horse Racing)
                "COMPETITION",          # ADDED: competition name/id — race series and class
                "MARKET_DESCRIPTION",   # ADDED: betting type, each-way divisor, rules, turn-in-play
            ],
            "maxResults": "200",
            "sort": "FIRST_TO_START"
        }
        response = requests.post(
            self.config.api_url + "listMarketCatalogue/",
            json=payload,
            headers=headers,
            timeout=30
        )
        self._handle_session_error(response)
        if response.status_code != 200:
            raise Exception(f"Catalogue API failed: {response.status_code} — {response.text}")
        return response.json()

    # -------------------- CATALOGUE GENERATOR --------------------
    # Calls the catalogue API and yields one flat dict per market.
    # Uses a generator (yield) so records stream one at a time into the
    # validation and batching steps — avoids loading all 200 markets into memory at once.
    # Nested API fields (eventType{}, event{}, competition{}, description{}) are
    # flattened here into simple keys for straightforward Pydantic validation and S3 storage.
    #
    # ADDED fields vs previous version:
    #   competition_id, competition_name  → from COMPETITION projection
    #   betting_type, each_way_divisor, turn_in_play, rules → from MARKET_DESCRIPTION projection
    def fetch_markets(self, from_utc: str, to_utc: str) -> Iterator[Dict[str, Any]]:
        markets = self._call_market_catalogue_api(from_utc, to_utc)
        logger.info(f"Fetched {len(markets)} markets")
        for market in markets:
            yield {
                # market level
                "market_id":         market.get("marketId"),
                "market_name":       market.get("marketName"),
                "market_start_time": market.get("marketStartTime"),
                "total_matched":     market.get("totalMatched"),
                "runners":           market.get("runners"),           # kept nested for Bronze

                # event type level
                "event_type_id":     market.get("eventType", {}).get("id"),
                "event_type_name":   market.get("eventType", {}).get("name"),

                # event level
                "event_id":          market.get("event", {}).get("id"),
                "event_name":        market.get("event", {}).get("name"),
                "event_country":     market.get("event", {}).get("countryCode"),
                "event_timezone":    market.get("event", {}).get("timezone"),
                "event_venue":       market.get("event", {}).get("venue"),
                "event_open_date":   market.get("event", {}).get("openDate"),

                # ADDED: competition level — race series and class
                "competition_id":    market.get("competition", {}).get("id"),
                "competition_name":  market.get("competition", {}).get("name"),

                # ADDED: market description — structure and rules from MARKET_DESCRIPTION
                "betting_type":      market.get("description", {}).get("bettingType"),
                "each_way_divisor":  market.get("description", {}).get("eachWayDivisor"),
                "turn_in_play":      market.get("description", {}).get("turnInPlayEnabled"),
                "rules":             market.get("description", {}).get("rules"),
            }

    # -------------------- BOOK API --------------------
    # Calls Betfair's listMarketBook endpoint for a batch of market IDs.
    # Returns current odds snapshot for each market.
    #
    # ADDED priceData vs previous version:
    #   EX_LTP        → last traded price (most recent price a bet was matched at)
    #   EX_TRADED_VOL → runner-level total volume matched
    #   SP_PROJECTED  → near/far SP projection prices (available pre-race only)
    #
    # WEIGHT WARNING: each priceData option adds weight per market.
    # With 5 priceData options, weight ≈ 8 per market.
    # 200 limit ÷ 8 = 25 max markets per request.
    # Batch size reduced from 10 → 8 to stay well within the limit.
    #
    # Pre-race run:  EX_BEST_OFFERS + SP_PROJECTED have data, SP_TRADED is empty
    # Post-race run: SP_TRADED (BSP) has data, EX_BEST_OFFERS is empty (market closed)
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=2, max=10),
        before_sleep=lambda rs: logger.warning(
            f"Book API failed — retrying... attempt {rs.attempt_number}"
        )
    )
    def _call_market_book_api(self, market_ids: List[str]) -> List[Dict[str, Any]]:
        headers = {
            "X-Application": self.config.api_key,
            "X-Authentication": self.token,
            "Content-Type": "application/json",
        }
        payload = {
            "marketIds": market_ids,
            "priceProjection": {
                "priceData": [
                    "EX_BEST_OFFERS",   # best 3 back and lay prices at each level
                    "EX_LTP",           # ADDED: last traded price per runner
                    "EX_TRADED_VOL",    # ADDED: total volume matched per runner
                    "SP_TRADED",        # final BSP — only available post-race
                    "SP_PROJECTED",     # ADDED: near/far SP projection — only available pre-race
                ],
                "virtualise": False     # exclude virtual bets from prices
            }
        }
        response = requests.post(
            self.config.api_url + "listMarketBook/",
            json=payload,
            headers=headers,
            timeout=30
        )
        self._handle_session_error(response)
        if response.status_code != 200:
            raise Exception(f"Book API failed: {response.status_code} — {response.text}")
        return response.json()

    # -------------------- VALIDATION --------------------
    # Validates a single catalogue record against the MarketCatalogue schema.
    # Returns the validated dict if valid, None if invalid.
    # Returning None (not {}) ensures empty records are filtered out
    # before reaching S3 — an empty dict would silently corrupt downstream data.
    def validate_catalogue(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return MarketCatalogue(**record).model_dump()
        except ValidationError as e:
            logger.warning(f"Catalogue validation failed: {e}")
            return None

    # Validates a single book record against the MarketBook schema.
    # Same pattern as validate_catalogue — returns None on failure so
    # invalid records never reach S3.
    def validate_book(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return MarketBook(**record).model_dump()
        except ValidationError as e:
            logger.warning(f"Book validation failed: {e}")
            return None

    # -------------------- BATCHING --------------------
    # Groups records from an iterator into fixed-size batches before uploading.
    # Using a generator (yield) means each batch is uploaded immediately —
    # we never hold the entire dataset in memory at once.
    # The final yield handles the last partial batch (e.g. 20 records when batch_size=50).
    # Used for catalogue (batch_size=50) and book API calls (batch_size=8).
    def batch_records(self, iterator: Iterator[Dict[str, Any]], batch_size: int = 50):
        batch = []
        for record in iterator:
            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:   # flush remaining records that didn't fill a full batch
            yield batch

    # -------------------- S3 UPLOAD --------------------
    # Uploads a batch of records to S3 as NDJSON (newline-delimited JSON).
    # NDJSON format: one JSON object per line — required for Databricks Auto Loader
    # to read files incrementally without loading the whole file into memory.
    # @retry retries up to 3 times if S3 is temporarily unavailable.
    # Key includes Hive-style partition (extracted_date=, run_time=) so
    # Databricks can filter by date and time without scanning all files.
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=2, max=10),
        before_sleep=lambda rs: logger.warning(
            f"S3 upload failed — retrying... attempt {rs.attempt_number}"
        )
    )
    def upload_batch(self, records: List[Dict[str, Any]], key: str):
        body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        self.s3.put_object(
            Bucket=self.config.s3_bucket,
            Key=key,
            Body=body,
            ContentType="application/json"
        )
        logger.info(f"Uploaded {len(records)} records → s3://{self.config.s3_bucket}/{key}")

    # -------------------- RUN CATALOGUE --------------------
    # Morning run — fetches all AU WIN markets for today and saves to S3.
    # Flow: API call → generator → validate → filter None → batch → upload
    # run_time is stamped into the S3 path so each run produces its own folder,
    # making it easy to identify when the data was captured.
    # Returns True if any data was uploaded — the dispatcher uses this
    # to decide whether to proceed with the book run.
    def run_catalogue(self, from_utc: str, to_utc: str, today: str, overwrite: bool = False) -> bool:
        prefix = f"betfair/market_catalogue/extracted_date={today}/"

        # Skip if data already exists for this date — prevents duplicate uploads
        # when cron fires twice or a backfill is re-run accidentally.
        # Returns True so the dispatcher still proceeds with the book run.
        if not overwrite and self._s3_prefix_has_files(prefix):
            logger.info(f"Catalogue already exists for {today} — skipping (use --overwrite to re-run)")
            return True

        logger.info(f"Starting catalogue run for {today} ({from_utc} → {to_utc} UTC)")

        # Capture the AEST run time once for all S3 keys in this run.
        # Stamped into path as run_time=HH-MM so morning and night runs
        # land in separate folders under the same extracted_date partition.
        run_time = datetime.now(self.SYDNEY).strftime("%H-%M")
        success, failed = 0, 0

        # Chain: fetch → validate → filter invalid records → batch → upload
        # Generator expression keeps the pipeline lazy — one record flows through
        # at a time rather than loading all 200 markets into memory first.
        validated_stream = (
            r
            for r in (self.validate_catalogue(rec) for rec in self.fetch_markets(from_utc, to_utc))
            if r is not None   # drop records that failed Pydantic validation
        )

        for i, batch in enumerate(self.batch_records(validated_stream, batch_size=50)):
            # S3 path: betfair/market_catalogue/extracted_date=YYYY-MM-DD/run_time=HH-MM/batch_N.json
            key = f"betfair/market_catalogue/extracted_date={today}/run_time={run_time}/batch_{i}.json"
            try:
                self.upload_batch(batch, key)
                success += len(batch)
            except Exception as e:
                logger.error(f"Catalogue upload failed after retries: {e}")
                failed += len(batch)

        logger.info(f"Catalogue done | Success: {success} | Failed: {failed}")
        return success > 0   # False if 0 markets found — dispatcher will skip book run

    # -------------------- RUN BOOK --------------------
    # Night run — reads market IDs from today's catalogue in S3, then
    # fetches the current odds snapshot for each market.
    # Reads from S3 instead of calling the catalogue API again — avoids an
    # extra API call and uses the exact same market IDs we already stored.
    #
    # CHANGED: batch_size reduced from 10 → 8 because we now request 5 priceData
    # options (EX_BEST_OFFERS, EX_LTP, EX_TRADED_VOL, SP_TRADED, SP_PROJECTED).
    # At ~8 weight points per market, 200 limit ÷ 8 = 25 max — 8 keeps us safe.
    def run_book(self, today: str, overwrite: bool = False):
        book_prefix = f"betfair/market_book/extracted_date={today}/"

        # Skip if book data already exists for today — same idempotency logic as catalogue.
        if not overwrite and self._s3_prefix_has_files(book_prefix):
            logger.info(f"Book already exists for {today} — skipping (use --overwrite to re-run)")
            return

        logger.info(f"Starting book run for {today}")

        # List ALL catalogue files under today's partition.
        # Using a paginator handles cases where there are more than 1000 files.
        # This picks up files from any run_time folder (e.g. 07-30, 11-00)
        # so a backfill or re-run still finds the correct catalogue files.
        catalogue_prefix = f"betfair/market_catalogue/extracted_date={today}/"
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.config.s3_bucket, Prefix=catalogue_prefix)
        catalogue_keys = [obj["Key"] for page in pages for obj in page.get("Contents", [])]

        if not catalogue_keys:
            logger.warning(f"No catalogue files found under {catalogue_prefix} — was catalogue run skipped?")
            return

        logger.info(f"Found {len(catalogue_keys)} catalogue file(s) in S3 — reading market IDs")

        # Read every catalogue NDJSON file and collect market IDs.
        # Each line in the file is one JSON record — parse each line separately.
        market_ids: List[str] = []
        for key in catalogue_keys:
            obj = self.s3.get_object(Bucket=self.config.s3_bucket, Key=key)
            lines = obj["Body"].read().decode("utf-8").strip().splitlines()
            for line in lines:
                mid = json.loads(line).get("market_id")
                if mid:
                    market_ids.append(mid)

        # Remove duplicates while preserving insertion order.
        # Prevents calling the Betfair book API twice for the same market
        # if it appeared in multiple catalogue batches or re-runs.
        market_ids = list(dict.fromkeys(market_ids))
        logger.info(f"{len(market_ids)} unique market IDs loaded from S3")

        if not market_ids:
            logger.warning("No market IDs found — skipping book run")
            return

        # Stamp run_time into S3 path — separates pre-race and post-race book snapshots.
        # Pre-race run (e.g. 11-00) captures live order book (EX_BEST_OFFERS) + SP_PROJECTED.
        # Post-race run (e.g. 23-30) captures final BSP (SP_TRADED).
        run_time = datetime.now(self.SYDNEY).strftime("%H-%M")
        success, failed = 0, 0

        # CHANGED: batch_size reduced from 10 → 8 to stay within Betfair's 200 weight limit.
        # With 5 priceData options requested, each market costs ~8 weight points.
        # 8 markets × 8 weight = 64 points per request — well within the 200 limit.
        batch_size = 8

        for i in range(0, len(market_ids), batch_size):
            id_batch = market_ids[i:i + batch_size]
            batch_num = i // batch_size
            logger.info(f"Fetching book batch {batch_num + 1}: markets {i + 1}–{i + len(id_batch)}")
            try:
                books = self._call_market_book_api(id_batch)

                # Validate each book record and drop any that fail schema check.
                # runners field stays nested (Bronze layer) — flattened later in Databricks.
                validated = [r for r in (self.validate_book(b) for b in books) if r is not None]

                # S3 path: betfair/market_book/extracted_date=YYYY-MM-DD/run_time=HH-MM/batch_N.json
                key = f"betfair/market_book/extracted_date={today}/run_time={run_time}/batch_{batch_num}.json"
                self.upload_batch(validated, key)
                success += len(validated)
            except Exception as e:
                logger.error(f"Book batch {batch_num + 1} failed after retries: {e}")
                failed += len(id_batch)

        logger.info(f"Book done | Success: {success} | Failed: {failed}")

    # -------------------- DISPATCHER --------------------
    # Routes execution to the correct run method based on --mode argument.
    # catalogue  → morning run only (7:30 AM)
    # book       → night run only  (11:30 PM) — reads market IDs from S3
    # all        → both in sequence — only runs book if catalogue uploaded data
    # Wraps everything in try/except so any unhandled crash triggers an SNS alert
    # and re-raises the error so cron records a non-zero exit code (job failed).
    def run(self, mode: str, from_utc: str, to_utc: str, today: str, overwrite: bool = False):
        try:
            if mode == "catalogue":
                self.run_catalogue(from_utc, to_utc, today, overwrite)
            elif mode == "book":
                self.run_book(today, overwrite)
            elif mode == "all":
                # Only proceed to book if catalogue actually uploaded data.
                # If 0 markets were found (e.g. ran at wrong time), skip book run.
                uploaded = self.run_catalogue(from_utc, to_utc, today, overwrite)
                if uploaded:
                    self.run_book(today, overwrite)
                else:
                    logger.warning("Catalogue returned 0 markets — skipping book run")

        except Exception as e:
            # Send SNS alert so you know immediately when the overnight cron fails.
            # re-raise after alerting so the cron job records a non-zero exit code.
            msg = f"Pipeline failed | Date: {today} | Mode: {mode} | Error: {e}"
            logger.error(msg)
            self._send_alert(subject=f"Betfair Pipeline FAILED — {today}", message=msg)
            raise


# -------------------- ENTRY --------------------
# Script entry point — only runs when executed directly, not when imported.
# Parses CLI arguments, computes the AEST date and UTC time window,
# then hands off to the pipeline.
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Betfair Racing Bronze Pipeline")

    # --mode controls which part of the pipeline runs.
    # catalogue = morning run  (7:30 AM AEST) — fetches market list
    # book      = night run    (11:30 PM AEST) — fetches odds + BSP
    # all       = both in sequence (used for testing or manual runs)
    parser.add_argument(
        "--mode",
        choices=["catalogue", "book", "all"],
        default="all",
        help="catalogue=morning run | book=night run | all=both"
    )

    # --date enables backfilling — run the pipeline for any past date.
    # Example: python script.py --mode catalogue --date 2026-05-01
    # Defaults to today AEST when not provided.
    parser.add_argument(
        "--date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Date to process. Defaults to today AEST. Use for backfilling missed days."
    )

    # --overwrite bypasses the idempotency check and re-uploads even if
    # data already exists in S3 for this date.
    # Use when re-running after a partial failure or data quality fix.
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Force re-run even if data already exists in S3 for this date."
    )
    args = parser.parse_args()

    SYDNEY = ZoneInfo("Australia/Sydney")

    # Build the target date and UTC window.
    # If --date is given, parse it as an AEST date for backfilling.
    # Otherwise default to today in AEST (handles daylight saving automatically).
    if args.date:
        y, m, d = int(args.date[:4]), int(args.date[5:7]), int(args.date[8:10])
        target = datetime(y, m, d, tzinfo=SYDNEY)
        today  = args.date
        logger.info(f"Backfill mode — processing date: {today}")
    else:
        target = datetime.now(SYDNEY)
        today  = target.strftime("%Y-%m-%d")

    # Convert the full AEST race day (00:00 → 23:59) to UTC for the Betfair filter.
    # Betfair's API only accepts UTC — ZoneInfo handles AEST/AEDT offset automatically.
    day_start = target.replace(hour=0,  minute=0,  second=0,  microsecond=0).astimezone(timezone.utc)
    day_end   = target.replace(hour=23, minute=59, second=59, microsecond=0).astimezone(timezone.utc)
    from_utc  = day_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_utc    = day_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(
        f"Mode: {args.mode} | Date: {today} AEST | "
        f"UTC: {from_utc} → {to_utc} | Overwrite: {args.overwrite}"
    )

    # Initialise the pipeline (connects to S3, SNS, logs in to Betfair)
    # then dispatch to the correct run method based on --mode.
    pipeline = BetfairPipeline(config)
    pipeline.run(args.mode, from_utc, to_utc, today, args.overwrite)
    