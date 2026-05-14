import os
import json
import time
import logging
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Iterator, Dict, Any, Optional, List
import datetime as dt_module  # Add this at the very top of your function if needed, or just use:
import requests
import boto3
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential

# -------------------- LOGGING --------------------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# -------------------- CONFIG --------------------
@dataclass(frozen=True)
class BetfairConfig:
    """Holds all API and AWS credentials in a read-only container."""

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


# Load credentials from local .env files
load_dotenv(os.path.join(os.path.dirname(__file__), "betfair_credits.env"))
load_dotenv(os.path.join(os.path.dirname(__file__), "aws_s3.env"))

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

# Fail fast if essential credentials are missing
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
class MarketCatalogue(BaseModel):
    """Schema for Discovery Layer: Defines what a 'Race' looks like."""

    model_config = {"extra": "ignore"}
    market_id: Optional[str] = None
    market_name: Optional[str] = None
    market_start_time: Optional[str] = None
    total_matched: Optional[float] = None
    runners: Optional[List[Dict[str, Any]]] = None
    event_type_id: Optional[str] = None
    event_type_name: Optional[str] = None
    event_id: Optional[str] = None
    event_name: Optional[str] = None
    event_country: Optional[str] = None
    event_timezone: Optional[str] = None
    event_venue: Optional[str] = None
    event_open_date: Optional[str] = None
    competition_id: Optional[str] = None
    competition_name: Optional[str] = None
    betting_type: Optional[str] = None
    each_way_divisor: Optional[float] = None
    turn_in_play: Optional[bool] = None
    rules: Optional[str] = None


class MarketBook(BaseModel):
    """Schema for Capture Layer: Defines price snapshots for dbt modeling."""

    model_config = {"extra": "ignore"}
    marketId: Optional[str] = None
    status: Optional[str] = None
    totalMatched: Optional[float] = None
    totalAvailable: Optional[float] = None
    runners: Optional[List[Dict[str, Any]]] = None
    isMarketDataDelayed: Optional[bool] = None
    betDelay: Optional[int] = None
    crossMatchingEnabled: Optional[bool] = None
    runnersVoidable: Optional[bool] = None
    version: Optional[int] = None
    complete: Optional[bool] = None

    # --- METADATA FOR DOWNSTREAM DBT ---
    # Used as the primary key for 'Latest State' logic in dbt
    ingested_at: Optional[str] = None
    # Used to monitor API health and Fargate performance
    api_latency_ms: Optional[int] = None


# -------------------- PIPELINE CLASS --------------------
class BetfairPipeline:
    """Main ETL class handling API ingestion and S3 storage."""

    SYDNEY = ZoneInfo("Australia/Sydney")

    # def __init__(self, config: BetfairConfig):
    #     self.config = config
    #     self.s3 = boto3.client(
    #         "s3",
    #         aws_access_key_id=config.aws_access_key,
    #         aws_secret_access_key=config.aws_secret_key,
    #         region_name=config.region,
    #     )
    #     self.sns = boto3.client("sns", ...) if config.sns_topic_arn else None
    #     self.token = self._get_session_token()
    #     self.metrics = {
    #         "total_snapshots": 0,
    #         "api_errors": 0,
    #         "start_time": datetime.now(),
    #     }
    def __init__(self, config: BetfairConfig):
        self.config = config
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key,
            aws_secret_access_key=config.aws_secret_key,
            region_name=config.region,
        )
        self.sns = boto3.client("sns") if config.sns_topic_arn else None
        self.token = self._get_session_token()
        self.metrics = {
            "total_snapshots": 0,
            "api_errors": 0,
            "start_time": datetime.now(),  # Corrected
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _get_session_token(self) -> str:
        """Authenticates with Betfair and returns the X-Authentication token."""
        response = requests.post(
            self.config.login_url,
            data={"username": self.config.username, "password": self.config.password},
            headers={
                "X-Application": self.config.api_key,
                "Accept": "application/json",
            },
        )
        data = response.json()
        if data.get("status") != "SUCCESS":
            raise Exception(f"Login failed: {data}")
        logger.info("Betfair login successful")
        return data["token"]

    def _handle_session_error(self, response: requests.Response):
        """Automatically refreshes token if the session expires (401/403)."""
        if response.status_code in {401, 403}:
            logger.warning(f"Session error ({response.status_code}) — refreshing token")
            self.token = self._get_session_token()
            raise Exception("Token refreshed — retrying")

    def _s3_prefix_has_files(self, prefix: str) -> bool:
        """Helper to check if data already exists for a specific day/run."""
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=prefix):
            if page.get("Contents"):
                return True
        return False

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _call_market_catalogue_api(
        self, from_utc: str, to_utc: str
    ) -> List[Dict[str, Any]]:
        """API Call: Searches for AU Win markets scheduled for today."""
        headers = {
            "X-Application": self.config.api_key,
            "X-Authentication": self.token,
            "Content-Type": "application/json",
        }
        payload = {
            "filter": {
                "eventTypeIds": ["7"],
                "marketCountries": ["AU"],
                "marketTypeCodes": ["WIN"],
                "marketStartTime": {"from": from_utc, "to": to_utc},
            },
            "marketProjection": [
                "EVENT",
                "MARKET_START_TIME",
                "RUNNER_DESCRIPTION",
                "EVENT_TYPE",
                "COMPETITION",
                "MARKET_DESCRIPTION",
            ],
            "maxResults": "200",
            "sort": "FIRST_TO_START",
        }
        response = requests.post(
            self.config.api_url + "listMarketCatalogue/",
            json=payload,
            headers=headers,
            timeout=30,
        )
        self._handle_session_error(response)
        return response.json()

    def fetch_markets(self, from_utc: str, to_utc: str) -> Iterator[Dict[str, Any]]:
        """Generator: Flattens the API response into the MarketCatalogue schema."""
        markets = self._call_market_catalogue_api(from_utc, to_utc)
        for market in markets:
            yield {
                "market_id": market.get("marketId"),
                "market_name": market.get("marketName"),
                "market_start_time": market.get("marketStartTime"),
                # ... other flattened fields ...
            }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _call_market_book_api(self, market_ids: List[str]) -> List[Dict[str, Any]]:
        """API Call: Fetches the actual prices for a list of market IDs."""
        headers = {
            "X-Application": self.config.api_key,
            "X-Authentication": self.token,
            "Content-Type": "application/json",
        }
        payload = {
            "marketIds": market_ids,
            "priceProjection": {
                "priceData": ["EX_BEST_OFFERS", "EX_TRADED", "SP_TRADED"],
                "virtualise": False,
            },
        }
        response = requests.post(
            self.config.api_url + "listMarketBook/",
            json=payload,
            headers=headers,
            timeout=30,
        )
        self._handle_session_error(response)
        return response.json()

    def validate_book(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Ensures data matches our Pydantic schema before saving to S3."""
        if not isinstance(record, dict):
            return None
        try:
            return MarketBook(**record).model_dump()
        except ValidationError as e:
            logger.error(f"Pydantic validation error: {e}")
            return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def upload_batch(self, records: List[Dict[str, Any]], key: str):
        """Writes JSON records to S3 as a single file (Batch processing)."""
        body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        self.s3.put_object(
            Bucket=self.config.s3_bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        logger.info(f"Uploaded {len(records)} records → {key}")

    def run_catalogue(
        self, from_utc: str, to_utc: str, today: str, overwrite: bool = False
    ) -> bool:
        """Discovery Logic: Populates S3 with today's race list."""
        prefix = f"betfair/market_catalogue/extracted_date={today}/"
        if not overwrite and self._s3_prefix_has_files(prefix):
            return True
        run_time = datetime.now(self.SYDNEY).strftime("%H-%M")
        success = 0
        validated_stream = (
            r
            for r in (
                self.validate_catalogue(rec)
                for rec in self.fetch_markets(from_utc, to_utc)
            )
            if r is not None
        )
        for i, batch in enumerate(self.batch_records(validated_stream, batch_size=50)):
            for record in batch:
                (
                    record["extracted_date"],
                    record["run_time"],
                    record["snapshot_type"],
                ) = today, run_time, None
            key = f"{prefix}run_time={run_time}/batch_{i}.json"
            self.upload_batch(batch, key)
            success += len(batch)
        return success > 0

    def load_schedule_from_s3(self, today: str) -> List[tuple]:
        """Deduplicates and groups all market IDs found in S3 by their start time."""
        logger.info(f"Loading schedule from S3 for {today}")
        prefix = f"betfair/market_catalogue/extracted_date={today}/"
        try:
            res = self.s3.list_objects_v2(Bucket=self.config.s3_bucket, Prefix=prefix)
            if "Contents" not in res:
                return []
            all_markets = []
            for obj in res["Contents"]:
                if not obj["Key"].endswith(".json"):
                    continue
                f = self.s3.get_object(Bucket=self.config.s3_bucket, Key=obj["Key"])
                for line in f["Body"].read().decode("utf-8").strip().splitlines():
                    all_markets.append(json.loads(line))

            # Use market_id as key to ensure we don't process the same race twice
            unique_markets = {m["market_id"]: m for m in all_markets}.values()
            return self._group_and_format(list(unique_markets))
        except Exception as e:
            logger.error(f"Schedule load failed: {e}")
            return []

    def _group_and_format(self, markets: List[Dict]) -> List[tuple]:
        """Calculates the T-5 minute trigger for every race group."""
        from collections import defaultdict

        groups = defaultdict(list)
        for m in markets:
            st = m.get("market_start_time")
            if st:
                dt = datetime.fromisoformat(st.replace("Z", "+00:00"))
                groups[dt].append(m["market_id"])
        schedule = []
        for dt in sorted(groups.keys()):
            trigger = dt - timedelta(minutes=5)
            aest = dt.astimezone(self.SYDNEY).strftime("%H:%M")
            schedule.append((dt, trigger, groups[dt], aest))
        return schedule

    def run_dynamic(self, today: str):
        """CONTINUOUS DISCOVERY LOOP: Runs until no more races exist for the day."""
        logger.info(f"Dynamic Mode: Starting continuous discovery for {today}")

        while True:
            # 1. REFRESH CATALOGUE: Always check for late-added races or schedule updates
            now_utc = datetime.now(timezone.utc)
            refresh_to = (now_utc + timedelta(days=1)).replace(
                hour=0, minute=0, second=0
            )
            self.run_catalogue(
                now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                refresh_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
                today,
                overwrite=True,
            )

            # 2. FILTER: Find races where the T-5 trigger hasn't passed yet
            full_schedule = self.load_schedule_from_s3(today)
            active_schedule = [
                item
                for item in full_schedule
                if item[1] > (datetime.now(timezone.utc) - timedelta(minutes=2))
            ]

            # 3. TERMINATION: If the schedule is empty, the racing day is finished
            if not active_schedule:
                logger.info(
                    "FINISHED: No more upcoming races found. Closing pipeline for the day."
                )
                break  # This ends the Fargate task and saves cost

            # 4. WAIT: Target the very next race group
            start_time, trigger_time, market_ids, aest_label = active_schedule[0]
            now_utc = datetime.now(timezone.utc)
            wait_secs = (trigger_time - now_utc).total_seconds()

            if wait_secs > 0:
                logger.info(
                    f"Next race: {aest_label}. Waiting {wait_secs / 60:.1f} min..."
                )
                time.sleep(wait_secs)

            # 5. CAPTURE: Poll for price snapshots until the race is CLOSED
            self.execute_poll_worker(
                market_ids, today, timedelta(minutes=15), aest_label
            )

    def execute_poll_worker(
        self,
        market_ids: List[str],
        today: str,
        max_duration: timedelta,
        aest_label: str,
    ):
        """Polls MarketBook until all markets are CLOSED or 15 mins pass."""
        poll_start = datetime.now(timezone.utc)
        poll_count = 0
        logger.info(f"Capture Layer: Starting T-5 poll for {aest_label}")

        while (datetime.now(timezone.utc) - poll_start) < max_duration:
            # SAFETY: Hard-stop at 11 PM AEST to prevent script running into next day
            current_sydney_time = datetime.now(self.SYDNEY).time()
            if current_sydney_time > dt_module.time(23, 0):
                logger.info("Hard stop reached for the day. Terminating Fargate task.")
                break

            run_time = datetime.now(self.SYDNEY).strftime("%H-%M-%S")
            api_start = time.time()

            try:
                books = self._call_market_book_api(market_ids)
                latency = int((time.time() - api_start) * 1000)

                valid_books = []
                if isinstance(books, list):
                    for b in books:
                        validated = self.validate_book(b)
                        if validated:
                            # Add metadata for dbt partitioning and state tracking
                            validated["ingested_at"] = datetime.now(
                                timezone.utc
                            ).isoformat()
                            validated["api_latency_ms"] = latency
                            validated["extracted_date"], validated["run_time"] = (
                                today,
                                run_time,
                            )
                            validated["snapshot_type"] = "PRE_RACE"
                            valid_books.append(validated)

                if valid_books:
                    key = f"betfair/market_book/extracted_date={today}/snapshot_type=PRE_RACE/run_time={run_time}/batch_{poll_count}.json"
                    self.upload_batch(valid_books, key)
                    self.metrics["total_snapshots"] += len(valid_books)
                    poll_count += 1

                    # SUCCESS EXIT: Stop polling if Betfair marks all markets as CLOSED
                    if all(b.get("status") == "CLOSED" for b in valid_books):
                        logger.info(
                            f"Capture Layer: {aest_label} confirmed CLOSED. Ending poll."
                        )
                        break

                time.sleep(15)  # Poll frequency: 4 snapshots per minute

            except Exception as e:
                self.metrics["api_errors"] += 1
                logger.error(f"Poll Worker Error ({aest_label}): {e}")
                time.sleep(15)

    def batch_records(self, iterable: Iterator, batch_size: int) -> Iterator[List]:
        """Helper to group a stream of records into smaller batches."""
        batch = []
        for item in iterable:
            batch.append(item)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def validate_catalogue(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Ensures catalogue data matches our Pydantic schema."""
        try:
            return MarketCatalogue(**record).model_dump()
        except ValidationError as e:
            logger.error(f"Catalogue validation error: {e}")
            return None

    def _send_alert(self, subject: str, message: str):
        """Sends a notification via SNS if configured."""
        if self.sns and self.config.sns_topic_arn:
            try:
                self.sns.publish(
                    TopicArn=self.config.sns_topic_arn, Subject=subject, Message=message
                )
            except Exception as e:
                logger.error(f"Failed to send SNS alert: {e}")
    def run(
        self,
        mode: str,
        from_utc: str,
        to_utc: str,
        today: str,
        overwrite: bool = False,
        snapshot_type: str = None,
    ):
        """Dispatcher: Orchestrates the pipeline modes."""
        try:
            if mode == "catalogue":
                self.run_catalogue(from_utc, to_utc, today, overwrite)
            elif mode == "dynamic":
                self.run_dynamic(today)
            elif mode == "all":
                # Ensure discovery is done before entering the execution loop
                if self.run_catalogue(from_utc, to_utc, today, overwrite):
                    logger.info("Discovery complete. Entering dynamic execution...")
                    self.run_dynamic(today)
                else:
                    logger.warning(
                        "No catalogue found. Starting dynamic mode heartbeat..."
                    )
                    self.run_dynamic(today)

        except Exception as e:
            msg = f"Pipeline failed | Date: {today} | Mode: {mode} | Error: {e}"
            logger.error(msg)
            self._send_alert(subject="Betfair Pipeline FAILED", message=msg)
            raise


# -------------------- ENTRY --------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", choices=["catalogue", "dynamic", "all"], default="all"
    )
    parser.add_argument("--date", default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    SYDNEY = ZoneInfo("Australia/Sydney")

    # Correct usage based on your 'from datetime import datetime' import
    if args.date:
        target = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=SYDNEY)
    else:
        target = datetime.now(SYDNEY)

    today = target.strftime("%Y-%m-%d")

    # Correct usage based on your 'from datetime import timezone' import
    day_start = target.replace(hour=0, minute=0, second=0).astimezone(timezone.utc)
    day_end = target.replace(hour=23, minute=59, second=59).astimezone(timezone.utc)

    pipeline = BetfairPipeline(config)
    pipeline.run(
        args.mode,
        day_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        day_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        today,
        args.overwrite,
    )







# Replace <SCHEDULE_NAME> with the name from the list above
aws scheduler get-schedule --name FormFav_PreRace_0900 --region ap-southeast-2