import os
import json
import time
import logging
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import datetime
from zoneinfo import ZoneInfo
from typing import Iterator, Dict, Any, Optional, List

import requests
import boto3
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential

# -------------------- LOGGING --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# -------------------- CONFIG --------------------
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
    
    # --- NEW METADATA FIELDS ---
    # Becomes your "primary key" for 'Latest State' logic in dbt
    ingested_at: Optional[str] = None 
    
    # Measures the health/speed of the Betfair API connection
    api_latency_ms: Optional[int] = None


# -------------------- PIPELINE CLASS --------------------
class BetfairPipeline:
    SYDNEY = ZoneInfo("Australia/Sydney")

    def __init__(self, config: BetfairConfig):
        self.config = config
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key,
            aws_secret_access_key=config.aws_secret_key,
            region_name=config.region
        )
        self.sns = (
            boto3.client(
                "sns",
                aws_access_key_id=config.aws_access_key,
                aws_secret_access_key=config.aws_secret_key,
                region_name=config.region
            )
            if config.sns_topic_arn else None
        )
        self.token = self._get_session_token()
        # --- NEW STATE TRACKERS ---
        self.metrics = {"total_snapshots": 0, "api_errors": 0, "start_time": datetime.now()}
        self.pending_schedule = [] 

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
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

    def _handle_session_error(self, response: requests.Response):
        if response.status_code in {401, 403}:
            logger.warning(f"Session error ({response.status_code}) — refreshing token")
            self.token = self._get_session_token()
            raise Exception("Token refreshed — retrying")

    def _s3_prefix_has_files(self, prefix: str) -> bool:
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=prefix):
            if page.get("Contents"):
                return True
        return False

    def _send_alert(self, subject: str, message: str):
        if not self.sns: return
        try:
            self.sns.publish(TopicArn=self.config.sns_topic_arn, Subject=subject, Message=message)
        except Exception as e:
            logger.error(f"Alert failed: {e}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _call_market_catalogue_api(self, from_utc: str, to_utc: str) -> List[Dict[str, Any]]:
        headers = {"X-Application": self.config.api_key, "X-Authentication": self.token, "Content-Type": "application/json"}
        payload = {
            "filter": {
                "eventTypeIds": ["7"], "marketCountries": ["AU"], "marketTypeCodes": ["WIN"],
                "marketStartTime": {"from": from_utc, "to": to_utc}
            },
            "marketProjection": ["EVENT", "MARKET_START_TIME", "RUNNER_DESCRIPTION", "EVENT_TYPE", "COMPETITION", "MARKET_DESCRIPTION"],
            "maxResults": "200", "sort": "FIRST_TO_START"
        }
        response = requests.post(self.config.api_url + "listMarketCatalogue/", json=payload, headers=headers, timeout=30)
        self._handle_session_error(response)
        return response.json()

    def fetch_markets(self, from_utc: str, to_utc: str) -> Iterator[Dict[str, Any]]:
        markets = self._call_market_catalogue_api(from_utc, to_utc)
        for market in markets:
            yield {
                "market_id": market.get("marketId"),
                "market_name": market.get("marketName"),
                "market_start_time": market.get("marketStartTime"),
                "total_matched": market.get("totalMatched"),
                "runners": market.get("runners"),
                "event_type_id": market.get("eventType", {}).get("id"),
                "event_type_name": market.get("eventType", {}).get("name"),
                "event_id": market.get("event", {}).get("id"),
                "event_name": market.get("event", {}).get("name"),
                "event_country": market.get("event", {}).get("countryCode"),
                "event_timezone": market.get("event", {}).get("timezone"),
                "event_venue": market.get("event", {}).get("venue"),
                "event_open_date": market.get("event", {}).get("openDate"),
                "competition_id": market.get("competition", {}).get("id"),
                "competition_name": market.get("competition", {}).get("name"),
                "betting_type": market.get("description", {}).get("bettingType"),
                "each_way_divisor": market.get("description", {}).get("eachWayDivisor"),
                "turn_in_play": market.get("description", {}).get("turnInPlayEnabled"),
                "rules": market.get("description", {}).get("rules"),
            }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _call_market_book_api(self, market_ids: List[str]) -> List[Dict[str, Any]]:
        headers = {"X-Application": self.config.api_key, "X-Authentication": self.token, "Content-Type": "application/json"}
        payload = {
            "marketIds": market_ids,
            "priceProjection": {"priceData": ["EX_BEST_OFFERS", "EX_TRADED", "SP_TRADED"], "virtualise": False}
        }
        response = requests.post(self.config.api_url + "listMarketBook/", json=payload, headers=headers, timeout=30)
        self._handle_session_error(response)
        return response.json()

    def validate_catalogue(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try: return MarketCatalogue(**record).model_dump()
        except ValidationError: return None

    # def validate_book(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    #     try: return MarketBook(**record).model_dump()
    #     except ValidationError: return None

    def validate_book(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Add this check to ensure the record is a dictionary
        if not isinstance(record, dict):
            logger.warning(f"Skipping record: Expected dict but got {type(record)}. Value: {record}")
            return None
        try: 
            return MarketBook(**record).model_dump()
        except ValidationError as e: 
            logger.error(f"Pydantic validation error: {e}")
            return None

    def batch_records(self, iterator: Iterator[Dict[str, Any]], batch_size: int = 50):
        batch = []
        for record in iterator:
            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch: yield batch

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def upload_batch(self, records: List[Dict[str, Any]], key: str):
        body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        self.s3.put_object(Bucket=self.config.s3_bucket, Key=key, Body=body, ContentType="application/json")
        logger.info(f"Uploaded {len(records)} records → {key}")

    def run_catalogue(self, from_utc: str, to_utc: str, today: str, overwrite: bool = False) -> bool:
        prefix = f"betfair/market_catalogue/extracted_date={today}/"
        if not overwrite and self._s3_prefix_has_files(prefix):
            return True
        run_time = datetime.now(self.SYDNEY).strftime("%H-%M")
        success = 0
        validated_stream = (r for r in (self.validate_catalogue(rec) for rec in self.fetch_markets(from_utc, to_utc)) if r is not None)
        for i, batch in enumerate(self.batch_records(validated_stream, batch_size=50)):
            for record in batch:
                record["extracted_date"], record["run_time"], record["snapshot_type"] = today, run_time, None
            key = f"{prefix}run_time={run_time}/batch_{i}.json"
            self.upload_batch(batch, key)
            success += len(batch)
        return success > 0

    # -------------------- DYNAMIC HELPERS (THE FIX) --------------------
    def load_schedule_from_s3(self, today: str) -> List[tuple]:
        logger.info(f"Loading schedule from S3 for {today}")
        prefix = f"betfair/market_catalogue/extracted_date={today}/"
        try:
            res = self.s3.list_objects_v2(Bucket=self.config.s3_bucket, Prefix=prefix)
            if 'Contents' not in res: return []
            all_markets = []
            for obj in res['Contents']:
                if not obj['Key'].endswith('.json'): continue
                f = self.s3.get_object(Bucket=self.config.s3_bucket, Key=obj['Key'])
                for line in f['Body'].read().decode('utf-8').strip().splitlines():
                    all_markets.append(json.loads(line))
            unique_markets = {m['market_id']: m for m in all_markets}.values()
            return self._group_and_format(list(unique_markets))
        except Exception as e:
            logger.error(f"Schedule load failed: {e}")
            return []

    def _group_and_format(self, markets: List[Dict]) -> List[tuple]:
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

    # 
    
    def run_dynamic(self, today: str):
        MAX_DURATION = timedelta(minutes=20)  # Safety buffer to catch final results
        last_catalogue_refresh = datetime.min.replace(tzinfo=timezone.utc)
        processed_market_ids = set()

        logger.info("Starting Separated Ingestion Engine (Discovery + Capture)")

        while True:
            now = datetime.now(timezone.utc)

            # --- LAYER 1: DISCOVERY (Every 15 mins) ---
            if (now - last_catalogue_refresh).total_seconds() > 900:
                logger.info("Discovery Layer: Refreshing market catalogue...")
                refresh_to = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0)
                self.run_catalogue(now.strftime("%Y-%m-%dT%H:%M:%SZ"), 
                                   refresh_to.strftime("%Y-%m-%dT%H:%M:%SZ"), today, overwrite=True)
                
                full_schedule = self.load_schedule_from_s3(today)
                # Filter for future races not yet processed
                self.pending_schedule = [
                    item for item in full_schedule 
                    if item[0] > now and not any(mid in processed_market_ids for mid in item[2])
                ]
                last_catalogue_refresh = now

            # --- LAYER 2: CAPTURE (The Orchestrator) ---
            # Sort to ensure we handle the earliest races first
            self.pending_schedule.sort(key=lambda x: x[0])

            found_race_to_poll = False
            for item in self.pending_schedule[:]:
                start_time, trigger_time, market_ids, aest_str = item
                
                if now >= trigger_time:
                    # Execute the worker for this specific race group
                    self.execute_poll_worker(market_ids, today, MAX_DURATION, aest_str)
                    
                    # Mark as processed and remove from pending
                    for mid in market_ids: processed_market_ids.add(mid)
                    self.pending_schedule.remove(item)
                    found_race_to_poll = True
                    break # Return to top of loop to re-check time/catalogue

            if not found_race_to_poll:
                # System Health Monitoring Log
                uptime = datetime.now() - self.metrics["start_time"]
                logger.info(f"HEALTH: {self.metrics['total_snapshots']} records saved | Uptime: {uptime}")
                time.sleep(30) # Heartbeat

    # def run(self, mode: str, from_utc: str, to_utc: str, today: str, overwrite: bool = False, snapshot_type: str = None):
    #     try:
    #         if mode == "catalogue": self.run_catalogue(from_utc, to_utc, today, overwrite)
    #         elif mode == "dynamic": self.run_dynamic(today)
    #         elif mode == "all":
    #             if self.run_catalogue(from_utc, to_utc, today, overwrite):
    #                 logger.info("One-shot catalogue complete.")
    #     except Exception as e:
    #         msg = f"Pipeline failed | Date: {today} | Mode: {mode} | Error: {e}"
    #         logger.error(msg)
    #         self._send_alert(subject="Betfair Pipeline FAILED", message=msg)
    #         raise
    def execute_poll_worker(self, market_ids: List[str], today: str, max_duration: timedelta, aest_label: str):
        poll_start = datetime.now(timezone.utc)
        poll_count = 0
        logger.info(f"Capture Layer: Starting T-5 poll for {aest_label}")

        while (datetime.now(timezone.utc) - poll_start) < max_duration:
            current_sydney_time = datetime.datetime.now(self.SYDNEY).time()
            if current_sydney_time > datetime.time(23, 0): # Hard stop at 11 PM
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
                            # POINT 2 & 5: Append-Only Metadata & Metrics
                            validated["ingested_at"] = datetime.now(timezone.utc).isoformat()
                            validated["api_latency_ms"] = latency
                            # Carry over metadata for partitioning
                            validated["extracted_date"] = today
                            validated["run_time"] = run_time
                            validated["snapshot_type"] = "PRE_RACE"
                            valid_books.append(validated)

                if valid_books:
                    # Uploading as a unique batch per poll (Append-only)
                    key = f"betfair/market_book/extracted_date={today}/snapshot_type=PRE_RACE/run_time={run_time}/batch_{poll_count}.json"
                    self.upload_batch(valid_books, key)
                    
                    self.metrics["total_snapshots"] += len(valid_books)
                    poll_count += 1

                    # POINT 4: Exit based on confirmed data status
                    if all(b.get("status") == "CLOSED" for b in valid_books):
                        logger.info(f"Capture Layer: {aest_label} confirmed CLOSED. Ending poll.")
                        break
                
                time.sleep(15) 

            except Exception as e:
                self.metrics["api_errors"] += 1
                logger.error(f"Poll Worker Error ({aest_label}): {e}")
                time.sleep(15)

                
                    
    def run(self, mode: str, from_utc: str, to_utc: str, today: str, overwrite: bool = False, snapshot_type: str = None):
        try:
            if mode == "catalogue": 
                self.run_catalogue(from_utc, to_utc, today, overwrite)
            
            elif mode == "dynamic": 
                self.run_dynamic(today)
            
            elif mode == "all":
                # 1. Fetch the initial catalogue to ensure S3 is populated
                if self.run_catalogue(from_utc, to_utc, today, overwrite):
                    logger.info("Initial catalogue fetch successful. Switching to Dynamic mode...")
                    # 2. Hand over control to the dynamic loop
                    self.run_dynamic(today)
                else:
                    logger.warning("Catalogue fetch returned no data. Dynamic mode may not have races to poll.")
                    self.run_dynamic(today) # Still start it in case 15-min heartbeat finds data later

        except Exception as e:
            msg = f"Pipeline failed | Date: {today} | Mode: {mode} | Error: {e}"
            logger.error(msg)
            self._send_alert(subject="Betfair Pipeline FAILED", message=msg)
            raise


# -------------------- ENTRY --------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["catalogue", "dynamic", "all"], default="all")
    parser.add_argument("--date", default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    SYDNEY = ZoneInfo("Australia/Sydney")
    target = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=SYDNEY) if args.date else datetime.now(SYDNEY)
    today = target.strftime("%Y-%m-%d")
    
    day_start = target.replace(hour=0, minute=0, second=0).astimezone(timezone.utc)
    day_end = target.replace(hour=23, minute=59, second=59).astimezone(timezone.utc)
    
    pipeline = BetfairPipeline(config)
    pipeline.run(args.mode, day_start.strftime("%Y-%m-%dT%H:%M:%SZ"), day_end.strftime("%Y-%m-%dT%H:%M:%SZ"), today, args.overwrite)