import os
import json
import time
import signal
import logging
import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from datetime import time as dt_time
from zoneinfo import ZoneInfo
from typing import Iterator, Dict, Any, Optional, List
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

# ECS sends SIGTERM before SIGKILL — catch it to finish the current poll cleanly
_shutdown = False

def _handle_sigterm(signum, frame):
    global _shutdown
    logger.info("SIGTERM received — finishing current poll then shutting down.")
    _shutdown = True

signal.signal(signal.SIGTERM, _handle_sigterm)


# -------------------- CONFIG --------------------
@dataclass(frozen=True)
class BetfairConfig:
    api_key:        str
    login_url:      str
    username:       str
    password:       str
    api_url:        str
    s3_bucket:      str
    aws_access_key: str
    aws_secret_key: str
    region:         str = "ap-southeast-2"
    sns_topic_arn:  Optional[str] = None


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
    "BETFAIR_APP_KEY", "BETFAIR_LOGIN_URL", "BETFAIR_USERNAME",
    "BETFAIR_PASSWORD", "BETFAIR_API_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
]
missing = [v for v in required_vars if not os.getenv(v)]
if missing:
    raise ValueError(f"Missing required environment variables: {missing}")


# -------------------- PYDANTIC SCHEMAS --------------------
class MarketCatalogue(BaseModel):
    model_config = {"extra": "ignore"}
    market_id:         Optional[str]                  = None
    market_name:       Optional[str]                  = None
    market_start_time: Optional[str]                  = None
    total_matched:     Optional[float]                = None
    runners:           Optional[List[Dict[str, Any]]] = None
    event_type_id:     Optional[str]                  = None
    event_type_name:   Optional[str]                  = None
    event_id:          Optional[str]                  = None
    event_name:        Optional[str]                  = None
    event_country:     Optional[str]                  = None
    event_timezone:    Optional[str]                  = None
    event_venue:       Optional[str]                  = None
    event_open_date:   Optional[str]                  = None
    competition_id:    Optional[str]                  = None
    competition_name:  Optional[str]                  = None
    betting_type:      Optional[str]                  = None
    each_way_divisor:  Optional[float]                = None
    turn_in_play:      Optional[bool]                 = None
    rules:             Optional[str]                  = None


class MarketBook(BaseModel):
    model_config = {"extra": "ignore"}
    marketId:              Optional[str]                  = None
    status:                Optional[str]                  = None
    totalMatched:          Optional[float]                = None
    totalAvailable:        Optional[float]                = None
    runners:               Optional[List[Dict[str, Any]]] = None
    isMarketDataDelayed:   Optional[bool]                 = None
    betDelay:              Optional[int]                  = None
    inplay:                Optional[bool]                 = None
    crossMatching:         Optional[bool]                 = None
    runnersVoidable:       Optional[bool]                 = None
    version:               Optional[int]                  = None
    complete:              Optional[bool]                 = None
    bspReconciled:         Optional[bool]                 = None
    numberOfWinners:       Optional[int]                  = None
    numberOfRunners:       Optional[int]                  = None
    numberOfActiveRunners: Optional[int]                  = None
    ingested_at:           Optional[str]                  = None
    api_latency_ms:        Optional[int]                  = None


# -------------------- PIPELINE CLASS --------------------
class BetfairPipeline:
    SYDNEY = ZoneInfo("Australia/Sydney")

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
            "api_errors":      0,
            "clean_exits":     0,
            "cap_exits":       0,
            "start_time":      datetime.now(timezone.utc),
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _get_session_token(self) -> str:
        response = requests.post(
            self.config.login_url,
            data={"username": self.config.username, "password": self.config.password},
            headers={"X-Application": self.config.api_key, "Accept": "application/json"},
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

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _call_market_catalogue_api(self, from_utc: str, to_utc: str) -> List[Dict[str, Any]]:
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
                "EVENT", "MARKET_START_TIME", "RUNNER_DESCRIPTION",
                "EVENT_TYPE", "COMPETITION", "MARKET_DESCRIPTION",
            ],
            "maxResults": "1000",
            "sort": "FIRST_TO_START",
        }
        response = requests.post(
            self.config.api_url + "listMarketCatalogue/",
            json=payload, headers=headers, timeout=30,
        )
        self._handle_session_error(response)
        return response.json()

    def fetch_markets(self, from_utc: str, to_utc: str) -> Iterator[Dict[str, Any]]:
        markets = self._call_market_catalogue_api(from_utc, to_utc)
        logger.info(f"Fetched {len(markets)} markets from API")
        harness = [
            m for m in markets
            if "pace" in (m.get("marketName") or "").lower()
            or "trot" in (m.get("marketName") or "").lower()
        ]
        logger.info(f"Filtered to {len(harness)} harness markets (Pace/Trot) from {len(markets)} total")
        for market in harness:
            yield {
                "market_id":         market.get("marketId"),
                "market_name":       market.get("marketName"),
                "market_start_time": market.get("marketStartTime"),
                "total_matched":     market.get("totalMatched"),
                "runners":           market.get("runners"),
                "event_type_id":     market.get("eventType", {}).get("id"),
                "event_type_name":   market.get("eventType", {}).get("name"),
                "event_id":          market.get("event", {}).get("id"),
                "event_name":        market.get("event", {}).get("name"),
                "event_country":     market.get("event", {}).get("countryCode"),
                "event_timezone":    market.get("event", {}).get("timezone"),
                "event_venue":       market.get("event", {}).get("venue"),
                "event_open_date":   market.get("event", {}).get("openDate"),
                "competition_id":    market.get("competition", {}).get("id"),
                "competition_name":  market.get("competition", {}).get("name"),
                "betting_type":      market.get("description", {}).get("bettingType"),
                "each_way_divisor":  market.get("description", {}).get("eachWayDivisor"),
                "turn_in_play":      market.get("description", {}).get("turnInPlayEnabled"),
                "rules":             market.get("description", {}).get("rules"),
            }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _call_market_book_api(self, market_ids: List[str]) -> List[Dict[str, Any]]:
        headers = {
            "X-Application": self.config.api_key,
            "X-Authentication": self.token,
            "Content-Type": "application/json",
        }
        payload = {
            "marketIds": market_ids,
            "priceProjection": {
                "priceData": ["EX_BEST_OFFERS", "EX_TRADED", "SP_TRADED", "SP_AVAILABLE"],
                "virtualise": False,
            },
        }
        response = requests.post(
            self.config.api_url + "listMarketBook/",
            json=payload, headers=headers, timeout=30,
        )
        self._handle_session_error(response)
        result = response.json()
        if not isinstance(result, list):
            logger.error(f"listMarketBook unexpected response: {result}")
            return []
        return result

    def validate_catalogue(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return MarketCatalogue(**record).model_dump()
        except ValidationError as e:
            logger.error(f"Catalogue validation error: {e}")
            return None

    def _derive_snapshot_type(self, book: Dict[str, Any]) -> str:
        runners = book.get("runners") or []
        if any(r.get("status") in ("WINNER", "LOSER") for r in runners):
            return "POST_RACE"
        if book.get("inplay"):
            return "ONGOING"
        return "PRE_RACE"

    def validate_book(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(record, dict):
            return None
        try:
            return MarketBook(**record).model_dump()
        except ValidationError as e:
            logger.error(f"MarketBook validation error: {e}")
            return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def upload_batch(self, records: List[Dict[str, Any]], key: str):
        body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        self.s3.put_object(
            Bucket=self.config.s3_bucket,
            Key=key, Body=body, ContentType="application/json",
        )
        logger.info(f"Uploaded {len(records)} records → {key}")

    def batch_records(self, iterable: Iterator, batch_size: int) -> Iterator[List]:
        batch = []
        for item in iterable:
            batch.append(item)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def run_catalogue(self, from_utc: str, to_utc: str, today: str, overwrite: bool = False) -> bool:
        prefix = f"betfair/market_catalogue/extracted_date={today}/"
        if not overwrite and self._s3_prefix_has_files(prefix):
            return True
        run_time = datetime.now(self.SYDNEY).strftime("%H-%M")
        success = 0
        validated_stream = (
            r for r in (self.validate_catalogue(rec) for rec in self.fetch_markets(from_utc, to_utc))
            if r is not None
        )
        for i, batch in enumerate(self.batch_records(validated_stream, batch_size=50)):
            for record in batch:
                record["extracted_date"] = today
                record["run_time"]       = run_time
                record["snapshot_type"]  = None
            key = f"{prefix}run_time={run_time}/batch_{i}.json"
            self.upload_batch(batch, key)
            success += len(batch)
        return success > 0

    def load_schedule_from_s3(self, today: str) -> List[tuple]:
        logger.info(f"Loading schedule from S3 for {today}")
        prefix = f"betfair/market_catalogue/extracted_date={today}/"
        try:
            paginator   = self.s3.get_paginator("list_objects_v2")
            all_markets = []
            for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if not obj["Key"].endswith(".json"):
                        continue
                    f = self.s3.get_object(Bucket=self.config.s3_bucket, Key=obj["Key"])
                    for line in f["Body"].read().decode("utf-8").strip().splitlines():
                        try:
                            all_markets.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
            if not all_markets:
                return []
            unique_markets = {m["market_id"]: m for m in all_markets if m.get("market_id")}.values()
            return self._group_and_format(list(unique_markets))
        except Exception as e:
            logger.error(f"Schedule load failed: {e}")
            return []

    def _group_and_format(self, markets: List[Dict]) -> List[tuple]:
        groups = defaultdict(list)
        for m in markets:
            st = m.get("market_start_time")
            if st:
                dt = datetime.fromisoformat(st.replace("Z", "+00:00"))
                groups[dt].append(m["market_id"])
        schedule = []
        for dt in sorted(groups.keys()):
            trigger = dt - timedelta(minutes=5)
            aest    = dt.astimezone(self.SYDNEY).strftime("%H:%M")
            schedule.append((dt, trigger, groups[dt], aest))
        return schedule

    def run_dynamic(self, today: str, from_utc: str, to_utc: str):
        """
        Single-threaded event loop — one listMarketBook API call per tick covers
        ALL active markets simultaneously.

        - Catalogue always queries the full AEST day (from_utc = midnight) so no
          morning races are missed even if the task starts or restarts late.
        - No grace period ceiling: any race whose trigger has arrived is activated
          immediately (CLOSED markets exit on the first poll).
        - Catalogue refresh failures are caught and retried next cycle.
        """
        CATALOGUE_REFRESH_INTERVAL = timedelta(minutes=5)
        POLL_CAP                   = timedelta(minutes=60)  # safety cap per market
        POLL_INTERVAL              = 15                     # seconds between API calls

        # market_id → {start_time, aest_label, poll_count, poll_start}
        active_markets: Dict[str, Dict] = {}
        processed_start_times: set      = set()
        completed_market_ids:  set      = set()   # individual IDs that have exited (CLOSED or cap)
        schedule_cache:        List[tuple] = []
        last_refresh = datetime.min.replace(tzinfo=timezone.utc)

        logger.info(f"Dynamic Mode: Single-loop engine started for {today}")

        while True:
            if _shutdown:
                logger.info("Shutdown received.")
                self._log_metrics()
                break

            if datetime.now(self.SYDNEY).time() > dt_time(23, 59):
                logger.info("Hard stop at midnight AEST.")
                self._log_metrics()
                break

            now_utc  = datetime.now(timezone.utc)
            run_time = datetime.now(self.SYDNEY).strftime("%H-%M-%S")

            # 1. REFRESH CATALOGUE + SCHEDULE every 5 min
            #    Always uses day_start as from_utc so late restarts never miss
            #    races that were scheduled earlier in the day.
            if (now_utc - last_refresh) >= CATALOGUE_REFRESH_INTERVAL:
                try:
                    ok = self.run_catalogue(from_utc, to_utc, today, overwrite=True)
                    if not ok:
                        logger.warning("Catalogue returned no markets — retaining cached schedule.")
                    new_schedule = self.load_schedule_from_s3(today)
                    if new_schedule:
                        schedule_cache = new_schedule
                    else:
                        logger.warning("Schedule load returned empty — retaining cached schedule.")
                    last_refresh = now_utc
                except Exception as e:
                    logger.error(f"Catalogue refresh failed: {e} — retaining cached schedule.")

            # 2. ACTIVATE markets whose T-5 trigger has arrived
            #    No upper grace period — trigger_time <= now covers any late restart.
            #    Already-CLOSED markets exit on the very first poll.
            for start_time, trigger_time, market_ids, aest_label in schedule_cache:
                if (
                    start_time not in processed_start_times
                    and now_utc >= trigger_time
                ):
                    for mid in market_ids:
                        if mid not in active_markets and mid not in completed_market_ids:
                            active_markets[mid] = {
                                "start_time": start_time,
                                "aest_label": aest_label,
                                "poll_count": 0,
                                "poll_start": now_utc,
                            }
                            logger.info(f"Activated {aest_label} → market {mid}")

            # 3. POLL all active markets — chunk at 10 to stay under Betfair TOO_MUCH_DATA limit
            BETFAIR_BATCH_LIMIT = 10
            if active_markets:
                all_ids   = list(active_markets.keys())
                chunks    = [all_ids[i:i + BETFAIR_BATCH_LIMIT] for i in range(0, len(all_ids), BETFAIR_BATCH_LIMIT)]
                api_start = time.time()
                try:
                    books = []
                    for chunk in chunks:
                        books.extend(self._call_market_book_api(chunk) or [])
                    latency = int((time.time() - api_start) * 1000)
                    logger.info(
                        f"Polled {len(all_ids)} markets in {len(chunks)} call(s) "
                        f"({latency} ms) | run_time={run_time}"
                    )

                    valid_books      = []
                    closed_market_ids = set()

                    for book in (books if isinstance(books, list) else []):
                        mid = book.get("marketId")
                        if mid not in active_markets:
                            continue
                        validated = self.validate_book(book)
                        if validated:
                            validated["extracted_date"] = today
                            validated["run_time"]       = run_time
                            validated["snapshot_type"]  = self._derive_snapshot_type(book)
                            validated["ingested_at"]    = now_utc.isoformat()
                            validated["api_latency_ms"] = latency
                            valid_books.append(validated)
                            active_markets[mid]["poll_count"] += 1

                        if book.get("status") == "CLOSED":
                            closed_market_ids.add(mid)

                    # Upload grouped by snapshot_type so S3 partition matches the data
                    by_type: Dict[str, List] = {}
                    for vb in valid_books:
                        by_type.setdefault(vb["snapshot_type"], []).append(vb)
                    for snap_type, records in by_type.items():
                        key = (
                            f"betfair/market_book/extracted_date={today}"
                            f"/snapshot_type={snap_type}/run_time={run_time}/batch_0.json"
                        )
                        self.upload_batch(records, key)
                        self.metrics["total_snapshots"] += len(records)

                    # Remove CLOSED markets; mark start_time done when all its markets close
                    for mid in closed_market_ids:
                        if mid not in active_markets:
                            continue
                        info = active_markets.pop(mid)
                        completed_market_ids.add(mid)
                        st   = info["start_time"]
                        # If no markets remain for this start_time → it's fully done
                        if not any(i["start_time"] == st for i in active_markets.values()):
                            processed_start_times.add(st)
                            self.metrics["clean_exits"] += 1
                            logger.info(f"{info['aest_label']} fully CLOSED ✓")

                except Exception as e:
                    self.metrics["api_errors"] += 1
                    logger.error(f"Poll error: {e}")

            # 4. SAFETY CAP — remove any market polling longer than 60 min
            cap_expired = [
                mid for mid, info in active_markets.items()
                if (now_utc - info["poll_start"]) > POLL_CAP
            ]
            for mid in cap_expired:
                info = active_markets.pop(mid)
                completed_market_ids.add(mid)
                self.metrics["cap_exits"] += 1
                logger.warning(
                    f"Safety cap: {info['aest_label']} market {mid} removed after 60 min"
                )
                st = info["start_time"]
                if not any(i["start_time"] == st for i in active_markets.values()):
                    processed_start_times.add(st)

            # 5. STATUS
            future = [
                item for item in schedule_cache
                if item[1] > now_utc
                and item[0] not in processed_start_times
                and not any(i["start_time"] == item[0] for i in active_markets.values())
            ]
            if active_markets:
                labels = {info["aest_label"] for info in active_markets.values()}
                nxt    = future[0][3] if future else "none"
                logger.info(f"Active: {sorted(labels)} | Next trigger: {nxt}")
            elif future:
                wait = (future[0][1] - now_utc).total_seconds()
                logger.info(f"Idle — next: {future[0][3]} AEST in {wait / 60:.1f} min")

            # 6. TERMINATION
            if not active_markets and not future:
                logger.info("FINISHED: All races complete. Closing pipeline for the day.")
                self._log_metrics()
                break

            time.sleep(POLL_INTERVAL)

    def _log_metrics(self):
        elapsed = datetime.now(timezone.utc) - self.metrics["start_time"]
        logger.info(
            f"Pipeline complete | "
            f"Snapshots: {self.metrics['total_snapshots']} | "
            f"Clean exits: {self.metrics['clean_exits']} | "
            f"Cap exits: {self.metrics['cap_exits']} | "
            f"API errors: {self.metrics['api_errors']} | "
            f"Duration: {str(elapsed).split('.')[0]}"
        )

    def _send_alert(self, subject: str, message: str):
        if self.sns and self.config.sns_topic_arn:
            try:
                self.sns.publish(
                    TopicArn=self.config.sns_topic_arn, Subject=subject, Message=message
                )
            except Exception as e:
                logger.error(f"Failed to send SNS alert: {e}")

    def run(self, mode: str, from_utc: str, to_utc: str, today: str, overwrite: bool = False):
        try:
            if mode == "catalogue":
                self.run_catalogue(from_utc, to_utc, today, overwrite)
            elif mode == "dynamic":
                self.run_dynamic(today, from_utc, to_utc)
            elif mode == "all":
                if self.run_catalogue(from_utc, to_utc, today, overwrite):
                    logger.info("Discovery complete. Entering dynamic execution...")
                else:
                    logger.warning("No catalogue found. Starting dynamic mode anyway...")
                self.run_dynamic(today, from_utc, to_utc)
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
    target = (
        datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=SYDNEY)
        if args.date
        else datetime.now(SYDNEY)
    )

    today     = target.strftime("%Y-%m-%d")
    day_start = target.replace(hour=0,  minute=0,  second=0 ).astimezone(timezone.utc)
    day_end   = target.replace(hour=23, minute=59, second=59).astimezone(timezone.utc)

    pipeline = BetfairPipeline(config)
    pipeline.run(
        args.mode,
        day_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        day_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        today,
        args.overwrite,
    )
