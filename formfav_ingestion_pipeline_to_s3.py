import os
import json
import time
import logging
import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Iterator, Dict, Any, Optional, List

import requests
import boto3
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential

PIPELINE_VERSION = "1.0.0"

# -------------------- LOGGING --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# -------------------- EXCEPTIONS --------------------
class FormFavAPIError(Exception):
    """Raised when a FormFav API call returns a non-200 status after all retries."""

class S3UploadError(Exception):
    """Raised when an S3 put_object call fails after all retries."""


# -------------------- CONFIG --------------------
# frozen=True prevents accidental mutation of credentials mid-pipeline.
# rate_limit_delay throttles race API calls — FormFav rate-limits heavy polling.
@dataclass(frozen=True)
class FormFavConfig:
    api_key: str
    base_url: str
    s3_bucket: str
    aws_access_key: str
    aws_secret_key: str
    region: str = "ap-southeast-2"
    rate_limit_delay: float = 0.3   # seconds between consecutive race API calls
    sns_topic_arn: Optional[str] = None


load_dotenv(os.path.join(os.path.dirname(__file__), "FormFav_API.env"))
load_dotenv(os.path.join(os.path.dirname(__file__), "aws_s3.env"))

config = FormFavConfig(
    api_key=os.getenv("API_KEY"),
    base_url=os.getenv("BASE_URL"),
    s3_bucket="project-racing-bronze",
    aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    sns_topic_arn=os.getenv("SNS_TOPIC_ARN"),
)

required_vars = ["API_KEY", "BASE_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
missing = [var for var in required_vars if not os.getenv(var)]
if missing:
    raise ValueError(f"Missing required environment variables: {missing}")


# -------------------- LOOKUP TABLES --------------------
# Country codes → full names for readable downstream reporting.
COUNTRY_NAMES: Dict[str, str] = {
    "au": "Australia",      "nz": "New Zealand",         "ae": "United Arab Emirates",
    "hk": "Hong Kong",      "sg": "Singapore",           "jp": "Japan",
    "gb": "Great Britain",  "ie": "Ireland",             "fr": "France",
    "us": "United States",  "za": "South Africa",        "ca": "Canada",
}

# Track slug → Australian state, used to partition races by state in Silver/Gold.
AU_TRACK_STATE: Dict[str, str] = {
    # New South Wales
    "albury": "NSW",             "armidale": "NSW",          "ballina": "NSW",
    "bankstown": "NSW",          "bathurst": "NSW",           "berrigan": "NSW",
    "broken-hill": "NSW",        "carrathool": "NSW",         "dubbo": "NSW",
    "gosford": "NSW",            "goulburn": "NSW",           "grafton": "NSW",
    "hawkesbury": "NSW",         "junee": "NSW",              "kembla-grange": "NSW",
    "leeton": "NSW",             "lightning-ridge": "NSW",    "maitland": "NSW",
    "menangle": "NSW",           "moree": "NSW",              "moruya": "NSW",
    "mudgee": "NSW",             "muswellbrook": "NSW",       "narrabri": "NSW",
    "newcastle": "NSW",          "nowra": "NSW",              "orange": "NSW",
    "parkes": "NSW",             "penrith": "NSW",            "port-macquarie": "NSW",
    "queanbeyan": "NSW",         "randwick": "NSW",           "randwick-kensington": "NSW",
    "rosehill": "NSW",           "sapphire-coast": "NSW",     "scone": "NSW",
    "tamworth": "NSW",           "taree": "NSW",              "temora": "NSW",
    "tocumwal": "NSW",           "tuncurry": "NSW",           "wagga": "NSW",
    "warren": "NSW",             "warwick-farm": "NSW",       "wyong": "NSW",
    "young": "NSW",
    # Victoria
    "ararat": "VIC",             "avoca": "VIC",              "bairnsdale": "VIC",
    "ballarat": "VIC",           "bendigo": "VIC",            "camperdown": "VIC",
    "caulfield": "VIC",          "caulfield-heath": "VIC",    "charlton": "VIC",
    "cranbourne": "VIC",         "echuca": "VIC",             "flemington": "VIC",
    "geelong": "VIC",            "hamilton": "VIC",           "healesville": "VIC",
    "kerang": "VIC",             "kilmore": "VIC",            "kyneton": "VIC",
    "melton": "VIC",             "mildura": "VIC",            "mornington": "VIC",
    "pakenham": "VIC",           "pioneer-park": "VIC",       "sale": "VIC",
    "sandown": "VIC",            "shepparton": "VIC",         "stawell": "VIC",
    "swan-hill": "VIC",          "terang": "VIC",             "wangaratta": "VIC",
    "warracknabeal": "VIC",      "warrnambool": "VIC",        "werribee": "VIC",
    "yarra-valley": "VIC",
    # Queensland
    "atherton": "QLD",           "augathella": "QLD",         "barcaldine": "QLD",
    "beaudesert": "QLD",         "bundaberg": "QLD",          "cairns": "QLD",
    "charleville": "QLD",        "cloncurry": "QLD",          "dalby": "QLD",
    "doomben": "QLD",            "eagle-farm": "QLD",         "emerald": "QLD",
    "flinton": "QLD",            "gladstone": "QLD",          "gold-coast": "QLD",
    "gold-coast-polytrack": "QLD", "ipswich": "QLD",          "kilcoy": "QLD",
    "longreach": "QLD",          "mackay": "QLD",             "marburg": "QLD",
    "maryborough": "QLD",        "moranbah": "QLD",           "mount-isa": "QLD",
    "nanango": "QLD",            "redcliffe": "QLD",          "rockhampton": "QLD",
    "roma": "QLD",               "springsure": "QLD",         "sunshine-coast": "QLD",
    "sunshine-coast-polytrack": "QLD", "toowoomba": "QLD",   "townsville": "QLD",
    "warwick": "QLD",
    # South Australia
    "balaklava": "SA",           "clare": "SA",               "gawler": "SA",
    "globe-derby": "SA",         "morphettville": "SA",       "morphettville-parks": "SA",
    "mount-gambier": "SA",       "murray-bridge": "SA",       "oakbank": "SA",
    "penola": "SA",              "port-lincoln": "SA",        "port-pirie": "SA",
    "strathalbyn": "SA",         "streaky-bay": "SA",         "victor-harbor": "SA",
    # Western Australia
    "albany": "WA",              "ascot": "WA",               "belmont": "WA",
    "bridgetown": "WA",          "bunbury": "WA",             "dongara": "WA",
    "geraldton": "WA",           "gloucester-park": "WA",     "kalgoorlie": "WA",
    "narrogin": "WA",            "northam": "WA",             "pinjarra": "WA",
    "pinjarra-scarpside": "WA",  "wagin": "WA",               "york": "WA",
    # Tasmania
    "burnie": "TAS",             "hobart": "TAS",             "launceston": "TAS",
    # Northern Territory
    "alice-springs": "NT",       "darwin": "NT",
    # Australian Capital Territory
    "canberra": "ACT",
}


# -------------------- PYDANTIC SCHEMAS --------------------
# Meeting validates one venue's metadata for a race day.
# track_slug, meeting_date, race_code are required — they are the composite key
# used to fetch race data and partition S3. Records missing any of these go to quarantine.
# distance_raw and prize_money_raw stay as raw strings (e.g. "1200m", "$35,000")
# and are parsed into numeric types in the Silver layer.
class Meeting(BaseModel):
    model_config = {"extra": "ignore"}

    track_slug:     str
    meeting_date:   str
    race_code:      str

    name:           Optional[str] = None
    country:        Optional[str] = None
    country_name:   Optional[str] = None
    state:          Optional[str] = None
    number_of_races: Optional[int] = None
    track_condition: Optional[str] = None
    weather:        Optional[str] = None


# Race validates one individual race at a venue.
# track_slug, race_date, race_number are required — composite key for downstream joins.
# runners stays as a nested list (Bronze layer) and is exploded into rows in Silver.
class Race(BaseModel):
    model_config = {"extra": "ignore"}

    track_slug:     str
    race_date:      str
    race_number:    int

    race_code:          Optional[str] = None
    race_name:          Optional[str] = None
    distance_raw:       Optional[str] = None
    condition:          Optional[str] = None
    race_class:         Optional[str] = None
    prize_money_raw:    Optional[str] = None
    number_of_runners:  Optional[int] = None
    country:            Optional[str] = None
    country_name:       Optional[str] = None
    state:              Optional[str] = None
    runners:            Optional[List[Dict[str, Any]]] = None


# -------------------- PIPELINE --------------------
class FormFavPipeline:

    SYDNEY = ZoneInfo("Australia/Sydney")

    def __init__(self, config: FormFavConfig):
        self.config = config
        self.headers = {"X-API-Key": config.api_key}

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key,
            aws_secret_access_key=config.aws_secret_key,
            region_name=config.region,
        )

        self.sns = (
            boto3.client(
                "sns",
                aws_access_key_id=config.aws_access_key,
                aws_secret_access_key=config.aws_secret_key,
                region_name=config.region,
            )
            if config.sns_topic_arn else None
        )

    # -------------------- IDEMPOTENCY --------------------
    def _s3_prefix_has_files(self, prefix: str) -> bool:
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=prefix):
            if page.get("Contents"):
                return True
        return False

    # -------------------- ALERTING --------------------
    def _send_alert(self, subject: str, message: str):
        if not self.sns or not self.config.sns_topic_arn:
            logger.info("Alerting not configured — skipping SNS")
            return
        try:
            self.sns.publish(
                TopicArn=self.config.sns_topic_arn,
                Subject=subject,
                Message=message,
            )
            logger.info("Alert sent via SNS")
        except Exception as e:
            logger.error(f"Failed to send SNS alert: {e}")

    # -------------------- QUARANTINE --------------------
    def _quarantine_record(self, record: Dict[str, Any], error: str, data_type: str, date_str: str):
        quarantine_record = {
            **record,
            "_quarantine_reason": error,
            "_quarantine_at":     datetime.now(timezone.utc).isoformat(),
            "_data_type":         data_type,
        }
        ts  = datetime.now(timezone.utc).strftime("%H%M%S%f")
        key = f"quarantine/formfav/{data_type}/extracted_date={date_str}/failed_{ts}.json"
        try:
            self.s3.put_object(
                Bucket=self.config.s3_bucket,
                Key=key,
                Body=json.dumps(quarantine_record).encode("utf-8"),
                ContentType="application/json",
            )
            logger.warning(f"Quarantined bad record → s3://{self.config.s3_bucket}/{key}")
        except Exception as e:
            logger.error(f"Failed to quarantine record: {e}")

    # -------------------- MEETINGS API --------------------
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=2, max=10),
        before_sleep=lambda rs: logger.warning(
            f"Meetings API failed — retrying... attempt {rs.attempt_number}"
        ),
    )
    def _call_meetings_api(self, date_str: str, race_code: str) -> List[Dict[str, Any]]:
        response = requests.get(
            f"{self.config.base_url}/form/meetings",
            params={"date": date_str, "race_code": race_code},
            headers=self.headers,
            timeout=15,
        )
        if response.status_code != 200:
            raise FormFavAPIError(
                f"Meetings API failed [{race_code} {date_str}]: "
                f"{response.status_code} — {response.text}"
            )
        return response.json().get("meetings", [])

    # -------------------- RACE API --------------------
    # 4xx → return None so the caller stops fetching races for this venue.
    # 5xx → raise FormFavAPIError so tenacity retries — server errors are transient.
    @retry(
        stop=stop_after_attempt(2),          # reduced from 3 → 2 retries
        wait=wait_exponential(min=1, max=5), # reduced backoff — fail fast
        before_sleep=lambda rs: logger.warning(
            f"Race API failed — retrying... attempt {rs.attempt_number}"
        ),
    )
    def _call_race_api(
        self, date_str: str, track_slug: str, race_num: int, race_code: str
    ) -> Optional[Dict[str, Any]]:
        response = requests.get(
            f"{self.config.base_url}/form",
            params={
                "date":      date_str,
                "track":     track_slug,
                "race":      str(race_num),
                "race_code": race_code,
                "country":   "au",
            },
            headers=self.headers,
            timeout=10,                      # reduced from 30s → 10s per request
        )
        if 400 <= response.status_code < 500:
            return None  # no race at this number — caller breaks the race loop
        if response.status_code != 200:
            raise FormFavAPIError(
                f"Race API failed [{track_slug} R{race_num} {date_str}]: "
                f"{response.status_code} — {response.text}"
            )
        return response.json()

    # -------------------- MEETINGS GENERATOR --------------------
    # Calls the meetings API and yields one flat dict per venue.
    # country_name and state are enriched here from lookup tables so they
    # land in Bronze and are available downstream without re-joining.
    def fetch_meetings(self, date_str: str, race_code: str) -> Iterator[Dict[str, Any]]:
        meetings = self._call_meetings_api(date_str, race_code)
        logger.info(f"Fetched {len(meetings)} {race_code} meetings for {date_str}")
        for m in meetings:
            track_slug = m.get("slug") or m.get("track")
            country    = m.get("country", "")
            yield {
                "track_slug":      track_slug,
                "meeting_date":    date_str,
                "race_code":       race_code,
                "name":            m.get("name"),
                "country":         country,
                "country_name":    COUNTRY_NAMES.get(country, country.upper() if country else None),
                "state":           AU_TRACK_STATE.get(track_slug) if country == "au" else None,
                "number_of_races": m.get("numberOfRaces"),
                "track_condition": m.get("trackCondition"),
                "weather":         m.get("weather"),
            }

    # -------------------- RACES GENERATOR --------------------
    # Iterates every meeting, polling race numbers 1–10.
    # Stops at a venue when the API returns None (4xx — no race at that number).
    # rate_limit_delay is applied after every successful race fetch to avoid
    # hitting FormFav's per-second request limit across a full day of venues.
    def fetch_races(
        self, date_str: str, race_code: str, meetings: List[Dict[str, Any]]
    ) -> Iterator[Dict[str, Any]]:
        for meeting in meetings:
            track_slug   = meeting.get("track_slug")
            country      = meeting.get("country", "")
            country_name = meeting.get("country_name")
            state        = meeting.get("state")

            if not track_slug:
                logger.warning(f"Meeting missing track_slug — skipping: {meeting}")
                continue

            num_races = meeting.get("number_of_races") or 10  # use known count, cap at 10
            for race_num in range(1, num_races + 1):
                try:
                    data = self._call_race_api(date_str, track_slug, race_num, race_code)
                except FormFavAPIError as e:
                    logger.error(f"Race API permanently failed for {track_slug} R{race_num}: {e}")
                    break

                if data is None:
                    break  # no more races at this venue

                yield {
                    "track_slug":       track_slug,
                    "race_date":        date_str,
                    "race_number":      race_num,
                    "race_code":        race_code,
                    "race_name":        data.get("raceName"),
                    "distance_raw":     data.get("distance"),
                    "condition":        data.get("condition"),
                    "race_class":       data.get("raceClass"),
                    "prize_money_raw":  data.get("prizeMoney"),
                    "number_of_runners": data.get("numberOfRunners"),
                    "country":          country,
                    "country_name":     country_name,
                    "state":            state,
                    "runners":          data.get("runners"),   # nested — exploded in Silver
                }

                time.sleep(self.config.rate_limit_delay)

    # -------------------- VALIDATION --------------------
    def validate_meeting(self, record: Dict[str, Any], date_str: str) -> Optional[Dict[str, Any]]:
        try:
            return Meeting(**record).model_dump()
        except ValidationError as e:
            self._quarantine_record(record, str(e), "meetings", date_str)
            return None

    def validate_race(self, record: Dict[str, Any], date_str: str) -> Optional[Dict[str, Any]]:
        try:
            return Race(**record).model_dump()
        except ValidationError as e:
            self._quarantine_record(record, str(e), "races", date_str)
            return None

    # -------------------- BATCHING --------------------
    def batch_records(self, iterator: Iterator[Dict[str, Any]], batch_size: int = 50):
        batch = []
        for record in iterator:
            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    # -------------------- S3 UPLOAD --------------------
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=2, max=10),
        before_sleep=lambda rs: logger.warning(
            f"S3 upload failed — retrying... attempt {rs.attempt_number}"
        ),
    )
    def upload_batch(self, records: List[Dict[str, Any]], key: str):
        now_utc  = datetime.now(timezone.utc).isoformat()
        enriched = [
            {
                **r,
                "_ingested_at":      now_utc,
                "_source_system":    "formfav_api",
                "_pipeline_version": PIPELINE_VERSION,
            }
            for r in records
        ]
        body = "\n".join(json.dumps(r) for r in enriched).encode("utf-8")
        try:
            self.s3.put_object(
                Bucket=self.config.s3_bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
        except Exception as e:
            raise S3UploadError(f"S3 upload failed for {key}: {e}") from e
        logger.info(f"Uploaded {len(records)} records → s3://{self.config.s3_bucket}/{key}")

    # -------------------- RUN DATE --------------------
    # Processes one race_code (gallops or harness) for one calendar date.
    # Meetings are fetched first and held in memory (small list, typically <20 venues)
    # so the same list drives both the meetings upload and the race fetch loop.
    # Using raw_meetings (pre-Pydantic) for the race fetch loop means we still
    # attempt to pull races for a venue even if its metadata failed validation.
    def run_date(self, date_str: str, race_code: str, overwrite: bool = False, snapshot_type: str = None) -> bool:
        # Build prefix — includes snapshot_type folder if provided
        snapshot_segment = f"/snapshot_type={snapshot_type}" if snapshot_type else ""
        meetings_prefix = f"formfav/meetings/extracted_date={date_str}/race_code={race_code}{snapshot_segment}/"

        if not overwrite and self._s3_prefix_has_files(meetings_prefix):
            logger.info(
                f"{race_code} {date_str} {snapshot_type or ''} already in S3 — skipping (use --overwrite to re-run)"
            )
            return True

        logger.info(f"Starting {race_code} run for {date_str} | snapshot_type={snapshot_type or 'None'}")
        run_time = datetime.now(self.SYDNEY).strftime("%H-%M")
        meetings_uploaded, races_uploaded, races_failed = 0, 0, 0

        # --- MEETINGS ---
        try:
            raw_meetings = list(self.fetch_meetings(date_str, race_code))
        except FormFavAPIError as e:
            logger.error(f"Meetings API permanently failed [{race_code} {date_str}]: {e}")
            return False

        if not raw_meetings:
            logger.info(f"No {race_code} meetings on {date_str} — skipping")
            return False

        validated_meetings = [
            r
            for r in (self.validate_meeting(m, date_str) for m in raw_meetings)
            if r is not None
        ]

        for i, batch in enumerate(self.batch_records(iter(validated_meetings), batch_size=50)):
            key = (
                f"formfav/meetings/extracted_date={date_str}"
                f"/race_code={race_code}{snapshot_segment}/run_time={run_time}/batch_{i}.json"
            )
            try:
                self.upload_batch(batch, key)
                meetings_uploaded += len(batch)
            except Exception as e:
                logger.error(f"Meetings upload failed after retries: {e}")

        logger.info(
            f"Meetings done | {race_code} {date_str} | Uploaded: {meetings_uploaded}"
        )

        # --- RACES ---
        race_stream = (
            r
            for r in (self.validate_race(rec, date_str) for rec in self.fetch_races(date_str, race_code, raw_meetings))
            if r is not None
        )

        for i, batch in enumerate(self.batch_records(race_stream, batch_size=50)):
            key = (
                f"formfav/races/extracted_date={date_str}"
                f"/race_code={race_code}{snapshot_segment}/run_time={run_time}/batch_{i}.json"
            )
            try:
                self.upload_batch(batch, key)
                races_uploaded += len(batch)
            except Exception as e:
                logger.error(f"Races upload failed after retries: {e}")
                races_failed += len(batch)

        logger.info(
            f"Races done | {race_code} {date_str} | "
            f"Uploaded: {races_uploaded} | Failed: {races_failed}"
        )
        return races_uploaded > 0

    # -------------------- DISPATCHER --------------------
    # Loops every date × race_code combination.
    # Per-date failures are caught, logged, and alerted individually so the loop
    # continues — one bad date never blocks subsequent dates.
    # A RuntimeError is raised at the end if any combinations failed so cron
    # records a non-zero exit code for the overall job.
    def run(self, dates: List[str], race_codes: List[str], overwrite: bool = False, snapshot_type: str = None):
        total_failed = 0
        for date_str in dates:
            for race_code in race_codes:
                try:
                    self.run_date(date_str, race_code, overwrite, snapshot_type)
                except Exception as e:
                    msg = (
                        f"FormFav pipeline failed | {race_code} {date_str} | Error: {e}"
                    )
                    logger.error(msg)
                    self._send_alert(
                        subject=f"FormFav Pipeline FAILED — {race_code} {date_str}",
                        message=msg,
                    )
                    total_failed += 1

        if total_failed:
            raise RuntimeError(
                f"FormFav pipeline: {total_failed} date/race_code combination(s) failed"
            )


# -------------------- ENTRY --------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="FormFav Racing Bronze Pipeline")

    # --mode controls the date range when --date is not provided.
    # today     = today only          (scheduled daily cron at 9:00 AM AEST)
    # default   = last 14 days        (catchup window after downtime)
    # backfill  = last 21 days        (longer recovery or initial load)
    parser.add_argument(
        "--mode",
        choices=["today", "default", "backfill"],
        default="default",
        help="today=today only | default=last 14 days | backfill=last 21 days",
    )
    parser.add_argument(
        "--race-code",
        choices=["gallops", "harness", "all"],
        default="all",
        dest="race_code",
        help="gallops | harness | all (default: all)",
    )
    # --date bypasses --mode and processes a single specific date.
    # Useful for targeted reruns or investigating one day's data quality.
    parser.add_argument(
        "--date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Process a specific date only. Overrides --mode.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Force re-run even if data already exists in S3 for this date.",
    )
    parser.add_argument(
        "--snapshot-type",
        choices=["PRE_RACE", "POST_RACE"],
        default=None,
        dest="snapshot_type",
        help="PRE_RACE=morning run | POST_RACE=night run",
    )
    args = parser.parse_args()

    SYDNEY = ZoneInfo("Australia/Sydney")
    today  = datetime.now(SYDNEY).date()

    if args.date:
        dates = [args.date]
        logger.info(f"Single date mode — processing: {args.date}")
    else:
        if args.mode == "today":
            start = today
        elif args.mode == "backfill":
            start = today - timedelta(days=21)
        else:
            start = today - timedelta(days=14)

        dates = [
            (start + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((today - start).days + 1)
        ]

    race_codes = ["gallops", "harness"] if args.race_code == "all" else [args.race_code]

    logger.info(
        f"Mode: {args.mode} | Race codes: {race_codes} | "
        f"Dates: {dates[0]} → {dates[-1]} ({len(dates)} days) | Overwrite: {args.overwrite}"
    )

    pipeline = FormFavPipeline(config)
    pipeline.run(dates, race_codes, args.overwrite, args.snapshot_type)


    # /Users/nageswarchowdarypotluri/.pyenv/versions/3.11.9/bin/python /Users/nageswarchowdarypotluri/my-de-project/formfav_ingestion_pipeline_to_s3.py --race-code harness




