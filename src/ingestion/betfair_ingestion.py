import os
import requests
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Iterator, Generator, Optional, Tuple, Union, List,Any
from pydantic import BaseModel
from dotenv import load_dotenv
from dataclasses import dataclass

from requests import Session
from tenacity import RetryError, retry_if_exception_type, stop_after_attempt, retry

# -------------------logging---------------


logging.basicConfig(
    level = logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# _________________________Dataclasses-----------

@dataclass(frozen=True)
class BetfairConfig:
    api_key: str
    api_url: str
    login_url: str
    username: str
    password: str

# ------------------------defining .env variables------------
load_dotenv("Betfair.env")

raw_env_var = {"BETFAIR_APP_KEY": os.getenv("BETFAIR_APP_KEY"), "API_URL": os.getenv("API_URL"), "LOGIN_URL": os.getenv("LOGIN_URL"), "BETFAIR_USERNAME": os.getenv("BETFAIR_USERNAME"),"BETFAIR_PASSWORD": os.getenv("BETFAIR_PASSWORD")}
# Dynamic Check: Identify exactly what is missing

missing_var = [k for k, v in raw_env_var.items() if not v]
if missing_var:
    error_msg = f"Missing environment variables: {','.join(missing_var)}"
    logger.error(error_msg)
    raise ValueError(error_msg)

betfair_Config = BetfairConfig(
    api_key = raw_env_var["BETFAIR_APP_KEY"],
    api_url = raw_env_var["API_URL"],
    login_url = raw_env_var["LOGIN_URL"],
    username = raw_env_var["BETFAIR_USERNAME"],
    password = raw_env_var["BETFAIR_PASSWORD"],


)
logger.info("Betfair configuration loaded successfully (credentials hidden).")


# -------------------------get_session_tokens---------------

def get_session_token(config: BetfairConfig) -> str:
    response = requests.post(
        f'{config.login_url}',
        data = {'username': config.username, 'password': config.password},
        headers= {"X-Application": raw_env_var["BETFAIR_APP_KEY"], "Accept": "application/json"}

    )

    response.raise_for_status()   #--------------------to catch HTTP errors.-----
    token_extract = response.json()
    if token_extract.get("status") != "SUCCESS":
        logger.error(f"Betfair_Login failed: {token_extract}")
        raise ValueError(f"Betfair_Login failed: {token_extract}")
    token = token_extract["token"]
    logger.info(f"Betfair_Login successfully retrieved token")
    return token

#--------------------Pydantic Validation===================

# class Betfair_market(BaseModel):
#
#
#
# #=========================Market Extraction ================


def get_market_stream(
    config: BetfairConfig,
    token: str,
    endpoint: str,
    payload: dict[str, Any]
) -> Generator[dict[str, Any], None, None]:

    headers = {
        "X-Application": config.api_key,
        "X-Authentication": token,
        "Content-Type": "application/json",
    }

    response = requests.post(
        config.api_url + endpoint,
        json=payload,
        headers=headers,
        timeout=30
    )

    if not response.ok:
        raise RuntimeError(
            f"Betfair API failed {response.status_code}: {response.text}"
        )

    data = response.json()

    for record in data:
        yield record

# --- STEP 1: LIST TODAY'S AU WIN MARKETS ---

def get_market_stream_Catalogue(config: BetfairConfig, token: str, from_utc: Any, to_utc: Any)-> Generator[dict[str, Any], None, None]:

    payload = {
        "filter": {
            "eventTypeIds": ["7"],  # 7 = Horse Racing
            "marketCountries": ["AU"],
            "marketTypeCodes": ["WIN"],
            "marketStartTime": {
                "from": from_utc,
                "to": to_utc
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
    yield from get_market_stream(config, token, "listMarketCatalogue/", payload)


# --- STEP 2: GET ODDS + BSP ---
def get_market_book(config: BetfairConfig, token: str, market_ids: list[str])-> Generator[dict[str, Any], None, None]:
    payload = {
        "marketIds": market_ids,
        "priceProjection": {
            "priceData": ["EX_BEST_OFFERS", "SP_TRADED"],
            "virtualise": False
        }
    }
    yield from get_market_stream(config, token, "listMarketBook/", payload)




SYDNEY = ZoneInfo("Australia/Sydney")   # handles AEST/AEDT automatically
now_aest = datetime.now(SYDNEY)
today    = now_aest.strftime("%Y-%m-%d")
day_start_utc = now_aest.replace(hour=0,  minute=0,  second=0,  microsecond=0).astimezone(timezone.utc)
day_end_utc   = now_aest.replace(hour=23, minute=59, second=59, microsecond=0).astimezone(timezone.utc)
from_utc = day_start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
to_utc   = day_end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")




if __name__ == "__main__":
    token = get_session_token(betfair_Config)

    market_catalogue = list(
        get_market_stream_Catalogue(
            betfair_Config,
            token,
            from_utc,
            to_utc
        )
    )

    logger.info(f"Total markets received: {len(market_catalogue)}")

    for market in market_catalogue[:3]:
        print(market) # test commit

    # -------- Extract market IDs --------
    market_ids = [m["marketId"] for m in market_catalogue]

    # -------- Get market book --------
    market_book = list(
        get_market_book(
            betfair_Config,
            token,
            market_ids[:5]   # keep small for testing
        )
    )

    logger.info(f"Total market books received: {len(market_book)}")

    for book in market_book[:3]:
        print(book)
