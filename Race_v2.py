import requests
import psycopg2
from datetime import date, timedelta
import time

API_KEY = "fk_4413b47e6c96b47dd79861424402006f3bd6a466bdcefbb7748af80c271db8e7"
HEADERS = {"X-API-Key": API_KEY}
BASE_URL = "https://api.formfav.com/v1"

# --- TRACK CODE MAPPING ---
TRACK_CODES = {
    # Victoria
    "flemington":           "FLM",
    "caulfield":            "CAU",
    "caulfield-heath":      "CAH",
    "moonee-valley":        "MVM",
    "sandown":              "SAN",
    "ballarat":             "BAL",
    "bendigo":              "BEN",
    "geelong":              "GEE",
    "cranbourne":           "CRN",
    "pakenham":             "PAK",
    "mornington":           "MOR",
    "sale":                 "SAL",
    "echuca":               "ECH",
    "seymour":              "SEY",
    "hamilton":             "HAM",
    "warrnambool":          "WBL",
    "horsham":              "HOR",
    "wodonga":              "WOD",
    "stawell":              "STW",
    "swan-hill":            "SWH",
    "yarra-valley":         "YVL",
    "kilmore":              "KIL",
    "bairnsdale":           "BAI",
    "wangaratta":           "WAN",
    "ararat":               "ARA",
    "terang":               "TER",
    "stony-creek":          "STC",
    "kyneton":              "KYN",
    "corowa":               "COR",
    "towong":               "TOW",
    "colac":                "COL",
    # New South Wales
    "randwick":             "RAN",
    "randwick-kensington":  "RNK",
    "rosehill":             "ROS",
    "warwick-farm":         "WFM",
    "kembla-grange":        "KEM",
    "newcastle":            "NEW",
    "gosford":              "GOS",
    "hawkesbury":           "HAW",
    "wyong":                "WYO",
    "albury":               "ALB",
    "wagga":                "WAG",
    "dubbo":                "DUB",
    "orange":               "ORA",
    "bathurst":             "BAT",
    "goulburn":             "GOU",
    "canberra":             "CAN",
    "scone":                "SCO",
    "tamworth":             "TAM",
    "taree":                "TAR",
    "grafton":              "GRF",
    "coffs-harbour":        "COF",
    "port-macquarie":       "PMA",
    "ballina":              "BNA",
    "moree":                "MOE",
    "gunnedah":             "GUN",
    "armidale":             "ARM",
    "sapphire-coast":       "SAP",
    "nowra":                "NOW",
    "queanbeyan":           "QBN",
    "cowra":                "COW",
    "muswellbrook":         "MUS",
    "warren":               "WRN",
    # Queensland
    "eagle-farm":           "EGF",
    "doomben":              "DOO",
    "gold-coast":           "GCO",
    "sunshine-coast":       "SNC",
    "ipswich":              "IPS",
    "toowoomba":            "TOO",
    "rockhampton":          "ROC",
    "townsville":           "TVL",
    "cairns":               "CAI",
    "mackay":               "MAC",
    "dalby":                "DAL",
    "warwick":              "WAK",
    "kilcoy":               "KLC",
    "beaudesert":           "BDS",
    "gatton":               "GAT",
    "bundaberg":            "BUN",
    "thangool":             "THA",
    "innisfail":            "INN",
    # South Australia
    "morphettville":        "MPV",
    "morphettville-parks":  "MRP",
    "balaklava":            "BLK",
    "mount-gambier":        "MGR",
    "murray-bridge":        "MBR",
    "naracoorte":           "NAR",
    "strathalbyn":          "STR",
    "oakbank":              "OAK",
    "port-lincoln":         "PLN",
    # Western Australia
    "ascot":                "ASC",
    "belmont":              "BEL",
    "pinjarra":             "PIN",
    "pinjarra-scarpside":   "PNS",
    "bunbury":              "BUR",
    "geraldton":            "GER",
    "albany":               "ALN",
    "york":                 "YRK",
    "esperance":            "ESP",
    "pingrup":              "PGP",
    # Tasmania
    "launceston":           "LAU",
    "hobart":               "HOB",
    # Northern Territory
    "darwin":               "DAR",
}

def make_race_code(track, race_date, race_num):
    """Generate race_id like FLM1501202507 and meeting_id like FLM15012025"""
    track_code = TRACK_CODES.get(track.lower(), track[:3].upper())
    day        = race_date.strftime("%d")
    month      = race_date.strftime("%m")
    year       = race_date.strftime("%Y")
    num        = str(race_num).zfill(2)
    race_id    = f"{track_code}{day}{month}{year}{num}"
    meeting_id = f"{track_code}{day}{month}{year}"
    return race_id, meeting_id

# --- CONNECT TO POSTGRES ---
conn = psycopg2.connect(
    host="localhost",
    dbname="Project_Racing",
    user="nageswarchowdarypotluri",
    password="",
    port=5432
)
cur = conn.cursor()

# --- DATE RANGE (last 1 year) ---
end_date   = date.today()
start_date = end_date - timedelta(days=365)
date_range = [start_date + timedelta(days=i)
              for i in range((end_date - start_date).days + 1)]

total_inserted = 0

for race_date in date_range:
    date_str = race_date.strftime("%Y-%m-%d")

    # Step 1 — get meetings for this date
    try:
        meetings_resp = requests.get(
            f"{BASE_URL}/form/meetings",
            params={"date": date_str, "race_code": "gallops"},
            headers=HEADERS,
            timeout=10
        )
        if meetings_resp.status_code != 200:
            continue
        meetings = meetings_resp.json().get("meetings", [])
    except Exception as e:
        print(f"{date_str} — meetings error: {e}")
        continue

    # Step 2 — loop meetings
    for meeting in meetings:
        track_slug = meeting.get("slug") or meeting.get("track")

        # Step 3 — loop race numbers
        for race_num in range(1, 11):
            try:
                race_resp = requests.get(
                    f"{BASE_URL}/form",
                    params={
                        "date": date_str,
                        "track": track_slug,
                        "race": str(race_num),
                        "race_code": "gallops",
                        "country": "au"
                    },
                    headers=HEADERS,
                    timeout=10
                )

                if race_resp.status_code != 200:
                    break

                d = race_resp.json()

                # Generate race_id and meeting_id
                race_id, meeting_id = make_race_code(track_slug, race_date, race_num)

                # Parse prize money
                prize_raw = d.get("prizeMoney")
                prize = float(prize_raw.replace("$", "").replace(",", "").strip()) if prize_raw else None

                # Parse distance
                dist_raw = d.get("distance")
                distance = int(dist_raw.replace("m", "")) if dist_raw else None

                cur.execute("""
                    INSERT INTO "Racing".race (
                        race_id,
                        race_name,
                        short_race_name,
                        meeting_id,
                        venue,
                        race_order,
                        race_class,
                        scheduled_date,
                        race_status,
                        distance_meters,
                        race_condition,
                        field_size,
                        prize_total,
                        record_created_date,
                        extracted_at
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                    ON CONFLICT (race_id) DO NOTHING
                """, (
                    race_id,
                    d.get("raceName"),
                    d.get("raceName", "")[:100],
                    meeting_id,
                    track_slug,
                    race_num,
                    d.get("raceClass"),
                    d.get("date"),
                    "completed",
                    distance,
                    d.get("condition"),
                    d.get("numberOfRunners"),
                    prize
                ))

                conn.commit()
                total_inserted += 1
                print(f"  ✓ {race_id} | {d.get('raceName')} | {track_slug}")

                time.sleep(0.3)

            except Exception as e:
                conn.rollback()
                print(f"  ✗ {date_str} | {track_slug} | Race {race_num} — error: {e}")
                continue

print(f"\nDone. {total_inserted} races inserted.")

cur.close()
conn.close()
