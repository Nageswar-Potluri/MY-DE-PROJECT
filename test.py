# # import requests
# # import psycopg2
# #
# # # --- 1. FETCH API DATA ---
# # url = "https://api.formfav.com/v1/form"
# # params = {
# #     "date": "2025-01-15",
# #     "track": "flemington",
# #     "race": "7",
# #     "race_code": "gallops",
# #     "country": "au",
# #     "timezone": "Australia/Sydney"
# # }
# # headers = {"X-API-Key": "fk_4413b47e6c96b47dd79861424402006f3bd6a466bdcefbb7748af80c271db8e7"}
# #
# # response = requests.get(url, params=params, headers=headers)
# # data = response.json()
# #
# # # --- 2. PARSE RACE LEVEL ---
# # race_row = (
# #     data["date"],
# #     data["track"],
# #     data["raceNumber"],
# #     data["raceName"],
# #     data["distance"],
# #     data["condition"],
# #     data["weather"],
# #     data["raceClass"],
# #     data["abandoned"],
# #     data["prizeMoney"],
# #     data["numberOfRunners"]
# # )
# #
# # # --- 3. PARSE RUNNER LEVEL ---
# # runner_rows = []
# # for r in data["runners"]:
# #     overall = r["stats"]["overall"] if r["stats"]["overall"] else {}
# #     runner_rows.append((
# #         data["date"],
# #         data["raceNumber"],
# #         r["number"],
# #         r["name"],
# #         r["jockey"],
# #         r["trainer"],
# #         r["weight"],
# #         r["barrier"],
# #         r["age"],
# #         r["form"],
# #         r["scratched"],
# #         r["racingColours"],
# #         r.get("gearChange"),
# #         overall.get("starts"),
# #         overall.get("wins"),
# #         overall.get("places"),
# #         overall.get("winPercent"),
# #         overall.get("placePercent")
# #     ))
# #
# # # --- 4. CONNECT TO POSTGRES ---
# # conn = psycopg2.connect(
# #     host="localhost",
# #     dbname="Project_Racing",
# #     user="nageswarchowdarypotluri",
# #     password="",
# #     port=5432
# # )
# # cur = conn.cursor()
# #
# # # --- 5. CREATE SCHEMA ---
# # cur.execute("CREATE SCHEMA IF NOT EXISTS \"Racing\"")
# #
# # # --- 6. CREATE TABLES ---
# # cur.execute("""
# #     CREATE TABLE IF NOT EXISTS "Racing".race_info (
# #         id              SERIAL PRIMARY KEY,
# #         race_date       DATE,
# #         track           VARCHAR(100),
# #         race_number     INT,
# #         race_name       VARCHAR(200),
# #         distance        VARCHAR(20),
# #         condition       VARCHAR(50),
# #         weather         VARCHAR(50),
# #         race_class      VARCHAR(100),
# #         abandoned       BOOLEAN,
# #         prize_money     VARCHAR(50),
# #         num_runners     INT,
# #         extracted_at    TIMESTAMP DEFAULT NOW()
# #     )
# # """)
# #
# # cur.execute("""
# #     CREATE TABLE IF NOT EXISTS "Racing".race_runners (
# #         id              SERIAL PRIMARY KEY,
# #         race_date       DATE,
# #         race_number     INT,
# #         runner_number   INT,
# #         horse_name      VARCHAR(100),
# #         jockey          VARCHAR(100),
# #         trainer         VARCHAR(100),
# #         weight          FLOAT,
# #         barrier         INT,
# #         age             INT,
# #         form            VARCHAR(50),
# #         scratched       BOOLEAN,
# #         racing_colours  VARCHAR(100),
# #         gear_change     VARCHAR(100),
# #         overall_starts  INT,
# #         overall_wins    INT,
# #         overall_places  INT,
# #         win_percent     FLOAT,
# #         place_percent   FLOAT,
# #         extracted_at    TIMESTAMP DEFAULT NOW()
# #     )
# # """)
# #
# # # --- 7. INSERT RACE INFO ---
# # cur.execute("""
# #     INSERT INTO "Racing".race_info
# #         (race_date, track, race_number, race_name, distance, condition,
# #          weather, race_class, abandoned, prize_money, num_runners)
# #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
# # """, race_row)
# #
# # print("1 race row inserted.")
# #
# # # --- 8. INSERT RUNNERS ---
# # cur.executemany("""
# #     INSERT INTO "Racing".race_runners
# #         (race_date, race_number, runner_number, horse_name, jockey, trainer,
# #          weight, barrier, age, form, scratched, racing_colours, gear_change,
# #          overall_starts, overall_wins, overall_places, win_percent, place_percent)
# #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
# # """, runner_rows)
# #
# # print(f"{cur.rowcount} runner rows inserted.")
# #
# # # --- 9. COMMIT & CLOSE ---
# # conn.commit()
# # cur.close()
# # conn.close()


# import requests
# import psycopg2
# from datetime import date, timedelta
# import time

# API_KEY = "fk_4413b47e6c96b47dd79861424402006f3bd6a466bdcefbb7748af80c271db8e7"
# HEADERS = {"X-API-Key": API_KEY}
# BASE_URL = "https://api.formfav.com/v1"

# # --- 1. CONNECT TO POSTGRES ---
# conn = psycopg2.connect(
#     host="localhost",
#     dbname="Project_Racing",
#     user="nageswarchowdarypotluri",
#     password="",
#     port=5432
# )
# cur = conn.cursor()

# # --- 2. CREATE SCHEMA & TABLES ---
# cur.execute('CREATE SCHEMA IF NOT EXISTS "Racing"')

# cur.execute("""
#     CREATE TABLE IF NOT EXISTS "Racing".race_info (
#         id              SERIAL PRIMARY KEY,
#         race_date       DATE,
#         track           VARCHAR(100),
#         race_number     INT,
#         race_name       VARCHAR(200),
#         distance        VARCHAR(20),
#         condition       VARCHAR(50),
#         weather         VARCHAR(50),
#         race_class      VARCHAR(100),
#         abandoned       BOOLEAN,
#         prize_money     VARCHAR(50),
#         num_runners     INT,
#         extracted_at    TIMESTAMP DEFAULT NOW(),
#         UNIQUE (race_date, track, race_number)
#     )
# """)

# cur.execute("""
#     CREATE TABLE IF NOT EXISTS "Racing".race_runners (
#         id              SERIAL PRIMARY KEY,
#         race_date       DATE,
#         race_number     INT,
#         track           VARCHAR(100),
#         runner_number   INT,
#         horse_name      VARCHAR(100),
#         jockey          VARCHAR(100),
#         trainer         VARCHAR(100),
#         weight          FLOAT,
#         barrier         INT,
#         age             INT,
#         form            VARCHAR(50),
#         scratched       BOOLEAN,
#         racing_colours  VARCHAR(100),
#         gear_change     VARCHAR(100),
#         overall_starts  INT,
#         overall_wins    INT,
#         overall_places  INT,
#         win_percent     FLOAT,
#         place_percent   FLOAT,
#         extracted_at    TIMESTAMP DEFAULT NOW(),
#         UNIQUE (race_date, track, race_number, runner_number)
#     )
# """)
# conn.commit()
# print("Schema and tables ready.")

# # --- 3. GENERATE DATE RANGE (last 1 year) ---
# end_date   = date.today()
# start_date = end_date - timedelta(days=100)
# date_range = [start_date + timedelta(days=i)
#               for i in range((end_date - start_date).days + 1)]

# # --- 4. LOOP THROUGH DATES ---
# total_races   = 0
# total_runners = 0

# for race_date in date_range:
#     date_str = race_date.strftime("%Y-%m-%d")

#     # Step A — get meetings for this date
#     try:
#         meetings_resp = requests.get(
#             f"{BASE_URL}/form/meetings",
#             params={"date": date_str, "race_code": "gallops"},
#             headers=HEADERS,
#             timeout=10
#         )
#         if meetings_resp.status_code != 200:
#             print(f"{date_str} — no meetings found, skipping.")
#             continue

#         meetings = meetings_resp.json().get("meetings", [])

#     except Exception as e:
#         print(f"{date_str} — meetings error: {e}")
#         continue

#     # Step B — loop through each meeting
#     for meeting in meetings:
#         track_slug = meeting.get("slug") or meeting.get("track")

#         # Step C — loop through race numbers 1 to 10
#         for race_num in range(1, 11):
#             try:
#                 race_resp = requests.get(
#                     f"{BASE_URL}/form",
#                     params={
#                         "date": date_str,
#                         "track": track_slug,
#                         "race": str(race_num),
#                         "race_code": "gallops",
#                         "country": "au"
#                     },
#                     headers=HEADERS,
#                     timeout=10
#                 )

#                 if race_resp.status_code != 200:
#                     break  # no more races for this track today

#                 data = race_resp.json()

#                 # --- INSERT race_info ---
#                 cur.execute("""
#                     INSERT INTO "Racing".race_info
#                         (race_date, track, race_number, race_name, distance,
#                          condition, weather, race_class, abandoned,
#                          prize_money, num_runners)
#                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#                     ON CONFLICT (race_date, track, race_number) DO NOTHING
#                 """, (
#                     data["date"], data["track"], data["raceNumber"],
#                     data["raceName"], data["distance"], data["condition"],
#                     data["weather"], data["raceClass"], data["abandoned"],
#                     data["prizeMoney"], data["numberOfRunners"]
#                 ))

#                 # --- INSERT race_runners ---
#                 for r in data["runners"]:
#                     overall = r["stats"]["overall"] if r["stats"]["overall"] else {}
#                     cur.execute("""
#                         INSERT INTO "Racing".race_runners
#                             (race_date, race_number, track, runner_number,
#                              horse_name, jockey, trainer, weight, barrier,
#                              age, form, scratched, racing_colours, gear_change,
#                              overall_starts, overall_wins, overall_places,
#                              win_percent, place_percent)
#                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#                         ON CONFLICT (race_date, track, race_number, runner_number) DO NOTHING
#                     """, (
#                         data["date"], data["raceNumber"], data["track"],
#                         r["number"], r["name"], r["jockey"], r["trainer"],
#                         r["weight"], r["barrier"], r["age"], r["form"],
#                         r["scratched"], r["racingColours"], r.get("gearChange"),
#                         overall.get("starts"), overall.get("wins"),
#                         overall.get("places"), overall.get("winPercent"),
#                         overall.get("placePercent")
#                     ))

#                 conn.commit()
#                 total_races   += 1
#                 total_runners += len(data["runners"])
#                 print(f"  ✓ {date_str} | {track_slug} | Race {race_num} | {len(data['runners'])} runners")

#                 time.sleep(0.3)  # rate limit — be nice to the API
#               except Exception as e:
#                  conn.rollback()
#                  print(f"  ✗ {date_str} | {track_slug} | Race {race_num} — error: {e}")

#                  # --- temporary debug ---
#                  for r in data["runners"]:
#                    print(f"  horse: {len(r['name'])} | jockey: {len(r['jockey'])} | trainer: {len(r['trainer'])} | colours: {len(r['racingColours'])}")
#                  continue

# # --- 5. CLOSE ---
# cur.close()
# conn.close()



import requests
import psycopg2
from datetime import date, timedelta
import time

API_KEY = "fk_4413b47e6c96b47dd79861424402006f3bd6a466bdcefbb7748af80c271db8e7"
HEADERS = {"X-API-Key": API_KEY}
BASE_URL = "https://api.formfav.com/v1"

# --- TRACK CODE MAPPING ---
TRACK_CODES = {
    "flemington":     "FLM",
    "ballarat":       "BAL",
    "caulfield":      "CAU",
    "moonee valley":  "MVM",
    "sandown":        "SAN",
    "bendigo":        "BEN",
    "geelong":        "GEE",
    "balaklava":      "BLK",
    "cranbourne":     "CRN",
    "pakenham":       "PAK",
    "mornington":     "MOR",
    "sale":           "SAL",
    "echuca":         "ECH",
    "seymour":        "SEY",
    "hamilton":       "HAM",
    "warrnambool":    "WBL",
    "horsham":        "HOR",
    "wodonga":        "WOD",
    "stawell":        "STW",
    "swan hill":      "SWH",
}

def make_race_code(track, race_date, race_num):
    """Generate race code like FLM1501202507"""
    track_code = TRACK_CODES.get(track.lower(), track[:3].upper())
    day   = race_date.strftime("%d")
    month = race_date.strftime("%m")
    year  = race_date.strftime("%Y")
    num   = str(race_num).zfill(2)
    return f"{track_code}{day}{month}{year}{num}"

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

                # Generate race code
                race_code = make_race_code(track_slug, race_date, race_num)

                # Parse prize money — strip $ and commas
                prize = d.get("prizeMoney", "0").replace("$", "").replace(",", "").strip()
                prize = float(prize) if prize else None

                cur.execute("""
                    INSERT INTO "Racing".race (
                        race_id,
                        race_name,
                        short_race_name,
                        meeting_id,
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
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                    ON CONFLICT (race_id) DO NOTHING
                """, (
                    race_code,
                    d.get("raceName"),
                    d.get("raceName", "")[:100],
                    track_slug,
                    race_num,
                    d.get("raceClass"),
                    d.get("date"),
                    "completed",
                    int(d.get("distance", "0").replace("m", "")) if d.get("distance") else None,
                    d.get("condition"),
                    d.get("numberOfRunners"),
                    prize
                ))

                conn.commit()
                total_inserted += 1
                print(f"  ✓ {race_code} | {d.get('raceName')} | {d.get('distance')}")

                time.sleep(0.3)

            except Exception as e:
                conn.rollback()
                print(f"  ✗ {date_str} | {track_slug} | Race {race_num} — error: {e}")
                continue

print(f"\nDone. {total_inserted} races inserted.")

cur.close()
conn.close()






