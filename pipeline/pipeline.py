#!/usr/bin/env python3
import csv
import datetime as dt
import glob
import json
import os
import re
from pathlib import Path
import subprocess

import duckdb

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

F1_RE = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z$")
F2_RE = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})([+-])([0-9]{2}):([0-9]{2})$")
F3_RE = re.compile(r"^[0-9]+$")
MAX_I64 = 9223372036854775807


def parse_field(raw):
    if b'"' not in raw:
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if len(raw) < 2 or raw[0] != 34 or raw[-1] != 34:
        return None
    inner = raw[1:-1]
    if b'"' in inner:
        return None
    try:
        return inner.decode("utf-8")
    except UnicodeDecodeError:
        return None


def parse_csv_line(raw_line, field_count):
    if raw_line.endswith(b"\n"):
        raw_line = raw_line[:-1]
    if not raw_line:
        return None
    parts = raw_line.split(b",")
    if len(parts) != field_count:
        return None
    fields = []
    for part in parts:
        value = parse_field(part)
        if value is None:
            return None
        fields.append(value)
    return fields


def csv_files(folder):
    return sorted(glob.glob(os.path.join(DATA, folder, "*.csv")))


def parse_timestamp(value):
    m = F1_RE.match(value)
    if m:
        y, mo, d, h, mi, s = map(int, m.groups())
        try:
            return int(dt.datetime(y, mo, d, h, mi, s, tzinfo=dt.timezone.utc).timestamp() * 1000)
        except (ValueError, OverflowError):
            return None

    m = F2_RE.match(value)
    if m:
        y, mo, d, h, mi, s = map(int, m.groups()[:6])
        sign = m.group(7)
        oh = int(m.group(8))
        omi = int(m.group(9))
        if oh > 23 or omi > 59:
            return None
        try:
            local = dt.datetime(y, mo, d, h, mi, s, tzinfo=dt.timezone.utc)
        except ValueError:
            return None
        offset = (oh * 60 + omi) * 60
        if sign == "-":
            offset = -offset
        return int((local.timestamp() - offset) * 1000)

    if F3_RE.match(value):
        try:
            number = int(value)
        except ValueError:
            return None
        if number > MAX_I64:
            return MAX_I64
        return number
    return None


def read_clean_csv(path, field_count):
    rows = []
    with open(path, "rb") as f:
        header = f.readline()
        for line in f:
            fields = parse_csv_line(line, field_count)
            if fields is not None:
                rows.append(fields)
    return rows


with open(os.path.join(DATA, "manifest.json"), encoding="utf-8") as f:
    manifest = json.load(f)
window_start = parse_timestamp(manifest["window_start"])
window_end = parse_timestamp(manifest["window_end"])

products = {}
for path in [os.path.join(DATA, "products.csv")]:
    for fields in read_clean_csv(path, 2):
        product_id, category = fields
        products[product_id] = category

campaigns = {}
for path in [os.path.join(DATA, "campaigns.csv")]:
    for fields in read_clean_csv(path, 2):
        campaign_id, channel = fields
        campaigns[campaign_id] = channel

os.makedirs(SCRATCH, exist_ok=True)
staging_dir = os.path.join(SCRATCH, "shopping_staging")
os.makedirs(staging_dir, exist_ok=True)

event_csvs = []
event_special_csvs = []
for path in csv_files("events"):
    with open(path, "rb") as f:
        content = f.read()
    if b"\r" in content:
        event_special_csvs.append(path)
        continue
    with open(path, "rb") as f:
        header = f.readline()
        remaining = f.read()
        if remaining.strip(b"\r\n"):
            event_csvs.append(path)
if not event_csvs:
    event_csvs = [os.path.join(staging_dir, "empty_events.csv")]
    with open(event_csvs[0], "w", encoding="utf-8", newline="") as out:
        out.write("event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n")

click_csvs = []
click_special_csvs = []
for path in csv_files("clicks"):
    with open(path, "rb") as f:
        content = f.read()
    if b"\r" in content:
        click_special_csvs.append(path)
        continue
    with open(path, "rb") as f:
        header = f.readline()
        remaining = f.read()
        if remaining.strip(b"\r\n"):
            click_csvs.append(path)
if not click_csvs:
    click_csvs = [os.path.join(staging_dir, "empty_clicks.csv")]
    with open(click_csvs[0], "w", encoding="utf-8", newline="") as out:
        out.write("click_id,user_id,campaign_id,ts\n")

special_events = os.path.join(staging_dir, "special_events.csv")
with open(special_events, "w", encoding="utf-8", newline="") as out:
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"])
    for path in event_special_csvs:
        with open(path, "rb") as f:
            f.readline()
            for line in f:
                fields = parse_csv_line(line, 7)
                if fields is not None:
                    writer.writerow(fields)

special_clicks = os.path.join(staging_dir, "special_clicks.csv")
with open(special_clicks, "w", encoding="utf-8", newline="") as out:
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(["click_id", "user_id", "campaign_id", "ts"])
    for path in click_special_csvs:
        with open(path, "rb") as f:
            f.readline()
            for line in f:
                fields = parse_csv_line(line, 4)
                if fields is not None:
                    writer.writerow(fields)

preprocess = "/app/shopping-preprocess"
if not os.path.exists(preprocess):
    local_preprocess = "/workspaces/HELEX/target/release/shopping-preprocess"
    if os.path.exists(local_preprocess):
        preprocess = local_preprocess
    else:
        subprocess.run(
            ["cargo", "build", "--release", "--manifest-path", "/workspaces/HELEX/Cargo.toml"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        preprocess = local_preprocess
subprocess.run(
    [preprocess],
    check=True,
    env={**os.environ, "DATA_DIR": DATA, "SCRATCH_DIR": SCRATCH},
)

con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")

con.execute(f"""
    CREATE TEMP TABLE raw_events AS
    SELECT * FROM read_csv(
        '{os.path.join(staging_dir, "events.csv")}',
        header=true,
        columns={{
            'event_id':'VARCHAR','user_id':'VARCHAR','ts':'VARCHAR','ingest_ts':'VARCHAR',
            'event_type':'VARCHAR','product_id':'VARCHAR','price_cents':'VARCHAR'
        }},
        all_varchar=true,
        strict_mode=true,
        null_padding=false
    )
""")
con.execute(f"""
    CREATE TEMP TABLE raw_clicks AS
    SELECT * FROM read_csv(
        '{os.path.join(staging_dir, "clicks.csv")}',
        header=true,
        columns={{
            'click_id':'VARCHAR','user_id':'VARCHAR','campaign_id':'VARCHAR','ts':'VARCHAR'
        }},
        all_varchar=true,
        strict_mode=true,
        null_padding=false
    )
""")

product_staging = os.path.join(staging_dir, "products.csv")
with open(product_staging, "w", encoding="utf-8", newline="") as out:
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(["product_id", "category"])
    for product_id, category in products.items():
        writer.writerow([product_id, category])

campaign_staging = os.path.join(staging_dir, "campaigns.csv")
with open(campaign_staging, "w", encoding="utf-8", newline="") as out:
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(["campaign_id", "channel"])
    for campaign_id, channel in campaigns.items():
        writer.writerow([campaign_id, channel])

con.execute(f"""
    CREATE TEMP TABLE product_map AS
    SELECT * FROM read_csv('{product_staging}', header=true, all_varchar=true)
""")
con.execute(f"""
    CREATE TEMP TABLE campaign_map AS
    SELECT * FROM read_csv('{campaign_staging}', header=true, all_varchar=true)
""")
event_ts_expr = """
CASE
  WHEN regexp_full_match(ts, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$')
    THEN CAST(epoch_ms(try_strptime(ts, '%Y-%m-%dT%H:%M:%SZ')) AS BIGINT)
  WHEN regexp_full_match(ts, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}$')
       AND TRY_CAST(substr(ts,21,2) AS BIGINT) <= 23
       AND TRY_CAST(substr(ts,24,2) AS BIGINT) <= 59
    THEN CAST(epoch_ms(try_strptime(substr(ts,1,19), '%Y-%m-%dT%H:%M:%S')) AS BIGINT)
         - (CASE WHEN substr(ts,20,1)='+' THEN 1 ELSE -1 END)
           * ((TRY_CAST(substr(ts,21,2) AS BIGINT)*3600
               + TRY_CAST(substr(ts,24,2) AS BIGINT)*60)*1000)
  WHEN regexp_full_match(ts, '^[0-9]+$')
    THEN TRY_CAST(ts AS BIGINT)
END
"""
ingest_expr = """
CAST(epoch_ms(try_strptime(ingest_ts, '%Y-%m-%dT%H:%M:%SZ')) AS BIGINT)
"""

con.execute(f"""
    CREATE TEMP TABLE events AS
    WITH parsed AS (
        SELECT
            event_id,
            re.user_id AS user_id,
            TRY_CAST(ingest_ts AS BIGINT) AS ingest_ms,
            TRY_CAST(ts AS BIGINT) AS ts,
            event_type,
            product_id,
            price_cents AS price
        FROM raw_events re
    ),
    kept AS (
        SELECT *
        FROM (
            SELECT *,
                   row_number() OVER (PARTITION BY event_id ORDER BY ingest_ms) AS rn
            FROM parsed
            WHERE event_id <> ''
              AND ingest_ms IS NOT NULL
        )
        WHERE rn = 1
    ),
    valid AS (
        SELECT
            user_id,
            ts,
            event_type,
            product_id,
            CASE
                WHEN event_type = 'purchase'
                     AND regexp_full_match(price, '^[0-9]+$')
                     AND TRY_CAST(price AS BIGINT) BETWEEN 1 AND 10000000
                THEN TRY_CAST(price AS BIGINT)
                ELSE 0
            END AS price_cents
        FROM kept
        WHERE user_id <> ''
          AND ts IS NOT NULL
          AND ts >= {window_start}
          AND ts < {window_end}
          AND event_type IN ('view', 'cart', 'purchase')
          AND product_id <> ''
          AND (event_type <> 'purchase'
               OR (regexp_full_match(price, '^[0-9]+$')
                   AND TRY_CAST(price AS BIGINT) BETWEEN 1 AND 10000000))
          AND ingest_ms - ts <= 86400000
    )
    SELECT * FROM valid
""")

click_ts_expr = event_ts_expr.replace("ts,", "ts,")
# raw_clicks has ts, not ts_ms
con.execute(f"""
    CREATE TEMP TABLE clicks AS
    WITH parsed AS (
        SELECT
            click_id,
            rc.user_id AS user_id,
            campaign_id,
            TRY_CAST(ts AS BIGINT) AS ts
        FROM raw_clicks rc
    ),
    dedup AS (
        SELECT DISTINCT click_id, user_id, campaign_id, ts
        FROM parsed
        WHERE ts IS NOT NULL
    )
    SELECT *
    FROM dedup
    WHERE click_id <> ''
      AND user_id <> ''
      AND ts >= {window_start - 604800000}
      AND ts < {window_end}
      AND campaign_id IN (SELECT campaign_id FROM campaign_map)
""")

con.execute("""
    CREATE TEMP TABLE sessionized AS
    WITH ordered AS (
        SELECT
            user_id,
            ts,
            event_type,
            product_id,
            price_cents,
            lag(ts) OVER (PARTITION BY user_id ORDER BY ts) AS previous_ts
        FROM events
    ),
    marked AS (
        SELECT *,
               CASE WHEN previous_ts IS NULL OR ts - previous_ts > 1800000
                    THEN 1 ELSE 0 END AS starts_session
        FROM ordered
    ),
    numbered AS (
        SELECT *,
               sum(starts_session) OVER (
                   PARTITION BY user_id
                   ORDER BY ts
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS session_no
        FROM marked
    )
    SELECT
        user_id,
        session_no,
        min(ts) AS start_ts,
        count(*) AS event_count,
        sum(CASE WHEN event_type = 'purchase' THEN price_cents ELSE 0 END) AS revenue_cents,
        max(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS has_purchase
    FROM numbered
    GROUP BY user_id, session_no
""")

con.execute("""
    CREATE TEMP TABLE sessions AS
    SELECT
        row_number() OVER (ORDER BY user_id, start_ts, session_no) AS session_id,
        *
    FROM sessionized
""")

con.execute("""
    CREATE TEMP TABLE session_categories AS
    WITH ordered AS (
        SELECT
            user_id,
            ts,
            event_type,
            product_id,
            price_cents
        FROM events
    ),
    lagged AS (
        SELECT *,
               lag(ts) OVER (PARTITION BY user_id ORDER BY ts) AS previous_ts
        FROM ordered
    ),
    marked AS (
        SELECT *,
               CASE WHEN previous_ts IS NULL OR ts - previous_ts > 1800000
                    THEN 1 ELSE 0 END AS starts_session
        FROM lagged
    ),
    numbered AS (
        SELECT *,
               sum(starts_session) OVER (
                   PARTITION BY user_id
                   ORDER BY ts
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS session_no
        FROM marked
    ),
    purchases AS (
        SELECT
            n.user_id,
            n.session_no,
            coalesce(p.category, 'unknown') AS category,
            sum(n.price_cents) AS category_revenue
        FROM numbered n
        LEFT JOIN product_map p ON p.product_id = n.product_id
        WHERE n.event_type = 'purchase'
        GROUP BY n.user_id, n.session_no, coalesce(p.category, 'unknown')
    )
    SELECT s.session_id, purchases.category, purchases.category_revenue
    FROM purchases
    JOIN sessions s
      ON s.user_id = purchases.user_id
     AND s.session_no = purchases.session_no
""")

con.execute("""
    CREATE TEMP TABLE collapsed_clicks AS
    WITH picked AS (
        SELECT person_clicks.person, person_clicks.ts, min(person_clicks.click_id) AS click_id
        FROM (
            SELECT user_id AS person, ts, click_id
            FROM clicks
        ) person_clicks
        GROUP BY person_clicks.person, person_clicks.ts
    )
    SELECT picked.person, picked.ts, picked.click_id, clicks.campaign_id
    FROM picked
    JOIN clicks
      ON clicks.user_id = picked.person
     AND clicks.ts = picked.ts
     AND clicks.click_id = picked.click_id
    ORDER BY picked.person, picked.ts
""")

con.execute("""
    CREATE TEMP TABLE session_credits AS
    SELECT
        s.session_id,
        CASE
            WHEN c.campaign_id IS NULL
              OR c.ts < s.start_ts - 604800000
            THEN 'organic'
            ELSE c.campaign_id
        END AS campaign_id
    FROM sessions s
    ASOF LEFT JOIN collapsed_clicks c
      ON c.person = s.user_id
     AND c.ts <= s.start_ts
""")

con.execute("""
    CREATE TEMP TABLE session_assists AS
    SELECT DISTINCT
        s.session_id,
        s.user_id,
        s.start_ts,
        c.campaign_id
    FROM sessions s
    JOIN clicks c
      ON c.user_id = s.user_id
     AND c.ts >= s.start_ts - 604800000
     AND c.ts <= s.start_ts
""")

con.execute("""
    CREATE TEMP TABLE user_daily AS
    WITH base AS (
        SELECT
            s.user_id,
            CAST(epoch_ms(s.start_ts) AS DATE) AS day,
            count(*) AS sessions,
            sum(s.event_count) AS events,
            sum(s.revenue_cents) AS revenue_cents
        FROM sessions s
        GROUP BY s.user_id, CAST(epoch_ms(s.start_ts) AS DATE)
    ),
    category_totals AS (
        SELECT
            s.user_id,
            CAST(epoch_ms(s.start_ts) AS DATE) AS day,
            sc.category,
            sum(sc.category_revenue) AS category_revenue
        FROM sessions s
        JOIN session_categories sc ON sc.session_id = s.session_id
        GROUP BY s.user_id, CAST(epoch_ms(s.start_ts) AS DATE), sc.category
    ),
    ranked_categories AS (
        SELECT *,
               row_number() OVER (
                   PARTITION BY user_id, day
                   ORDER BY category_revenue DESC, category ASC
               ) AS category_rank
        FROM category_totals
    )
    SELECT
        b.user_id,
        b.day,
        b.sessions,
        b.events,
        b.revenue_cents,
        r.category AS top_category
    FROM base b
    LEFT JOIN ranked_categories r
      ON r.user_id = b.user_id
     AND r.day = b.day
     AND r.category_rank = 1
""")

con.execute("""
    CREATE TEMP TABLE campaign_daily AS
    WITH credited AS (
        SELECT
            sc.campaign_id,
            CAST(epoch_ms(s.start_ts) AS DATE) AS day,
            count(*) AS sessions,
            sum(s.has_purchase) AS purchasing_sessions,
            sum(s.revenue_cents) AS revenue_cents
        FROM session_credits sc
        JOIN sessions s ON s.session_id = sc.session_id
        GROUP BY sc.campaign_id, CAST(epoch_ms(s.start_ts) AS DATE)
    ),
    assisted AS (
        SELECT
            sa.campaign_id,
            CAST(epoch_ms(sa.start_ts) AS DATE) AS day,
            count(DISTINCT sa.session_id) AS assisted_sessions
        FROM session_assists sa
        WHERE sa.campaign_id <> 'organic'
        GROUP BY sa.campaign_id, CAST(epoch_ms(sa.start_ts) AS DATE)
    ),
    keys AS (
        SELECT campaign_id, day FROM credited
        UNION
        SELECT campaign_id, day FROM assisted
    )
    SELECT
        k.campaign_id,
        CASE WHEN k.campaign_id = 'organic' THEN 'organic'
             ELSE cm.channel END AS channel,
        k.day,
        coalesce(c.sessions, 0) AS sessions,
        coalesce(c.purchasing_sessions, 0) AS purchasing_sessions,
        coalesce(c.revenue_cents, 0) AS revenue_cents,
        coalesce(a.assisted_sessions, 0) AS assisted_sessions
    FROM keys k
    LEFT JOIN credited c ON c.campaign_id = k.campaign_id AND c.day = k.day
    LEFT JOIN assisted a ON a.campaign_id = k.campaign_id AND a.day = k.day
    LEFT JOIN campaign_map cm ON cm.campaign_id = k.campaign_id
""")

os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
con.execute(f"""
    COPY (
        SELECT
            CAST(user_id AS VARCHAR) AS user_id,
            CAST(day AS DATE) AS day,
            CAST(sessions AS BIGINT) AS sessions,
            CAST(events AS BIGINT) AS events,
            CAST(revenue_cents AS BIGINT) AS revenue_cents,
            CAST(top_category AS VARCHAR) AS top_category
        FROM user_daily
    ) TO '{os.path.join(OUT, "user_daily", "part-0.parquet")}' (FORMAT parquet)
""")
con.execute(f"""
    COPY (
        SELECT
            CAST(campaign_id AS VARCHAR) AS campaign_id,
            CAST(channel AS VARCHAR) AS channel,
            CAST(day AS DATE) AS day,
            CAST(sessions AS BIGINT) AS sessions,
            CAST(purchasing_sessions AS BIGINT) AS purchasing_sessions,
            CAST(revenue_cents AS BIGINT) AS revenue_cents,
            CAST(assisted_sessions AS BIGINT) AS assisted_sessions
        FROM campaign_daily
    ) TO '{os.path.join(OUT, "campaign_daily", "part-0.parquet")}' (FORMAT parquet)
""")
