#!/usr/bin/env python3
"""
Subscribe to Pub/Sub, validate each breadcrumb with 10 separate assertions,
transform it, and bulk-load into PostgreSQL with psycopg2.copy_from.
"""

import json, logging, io, csv
from datetime import datetime, timedelta
from typing import Dict, Callable, List

from google.cloud import pubsub_v1
import psycopg2

# ────────────────────────────── configuration ──────────────────────────────
SUBSCRIPTION_PATH = "projects/somalias-data-eng/subscriptions/breadcrumbs-sub"
DB_CONFIG = {
    "dbname":   "trimet",
    "user":     "postgres",
    "password": "password",
    "host":     "127.0.0.1",
    "port":     5432,
}

MAX_SPEED_M_S = 35.0          # ~78 mph
BATCH_SIZE     = 1_000        # rows per COPY
# ────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("receiver")

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True
cur  = conn.cursor()

previous: Dict[int, Dict[str, int]] = {}      # for inter-record checks
buffer     = io.StringIO()
csv_writer = csv.writer(buffer)
rows_in_buf = 0

# ────────────────────────── assertion helpers (10) ─────────────────────────
def assert_required(rec):
    required = ("VEHICLE_ID","EVENT_NO_TRIP","EVENT_NO_STOP",
                "OPD_DATE","ACT_TIME","METERS",
                "GPS_LATITUDE","GPS_LONGITUDE")
    missing = [k for k in required if rec.get(k) is None]
    assert not missing, f"missing {missing}"

def assert_act_time(rec):
    assert 0 <= rec["ACT_TIME"] <= 86_399, "ACT_TIME out of range"

def assert_coords(rec):
    lat,lon = rec["GPS_LATITUDE"], rec["GPS_LONGITUDE"]
    assert 45.0 <= lat <= 46.0 and -123.5 <= lon <= -122.0, "coords OOB"

def assert_satellites(rec):
    sat = rec.get("GPS_SATELLITES")
    if sat is not None:
        assert 4 <= sat <= 20, "GPS_SATELLITES OOB"

def assert_hdop_positive(rec):
    hdop = rec.get("GPS_HDOP")
    if hdop is not None:
        assert hdop > 0, "HDOP ≤ 0"

def assert_meters_consistency(rec):
    if rec["METERS"] == 0 and rec["ACT_TIME"] > 0:
        raise AssertionError("METERS=0 yet ACT_TIME>0")

def assert_hdop_reasonable(rec):
    hdop = rec.get("GPS_HDOP")
    if hdop is not None:
        assert hdop <= 10, "HDOP too high"

def assert_same_service_day(rec):
    prev = previous.get(rec["EVENT_NO_TRIP"])
    if prev:
        assert prev["OPD_DATE"] == rec["OPD_DATE"], "OPD_DATE jumped"

def assert_time_forward(rec):
    prev = previous.get(rec["EVENT_NO_TRIP"])
    if prev:
        assert rec["ACT_TIME"] >= prev["ACT_TIME"], "time moved back"

def assert_speed(rec):
    prev = previous.get(rec["EVENT_NO_TRIP"])
    speed = 0.0
    if prev:
        dt = rec["ACT_TIME"] - prev["ACT_TIME"]
        ds = rec["METERS"]   - prev["METERS"]
        speed = (ds/dt) if dt else 0.0
    rec["speed"] = speed        # stash for later use
    assert 0 <= speed <= MAX_SPEED_M_S, f"speed {speed:.2f} m/s"

ASSERTIONS: List[Callable[[Dict],None]] = [
    assert_required, assert_act_time, assert_coords, assert_satellites,
    assert_hdop_positive, assert_meters_consistency, assert_hdop_reasonable,
    assert_same_service_day, assert_time_forward, assert_speed,
]

# ───────────────────────────── utility helpers ─────────────────────────────
def opd_to_date(opd: str) -> datetime:
    return datetime.strptime(opd.split(":",1)[0].title(), "%d%b%Y")

def apply_assertions(rec: Dict) -> bool:
    for fn in ASSERTIONS:
        try:
            fn(rec)
        except AssertionError as e:
            log.warning("%s: %s", fn.__name__, e)
            return False
        except Exception as e:
            log.error("%s unexpected: %s", fn.__name__, e)
            return False
    return True

def flush():
    global rows_in_buf
    if rows_in_buf == 0:
        return
    buffer.seek(0)
    try:
        cur.copy_from(buffer, "breadcrumb",
                      sep=",",
                      columns=("tstamp","latitude","longitude","speed","trip_id"))
        log.info("Flushed %d rows via COPY", rows_in_buf)
    except Exception as e:
        log.error("COPY failed: %s", e)
    buffer.truncate(0); buffer.seek(0); rows_in_buf = 0

# ───────────────────────────── pub/sub callback ────────────────────────────
def callback(msg):
    global rows_in_buf
    try:
        rec = json.loads(msg.data.decode())
    except Exception as e:
        log.error("JSON decode: %s", e); msg.ack(); return

    if not apply_assertions(rec):
        msg.ack(); return

    tstamp = opd_to_date(rec["OPD_DATE"]) + timedelta(seconds=rec["ACT_TIME"])
    csv_writer.writerow([tstamp, rec["GPS_LATITUDE"], rec["GPS_LONGITUDE"],
                         rec["speed"], rec["EVENT_NO_TRIP"]])
    rows_in_buf += 1

    previous[rec["EVENT_NO_TRIP"]] = {
        "ACT_TIME": rec["ACT_TIME"],
        "METERS":   rec["METERS"],
        "OPD_DATE": rec["OPD_DATE"],
    }

    if rows_in_buf >= BATCH_SIZE:
        flush()

    msg.ack()

# ──────────────────────────────── main ─────────────────────────────────────
if __name__ == "__main__":
    log.info("Receiver starting — subscribing to %s", SUBSCRIPTION_PATH)
    subscriber = pubsub_v1.SubscriberClient()
    future = subscriber.subscribe(SUBSCRIPTION_PATH, callback=callback)
    try:
        future.result()
    except KeyboardInterrupt:
        log.info("Ctrl-C received, shutting down …")
    finally:
        future.cancel()
        flush()
        cur.close(); conn.close()
        log.info("Receiver stopped cleanly")
