# stop_events/common.py
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict

import google.auth
from google.oauth2 import service_account
import psycopg2
from google.cloud import pubsub_v1

# ─── Logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="[%(asctime)s] %(levelname)s:%(name)s: %(message)s",
)
logger = logging.getLogger("stop_events")

# ─── Pub/Sub credentials (force pubsub scope) ───────────────────────
SCOPES = ["https://www.googleapis.com/auth/pubsub"]

if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
    CREDS = service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=SCOPES
    )
else:
    CREDS, _ = google.auth.default(scopes=SCOPES)

PROJECT_ID = os.getenv("GCP_PROJECT") or CREDS.project_id
TOPIC_ID = os.getenv("STOP_TOPIC", "stop-events-topic")
SUB_ID = os.getenv("STOP_SUBSCRIPTION", "stop-events-sub")

publisher = pubsub_v1.PublisherClient(credentials=CREDS)
subscriber = pubsub_v1.SubscriberClient(credentials=CREDS)

TOPIC_PATH = publisher.topic_path(PROJECT_ID, TOPIC_ID)
SUB_PATH = subscriber.subscription_path(PROJECT_ID, SUB_ID)

def publish_json(payload: Dict[str, Any]):
    data = json.dumps(payload).encode()
    return publisher.publish(TOPIC_PATH, data)

# ─── Postgres helpers ────────────────────────────────────────────────
@dataclass
class PgConfig:
    dbname: str = os.getenv("PG_DB", "trimet")
    user:   str = os.getenv("PG_USER", "postgres")
    pwd:    str = os.getenv("PG_PWD",  "")
    host:  str = os.getenv("PG_HOST", "localhost")
    port:  int = int(os.getenv("PG_PORT", 5432))

def connect_db(cfg: PgConfig = PgConfig()):
    conn = psycopg2.connect(
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.pwd,
        host=cfg.host,
        port=cfg.port,
    )
    conn.autocommit = True
    return conn

# ─── Simple validation ──────────────────────────────────────────────
def validate_stop(rec: Dict[str, Any]) -> bool:
    needed = ("VEHICLE_ID", "EVENT_NO_TRIP", "EVENT_NO_STOP",
              "OPD_DATE", "DEPARTURE_TIME", "GPS_LATITUDE", "GPS_LONGITUDE")
    return all(rec.get(k) not in ("", None) for k in needed)
