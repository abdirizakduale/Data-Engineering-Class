# stop_events/publisher.py
"""
Stop-Events PUBLISHER
────────────────────────────────────────────────────────────────────────────
• Scrapes TriMet Stop-Event HTML tables (one request per bus)
• Normalises header names → DB column names
• Publishes every dict to Cloud Pub/Sub
• Saves a newline-delimited JSON file for replay/back-fill
Env:
    VEHICLE_LIST   comma-sep bus IDs, e.g. 3909,3913   (falls back to list below)
"""

from __future__ import annotations

import concurrent.futures
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List

import requests
from bs4 import BeautifulSoup

from .common import logger, publish_json

# ─── Endpoint ──────────────────────────────────────────────────────────────
BASE_URL = (
    "https://busdata.cs.pdx.edu/api/getStopEvents"
    "?vehicle_num={id}"
)

# ─── Header aliases → DB column names ───────────────────────────────────────
HEADER_MAP = {
    # trip / stop identifiers
    "TRIPNO":        "trip_id",
    "TRIP_NO":       "trip_id",
    "TRIP_NUMBER":   "trip_id",

    "STOPNO":        "event_no_stop",
    "EVENT_NO_STOP": "event_no_stop",

    # date / time
    "OPD_DATE":      "opd_date",
    "DEPARTURE_TIME":"departure_time",
    "ARRIVAL_TIME":  "arrival_time",

    # route / vehicle / direction / service
    "ROUTE":         "route_id",
    "ROUTE_ID":      "route_id",
    "ROUTE_NUMBER":  "route_id",
    "BLOCK":         "route_id",

    "VEHICLE":       "vehicle_id",
    "VEHICLE_ID":    "vehicle_id",
    "VEHICLE_NUMBER":"vehicle_id",

    "DIR":           "direction",
    "DIRECTION":     "direction",

    "SERVICEKEY":    "service_key",
    "SERVICE_KEY":   "service_key",
}

def _normalise(raw_header: str) -> str:
    """Map raw HTML header → DB column name."""
    key = raw_header.strip().upper().replace(" ", "_")
    return HEADER_MAP.get(key, key.lower())

# ─── Scraper helper ────────────────────────────────────────────────────────
def fetch_stop_events(bus_id: int) -> List[dict]:
    """Return list[dict] for one bus. Empty list if no data."""
    resp = requests.get(BASE_URL.format(id=bus_id), timeout=10)

    if resp.status_code == 404:
        logger.warning("Bus %s → 404 (no data)", bus_id)
        return []
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")
    if not table:
        logger.warning("Bus %s → page contains no <table>", bus_id)
        return []

    headers = [_normalise(th.get_text()) for th in table.select("th")]
    rows: List[dict] = []

    for tr in table.select("tr")[1:]:             # skip header row
        cells = [td.get_text(strip=True) for td in tr.select("td")]
        if len(cells) != len(headers):
            continue
        rows.append(dict(zip(headers, cells)))

    return rows

# ─── Publisher class ───────────────────────────────────────────────────────
class StopEventPublisher:
    def __init__(self, vehicle_nums: List[int]) -> None:
        self.vehicle_nums = vehicle_nums
        self.out_file = Path(f"{datetime.now():%Y-%m-%d}_stop_events.json")

    def run(self) -> None:
        futs, total = [], 0
        with self.out_file.open("w", encoding="utf-8") as fout:
            for vid in self.vehicle_nums:
                try:
                    recs = fetch_stop_events(vid)
                    if not recs:
                        continue

                    for rec in recs:
                        fout.write(json.dumps(rec) + "\n")
                        futs.append(publish_json(rec))

                    total += len(recs)
                    logger.info("Bus %s → published %d events", vid, len(recs))
                except Exception as exc:                 # noqa: BLE001
                    logger.error("Bus %s failed: %s", vid, exc, exc_info=True)

        concurrent.futures.wait(futs, timeout=120)
        logger.info("Finished publishing %d stop events", total)

# ─── Main entry-point ──────────────────────────────────────────────────────
if __name__ == "__main__":
    env_ids = os.getenv("VEHICLE_LIST", "3909,3913")
    VEHICLES = [int(v) for v in env_ids.split(",") if v.strip().isdigit()]
    if not VEHICLES:
        raise SystemExit("No VEHICLE_LIST configured!")
    StopEventPublisher(VEHICLES).run()
