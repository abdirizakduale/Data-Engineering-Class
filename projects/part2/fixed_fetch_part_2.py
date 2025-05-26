#!/usr/bin/env python3
"""
Download breadcrumbs for each vehicle_id and publish them to Pub/Sub.
Every exception is logged – nothing silently “continues”.
"""

import concurrent.futures, json, logging, requests
from datetime import datetime
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s fetch: %(message)s")
log = logging.getLogger("fetch")

TOPIC_PATH = "projects/somalias-data-eng/topics/breadcrumbs"
publisher  = pubsub_v1.PublisherClient()
BASE_URL   = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="

def publish(msg: str):
    return publisher.publish(TOPIC_PATH, data=msg.encode())

VEHICLE_IDS = [
    2901, 2902, 2904, 2905, 2907, 2908, 2910, 2922, 2924, 2926, 2929, 2935,
    # … (same list you were already using) …
    4526, 4528, 4530
]

publish_futures = []
for vid in VEHICLE_IDS:
    url = f"{BASE_URL}{vid}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        log.error("Vehicle %s fetch/parsing error: %s", vid, e)
        continue

    if not isinstance(data, list):
        data = [data]

    for rec in data:
        publish_futures.append(publish(json.dumps(rec)))

try:
    concurrent.futures.wait(publish_futures, timeout=60)
    log.info("Published %d messages", len(publish_futures))
except Exception as e:
    log.error("Waiting on publish futures failed: %s", e)
