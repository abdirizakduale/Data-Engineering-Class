"""
Stop-Events RECEIVER
─────────────────────────────────────────────────────────────────────────────
• Listens to the Pub/Sub subscription defined in common.py
• Validates each JSON message
• Inserts rows into the Postgres `trip` table
  (create the table beforehand -- see README or Assignment 2)
Environment variables required (picked up by common.connect_db):
    PG_DB, PG_USER, PG_PWD, PG_HOST, PG_PORT
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

from google.cloud import pubsub_v1

from .common import SUB_PATH, connect_db, logger, validate_stop, subscriber


class StopEventReceiver:
    def __init__(self) -> None:
        self.conn = connect_db()        # reads PG_* env-vars
        self.cur = self.conn.cursor()

    # ------------------------------------------------------------------ #
    def _upsert_trip(self, rec: dict) -> None:
        """Insert / ignore-on-conflict example for the Trip table."""
        try:
            self.cur.execute(
                """
                INSERT INTO trip (
                    event_no_trip,
                    vehicle_id,
                    route_id,
                    event_no_stop,
                    opd_date,
                    departure_time,
                    arrival_time
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (event_no_trip, event_no_stop) DO NOTHING
                """,
                (
                    rec["EVENT_NO_TRIP"],
                    rec["VEHICLE_ID"],
                    rec.get("ROUTE_ID"),
                    rec["EVENT_NO_STOP"],
                    rec["OPD_DATE"],
                    self._ts(rec["OPD_DATE"], rec["DEPARTURE_TIME"]),
                    self._ts(rec["OPD_DATE"], rec.get("ARRIVAL_TIME")),
                ),
            )
        except Exception as exc:             # noqa: BLE001
            logger.error("DB insert failed: %s", exc, exc_info=True)

    # ------------------------------------------------------------------ #
    def _callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        try:
            rec = json.loads(message.data)
            if validate_stop(rec):
                self._upsert_trip(rec)
        finally:
            message.ack()

    # ------------------------------------------------------------------ #
    @staticmethod
    def _ts(opd: str, seconds: str | None):
        """Convert OPD_DATE + seconds-since-midnight → datetime."""
        if seconds in (None, "", "NULL"):
            return None
        date_obj = datetime.strptime(opd.split(":", 1)[0].title(), "%d%b%Y")
        return date_obj + timedelta(seconds=int(seconds))

    # ------------------------------------------------------------------ #
    def run(self) -> None:
        logger.info("Listening on %s", SUB_PATH)
        future = subscriber.subscribe(SUB_PATH, callback=self._callback)
        try:
            future.result()
        except KeyboardInterrupt:            # graceful exit
            future.cancel()
            self.cur.close()
            self.conn.close()
            logger.info("Receiver shut down cleanly")


# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    StopEventReceiver().run()
