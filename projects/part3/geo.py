"""
Fetches Breadcrumb + StopEvent data for a given service-day and returns a
GeoJSON FeatureCollection suitable for MapboxGL (or Folium).

Relies on the existing connect_db() helper from stop_events.common.
"""
from __future__ import annotations
import json
from datetime import date
from typing import Any, Dict, List

from stop_events.common import connect_db, PgConfig


def _fetch_rows(opd: date) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            b.gps_latitude  AS lat,
            b.gps_longitude AS lon,
            b.speed_mph     AS speed,
            b.vehicle_id,
            t.route_id,
            t.event_no_trip AS trip_id,
            b.ts            AS breadcrumb_ts,
            t.departure_time,
            t.arrival_time
        FROM   breadcrumb  b
        JOIN   trip        t  ON t.event_no_trip = b.event_no_trip
        WHERE  b.opd_date = %s
        ORDER  BY trip_id, b.ts;
    """
    with connect_db(PgConfig()) as conn, conn.cursor() as cur:
        cur.execute(sql, (opd,))
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur]


def geojson_for_date(opd: date) -> Dict[str, Any]:
    """
    Return a GeoJSON FeatureCollection of all breadcrumbs for the given date,
    each enriched with Stop-Event (Trip) info.
    """
    features = []
    for row in _fetch_rows(opd):
        feat = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row["lon"], row["lat"]],
            },
            "properties": {
                "vehicle": row["vehicle_id"],
                "route":   row["route_id"],
                "trip":    row["trip_id"],
                "speed":   row["speed"],
                "ts":      row["breadcrumb_ts"].isoformat(),
                "depart":  row["departure_time"],
                "arrive":  row["arrival_time"],
            },
        }
        features.append(feat)
    return {"type": "FeatureCollection", "features": features}


def to_json(opd: date) -> str:
    "Convenience wrapper for sending over HTTP."
    return json.dumps(geojson_for_date(opd), separators=(",", ":")
