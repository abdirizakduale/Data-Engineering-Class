# stop_events/app.py
"""
Flask front-end + JSON API for TriMet Breadcrumb ▸ Stop-Event project
--------------------------------------------------------------------
Routes
  /                         → redirect to /map
  /map                      → HTML map + date-picker
  /api/breadcrumb_trip/<d>  → GeoJSON for YYYY-MM-DD
"""

from __future__ import annotations
import os
from datetime import date, datetime
from typing import Any, Dict, List

from flask import (
    Flask, abort, jsonify, redirect, render_template, url_for
)
from .common import connect_db, logger

MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN", "")
POINT_LIMIT  = 10_000               # max points the API will return

app = Flask(__name__)

# ──────────────────────────────────────────────────────────────────────
def _rows_for_date(day: date) -> List[Dict[str, Any]]:
    """
    Return up to POINT_LIMIT random rows from the integrated view for *day*.
    `ORDER BY random()` works on views; LIMIT caps payload size.
    """
    sql = f"""
        SELECT  ts, latitude, longitude, speed,
                route_id, vehicle_id, trip_id,
                service_key, direction
        FROM    v_breadcrumb_trip
        WHERE   ts::date = %s
        ORDER BY random()
        LIMIT   {POINT_LIMIT};
    """
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(sql, (day,))
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur]

# --------------------------------------------------------------------
def _to_geojson(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    feats = []
    for r in rows:
        try:
            lat = float(r["latitude"]); lon = float(r["longitude"])
        except (TypeError, ValueError):
            continue
        feats.append({
            "type": "Feature",
            "geometry": {"type":"Point","coordinates":[lon,lat]},
            "properties": {
                "ts":   r["ts"].isoformat()
                         if hasattr(r["ts"], "isoformat") else r["ts"],
                "speed": r["speed"],
                "route": r["route_id"],
                "veh":   r["vehicle_id"],
                "svc":   r["service_key"],
                "dir":   r["direction"],
                "trip":  r["trip_id"],
            }
        })
    return {"type":"FeatureCollection","features":feats}

# ────────────────────────── routes ───────────────────────────────────
@app.route("/")
def root():
    return redirect(url_for("map_page"))

@app.route("/map")
def map_page():
    return render_template(
        "map.html",
        token=MAPBOX_TOKEN,
        today=date.today().isoformat(),
    )

@app.route("/api/breadcrumb_trip/<date_str>")
def breadcrumb_trip(date_str: str):
    try:
        day = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        abort(400, "Date must be YYYY-MM-DD")

    rows = _rows_for_date(day)
    geo  = _to_geojson(rows)
    logger.info("API %s → %d features", day, len(geo["features"]))
    return jsonify(geo)

# ────────────────────────── main ─────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
