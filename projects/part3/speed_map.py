#!/usr/bin/env python3
"""
speed_map.py  <in.csv> <out.html>  [MAPBOX_TOKEN]

• <in.csv>  must have columns: latitude, longitude, speed   (header row ok)
• <out.html> is the standalone map you can open in any browser
• MAPBOX_TOKEN is looked up in env if not supplied as CLI arg
"""

import csv, json, sys, os, pathlib, statistics

# ── args & token ──────────────────────────────────────────────────────────
if len(sys.argv) < 3:
    print("usage: speed_map.py in.csv out.html [MAPBOX_TOKEN]", file=sys.stderr)
    sys.exit(1)

in_csv   = pathlib.Path(sys.argv[1]).expanduser()
out_html = pathlib.Path(sys.argv[2]).expanduser()
token    = sys.argv[3] if len(sys.argv) > 3 else os.getenv("MAPBOX_TOKEN")

if not token:
    print("❌  Mapbox token missing (supply as arg or MAPBOX_TOKEN env-var)", file=sys.stderr)
    sys.exit(1)

# ── read csv → geojson ────────────────────────────────────────────────────
features, speeds = [], []

with in_csv.open() as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            lat = float(row["latitude"]); lon = float(row["longitude"])
            spd = float(row["speed"])
        except (KeyError, ValueError):
            continue  # skip malformed
        speeds.append(spd)
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {"speed": spd}
        })

if not features:
    print("❌  no valid points found!", file=sys.stderr)
    sys.exit(1)

geojson = {"type": "FeatureCollection", "features": features}
spd_min, spd_max = min(speeds), max(speeds)
spd_mid          = statistics.median(speeds)

# ── html template (Mapbox GL) ─────────────────────────────────────────────
html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Speed map</title>
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>
  <link   href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet" />
  <style>
    html,body,#map {{ margin:0; padding:0; height:100%; }}
  </style>
</head>
<body>
<div id="map"></div>

<script>
mapboxgl.accessToken = "{token}";
const map = new mapboxgl.Map({{
    container: "map",
    style: "mapbox://styles/mapbox/dark-v11",
    center: [{features[0]["geometry"]["coordinates"][0]:.5f},
             {features[0]["geometry"]["coordinates"][1]:.5f}],
    zoom: 13
}});

map.on("load", () => {{
  // add data
  map.addSource("trip", {{
      "type": "geojson",
      "data": {json.dumps(geojson)}
  }});

  // colour scale: blue → yellow → red
  map.addLayer({{
      "id": "speed-points",
      "type": "circle",
      "source": "trip",
      "paint": {{
          "circle-radius": 4,
          "circle-opacity": 0.8,
          "circle-color": [
              "interpolate", ["linear"], ["get", "speed"],
              {spd_min:.2f}, "#2b83ba",
              {spd_mid:.2f}, "#ffffbf",
              {spd_max:.2f}, "#d7191c"
          ]
      }}
  }});

  // add speed legend
  const legend = document.createElement('div');
  legend.style = 'position:absolute;bottom:20px;left:10px;padding:6px 10px;background:#000a;color:#fff;font:12px/18px sans-serif';
  legend.innerHTML = `<b>Speed (mph)</b><br>
                      <span style="color:#2b83ba">●</span> ≤ {spd_min:.1f}<br>
                      <span style="color:#ffffbf">●</span> ≈ {spd_mid:.1f}<br>
                      <span style="color:#d7191c">●</span> ≥ {spd_max:.1f}`;
  map.getContainer().appendChild(legend);
}});
</script>
</body>
</html>
"""

# ── write file ────────────────────────────────────────────────────────────
out_html.write_text(html, encoding="utf-8")
print(f"✅ map written → {out_html}")
