"""Build the dashboard's map layers from the City of Chicago Data Portal (run once; output is committed).

    python scripts/build_map_geodata.py

Sources (public, no key):
  * CTA 'L' (Rail) Lines   https://data.cityofchicago.org/d/xbyr-jnvx  - track segments + the lines using them
  * CTA 'L' Stops          https://data.cityofchicago.org/d/8pix-ypme  - stops with coordinates and line flags

Output (frontend/public/data/geo/):
  * cta_lines.geojson     one feature per (segment, route). ``offset_index`` lets the map draw
                          shared track (e.g. the Loop) as parallel colored lines
  * cta_stations.geojson  one point per parent station (map_id), with its routes
"""

from __future__ import annotations

import json
import re
import sys
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "frontend" / "public" / "data" / "geo"
LINES_URL = "https://data.cityofchicago.org/resource/xbyr-jnvx.json?$limit=1000"
STOPS_URL = "https://data.cityofchicago.org/resource/8pix-ypme.json?$limit=2000"

# Canonical order matches the dashboard palette order.
ROUTES = ("Red", "Blue", "Brn", "G", "Org", "P", "Pink", "Y")
NAME_TO_ROUTE = {"Red": "Red", "Blue": "Blue", "Brown": "Brn", "Green": "G", "Orange": "Org",
                 "Purple": "P", "Pink": "Pink", "Yellow": "Y"}
STOP_FLAGS = {"red": "Red", "blue": "Blue", "brn": "Brn", "g": "G", "o": "Org", "p": "P", "pexp": "P",
              "pnk": "Pink", "y": "Y"}


def fetch(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "cta-transit-intelligence/0.1"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def round_coords(coords, ndigits: int = 5):
    if isinstance(coords[0], (int, float)):
        return [round(c, ndigits) for c in coords]
    return [round_coords(c, ndigits) for c in coords]


def routes_from_text(text: str) -> list[str]:
    found = {NAME_TO_ROUTE[m] for m in re.findall(r"Red|Blue|Brown|Green|Orange|Purple|Pink|Yellow", text)}
    return [r for r in ROUTES if r in found]


def build_lines(rows: list[dict]) -> dict:
    features = []
    for row in rows:
        geom = row.get("the_geom")
        routes = routes_from_text(row.get("lines", ""))
        if not geom or not routes:
            continue
        geometry = {"type": geom["type"], "coordinates": round_coords(geom["coordinates"])}
        for i, route in enumerate(routes):
            features.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "route": route,
                    "offset_index": i - (len(routes) - 1) / 2,  # centered: -1, 0, +1 ...
                    "shared": len(routes),
                    "description": row.get("description", ""),
                },
            })
    return {"type": "FeatureCollection", "features": features}


def build_stations(rows: list[dict]) -> dict:
    stations: dict[str, dict] = {}
    for row in rows:
        loc = row.get("location") or {}
        lat, lon = loc.get("latitude"), loc.get("longitude")
        map_id = row.get("map_id")
        if not (map_id and lat and lon):
            continue
        s = stations.setdefault(map_id, {"name": row.get("station_name", ""), "lat": [], "lon": [], "routes": set(),
                                         "ada": False})
        s["lat"].append(float(lat))
        s["lon"].append(float(lon))
        s["ada"] = s["ada"] or bool(row.get("ada"))
        s["routes"] |= {route for flag, route in STOP_FLAGS.items() if row.get(flag) is True}
    features = []
    for map_id, s in sorted(stations.items()):
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [round(sum(s["lon"]) / len(s["lon"]), 5),
                                                          round(sum(s["lat"]) / len(s["lat"]), 5)]},
            "properties": {"station_id": map_id, "name": s["name"],
                           "routes": [r for r in ROUTES if r in s["routes"]], "ada": s["ada"]},
        })
    return {"type": "FeatureCollection", "features": features}


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    lines = build_lines(fetch(LINES_URL))
    stations = build_stations(fetch(STOPS_URL))
    if len(lines["features"]) < 100 or len(stations["features"]) < 100:
        print("Unexpectedly few features - refusing to overwrite the committed files.")
        return 1
    (OUT_DIR / "cta_lines.geojson").write_text(json.dumps(lines, separators=(",", ":")), encoding="utf-8")
    (OUT_DIR / "cta_stations.geojson").write_text(json.dumps(stations, separators=(",", ":")), encoding="utf-8")
    print(f"lines: {len(lines['features'])} route-segments; stations: {len(stations['features'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
