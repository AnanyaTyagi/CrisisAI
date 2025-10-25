import os, asyncio, httpx
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any, Optional

def _now():
    return datetime.now(timezone.utc)

# ---- USGS quakes (GeoJSON summary feed) ----
async def usgs_stream(poll_secs: int = 15) -> AsyncGenerator[Dict[str, Any], None]:
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
    etag: Optional[str] = None
    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            headers = {"If-None-Match": etag} if etag else {}
            r = await client.get(url, headers=headers)
            if r.status_code == 200:
                etag = r.headers.get("ETag", etag)
                j = r.json()
                for f in j.get("features", []):
                    pid = f.get("id")
                    props = f.get("properties", {})
                    coords = (f.get("geometry", {}) or {}).get("coordinates", [None, None])
                    yield {
                        "id": f"usgs:{pid}",
                        "source": "usgs",
                        "received_at": _now().isoformat(),
                        "lat": coords[1],
                        "lon": coords[0],
                        "when": datetime.fromtimestamp(props.get("time", 0)/1000, tz=timezone.utc).isoformat(),
                        "hazard": "earthquake",
                        "payload": props
                    }
            await asyncio.sleep(poll_secs)

# ---- NWS CAP alerts (active) ----
async def nws_stream(area: str = "CA", poll_secs: int = 60) -> AsyncGenerator[Dict[str, Any], None]:
    base = "https://api.weather.gov/alerts/active"
    params = {"area": area} if area else {}
    async with httpx.AsyncClient(timeout=10, headers={"User-Agent": "gov-dashboard/1.0"}) as client:
        while True:
            r = await client.get(base, params=params)
            if r.status_code == 200:
                j = r.json()
                for f in j.get("features", []):
                    pid = f.get("id")
                    props = f.get("properties", {})
                    # many alerts have polygons; take centroid if present
                    geom = (f.get("geometry") or {})
                    coords = geom.get("coordinates")
                    lat = lon = None
                    if coords and geom.get("type") == "Polygon":
                        pts = coords[0]
                        if pts:
                            lon = sum(p[0] for p in pts)/len(pts)
                            lat = sum(p[1] for p in pts)/len(pts)
                    yield {
                        "id": f"nws:{pid}",
                        "source": "nws",
                        "received_at": _now().isoformat(),
                        "lat": lat, "lon": lon,
                        "when": props.get("onset") or props.get("effective") or props.get("sent"),
                        "hazard": "weather_alert",
                        "payload": props
                    }
            await asyncio.sleep(poll_secs)

# ---- FIRMS active fires (region) ----
# Note: public NRT CSV endpoints exist; we consume the geojson-like JSON for simplicity if available.
async def firms_stream(region: str = "north_america", poll_secs: int = 900) -> AsyncGenerator[Dict[str, Any], None]:
    # Example public JSON (adjust as needed to your FIRMS access pattern)
    # You may need to adapt to CSV if JSON isn't provided in your region.
    urls = {
        "global": "https://firms.modaps.eosdis.nasa.gov/mapserver/wfs/viirs/?day=1",
        "north_america": "https://firms.modaps.eosdis.nasa.gov/mapserver/wfs/viirs?country=USA&day=1"
    }
    url = urls.get(region, urls["global"])
    async with httpx.AsyncClient(timeout=20) as client:
        while True:
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    j = r.json()
                    # Fallback: if you use CSV, parse it instead. Core idea: yield one per fire point.
                    for feat in j.get("features", []):
                        pid = feat.get("id") or feat.get("properties", {}).get("id", "")
                        coords = (feat.get("geometry") or {}).get("coordinates", [None, None])
                        props = feat.get("properties", {})
                        yield {
                            "id": f"firms:{pid}",
                            "source": "firms",
                            "received_at": _now().isoformat(),
                            "lat": coords[1], "lon": coords[0],
                            "when": _now().isoformat(),  # if none in feed, stamp now
                            "hazard": "fire",
                            "payload": props
                        }
            except Exception:
                pass
            await asyncio.sleep(poll_secs)
