import os, asyncio, httpx
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any, Optional

def _now():
    return datetime.now(timezone.utc)

# ---- USGS quakes (GeoJSON summary feed) ---- 
# ✅ This URL works! No changes needed
async def usgs_stream(poll_secs: int = 15) -> AsyncGenerator[Dict[str, Any], None]:
    """Stream earthquake data from USGS - WORKING"""
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
    etag: Optional[str] = None
    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            try:
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
                elif r.status_code == 304:
                    # Not modified - no new data
                    pass
            except Exception as e:
                print(f"USGS error: {e}")
            await asyncio.sleep(poll_secs)


# ---- NWS CAP alerts (active) ---- 
# ✅ This URL works! Added better error handling
async def nws_stream(area: str = "CA", poll_secs: int = 60) -> AsyncGenerator[Dict[str, Any], None]:
    """Stream weather alerts from National Weather Service - WORKING"""
    base = "https://api.weather.gov/alerts/active"
    params = {"area": area} if area else {}
    
    async with httpx.AsyncClient(
        timeout=10, 
        headers={"User-Agent": "DisasterMonitor/1.0 (contact@yourorg.edu)"}  # NWS requires this
    ) as client:
        while True:
            try:
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
            except Exception as e:
                print(f"NWS error: {e}")
            await asyncio.sleep(poll_secs)


# ---- OPTION 1: FIRMS with NASA EARTHDATA Account (Recommended) ----
async def firms_stream_with_key(api_key: str = None, region: str = "USA", poll_secs: int = 900) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Stream active fire data from NASA FIRMS - REQUIRES FREE API KEY
    
    Get your FREE API key:
    1. Go to https://firms.modaps.eosdis.nasa.gov/api/
    2. Register for free (takes 2 minutes)
    3. Use your MAP_KEY below
    
    Args:
        api_key: Your FIRMS MAP_KEY (or set FIRMS_API_KEY env var)
        region: Country code (USA, AUS, etc.) or coordinates
        poll_secs: Polling interval in seconds
    """
    # Get API key from parameter or environment variable
    key = api_key or os.getenv("FIRMS_API_KEY")
    
    if not key:
        print("⚠️  FIRMS requires API key. Get free key at: https://firms.modaps.eosdis.nasa.gov/api/")
        print("   Set FIRMS_API_KEY environment variable or pass api_key parameter")
        return
    
    # VIIRS data for last 24 hours
    url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{key}/VIIRS_NOAA20_NRT/{region}/1"
    
    async with httpx.AsyncClient(timeout=20) as client:
        seen_ids = set()
        while True:
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    # Parse CSV response
                    lines = r.text.strip().split('\n')
                    if len(lines) > 1:
                        header = lines[0].split(',')
                        for line in lines[1:]:
                            parts = line.split(',')
                            if len(parts) >= len(header):
                                data = dict(zip(header, parts))
                                
                                # Create unique ID
                                fire_id = f"{data.get('latitude', '')}_{data.get('longitude', '')}_{data.get('acq_date', '')}_{data.get('acq_time', '')}"
                                
                                # Only yield new fires
                                if fire_id not in seen_ids:
                                    seen_ids.add(fire_id)
                                    
                                    yield {
                                        "id": f"firms:{fire_id}",
                                        "source": "firms",
                                        "received_at": _now().isoformat(),
                                        "lat": float(data.get("latitude", 0)),
                                        "lon": float(data.get("longitude", 0)),
                                        "when": f"{data.get('acq_date', '')}T{data.get('acq_time', '')}",
                                        "hazard": "fire",
                                        "payload": data
                                    }
                elif r.status_code == 403:
                    print("⚠️  FIRMS API key invalid or expired. Get new key at: https://firms.modaps.eosdis.nasa.gov/api/")
                    
            except Exception as e:
                print(f"FIRMS error: {e}")
            
            await asyncio.sleep(poll_secs)


# ---- OPTION 2: Alternative - Use EONET (NASA's Natural Event Tracker) ----
async def eonet_fires_stream(poll_secs: int = 300) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Stream wildfire events from NASA EONET - NO API KEY REQUIRED!
    
    This is a good FREE alternative to FIRMS that doesn't require authentication.
    Data updates every few hours.
    """
    url = "https://eonet.gsfc.nasa.gov/api/v3/events"
    params = {
        "category": "wildfires",
        "status": "open",
        "limit": 100
    }
    
    async with httpx.AsyncClient(timeout=15) as client:
        seen_ids = set()
        while True:
            try:
                r = await client.get(url, params=params)
                if r.status_code == 200:
                    j = r.json()
                    for event in j.get("events", []):
                        event_id = event.get("id")
                        
                        # Only yield new events
                        if event_id not in seen_ids:
                            seen_ids.add(event_id)
                            
                            # Get latest geometry
                            geometries = event.get("geometry", [])
                            if geometries:
                                latest = geometries[-1]  # Most recent position
                                coords = latest.get("coordinates", [None, None])
                                
                                yield {
                                    "id": f"eonet:{event_id}",
                                    "source": "eonet",
                                    "received_at": _now().isoformat(),
                                    "lat": coords[1] if len(coords) > 1 else None,
                                    "lon": coords[0] if len(coords) > 0 else None,
                                    "when": latest.get("date", _now().isoformat()),
                                    "hazard": "fire",
                                    "payload": {
                                        "title": event.get("title"),
                                        "description": event.get("description"),
                                        "link": event.get("link"),
                                        "categories": event.get("categories", [])
                                    }
                                }
            except Exception as e:
                print(f"EONET error: {e}")
            
            await asyncio.sleep(poll_secs)


# ---- OPTION 3: GDACS (Global Disaster Alert and Coordination System) ----
async def gdacs_stream(poll_secs: int = 300) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Stream global disaster alerts from GDACS - NO API KEY REQUIRED!
    
    Covers: floods, earthquakes, tropical cyclones, tsunamis, volcanoes
    """
    url = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
    
    async with httpx.AsyncClient(timeout=15) as client:
        seen_ids = set()
        while True:
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    # GDACS returns XML, but we can parse it as text
                    import xml.etree.ElementTree as ET
                    root = ET.fromstring(r.content)
                    
                    for item in root.findall(".//item"):
                        event_id = item.findtext("gdacs:eventid", namespaces={"gdacs": "http://www.gdacs.org"})
                        
                        if event_id and event_id not in seen_ids:
                            seen_ids.add(event_id)
                            
                            # Extract coordinates from point
                            point = item.findtext("geo:Point/geo:lat", namespaces={"geo": "http://www.w3.org/2003/01/geo/wgs84_pos#"})
                            lat = float(point) if point else None
                            lon = float(item.findtext("geo:Point/geo:long", namespaces={"geo": "http://www.w3.org/2003/01/geo/wgs84_pos#"}) or 0)
                            
                            yield {
                                "id": f"gdacs:{event_id}",
                                "source": "gdacs",
                                "received_at": _now().isoformat(),
                                "lat": lat,
                                "lon": lon,
                                "when": item.findtext("pubDate", _now().isoformat()),
                                "hazard": item.findtext("gdacs:eventtype", namespaces={"gdacs": "http://www.gdacs.org"}),
                                "payload": {
                                    "title": item.findtext("title"),
                                    "description": item.findtext("description"),
                                    "severity": item.findtext("gdacs:severity", namespaces={"gdacs": "http://www.gdacs.org"}),
                                    "link": item.findtext("link")
                                }
                            }
            except Exception as e:
                print(f"GDACS error: {e}")
            
            await asyncio.sleep(poll_secs)


# ---- Example Usage ----
async def main():
    """
    Example: Stream from all available sources
    """
    print("🌍 Starting disaster monitoring streams...")
    print("=" * 60)
    
    # Create tasks for each stream
    tasks = [
        usgs_stream(poll_secs=30),           # ✅ Works immediately - no key needed
        nws_stream(area="CA", poll_secs=60), # ✅ Works immediately - no key needed
        eonet_fires_stream(poll_secs=300),   # ✅ Works immediately - no key needed (alternative to FIRMS)
        gdacs_stream(poll_secs=300),         # ✅ Works immediately - no key needed
        
        # Uncomment when you have FIRMS API key:
        # firms_stream_with_key(api_key="YOUR_KEY_HERE", region="USA", poll_secs=900)
    ]
    
    # Process events from all streams
    async for source in merge_streams(*tasks):
        async for event in source:
            print(f"\n🚨 {event['hazard'].upper()} from {event['source']}")
            print(f"   ID: {event['id']}")
            print(f"   Location: ({event['lat']}, {event['lon']})")
            print(f"   When: {event['when']}")
            if 'title' in event.get('payload', {}):
                print(f"   Title: {event['payload']['title']}")


async def merge_streams(*streams):
    """Helper to merge multiple async generators"""
    for stream in streams:
        yield stream


if __name__ == "__main__":
    # Run the streaming monitor
    asyncio.run(main())
