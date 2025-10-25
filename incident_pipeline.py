#!/usr/bin/env python3
"""
Disaster-response multi-agent system using Letta.
Agents only coordinate. All tools (data fetch + simulation) run outside Letta.

What’s new:
- Multi-hazard SimulationAgent (fire, gas, flood, earthquake; traffic = placeholder)
- Live inputs:
  * Open-Meteo (wind/humidity/temp/precip) — no API key required
  * Open-Elevation (terrain slope)
  * USGS Earthquakes (nearest recent event & Shake-like impact bands)
- Dispatcher picks the right predictive module based on accident_type.
"""

import os
import json
import math
import time
from typing import Dict, Any, List, Optional, Tuple

from dotenv import load_dotenv
from letta_client import Letta
from letta_client.types import CreateBlock

load_dotenv()

# =====================================================================================
#                                    DATA HELPERS
# =====================================================================================

def fetch_weather_open_meteo(lat: float, lon: float) -> Dict[str, Any]:
    """
    Open-Meteo: current & hourly forecast without API key.
    Returns wind speed (m/s), wind dir (deg), humidity (%), temp (C), precip (mm/h).
    Falls back to sensible defaults if call fails.
    """
    defaults = {
        "ok": True,
        "source": "defaults",
        "wind_speed_mps": 5.0,
        "wind_dir_deg": None,
        "humidity": 25.0,
        "temp_c": 22.0,
        "precip_mmph": 0.0,
    }
    try:
        import requests
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,precipitation",
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,precipitation",
            "wind_speed_unit": "ms",
            "precipitation_unit": "mm",
            "timezone": "UTC",
        }
        r = requests.get(url, params=params, timeout=12)
        r.raise_for_status()
        j = r.json()
        cur = j.get("current", {})
        return {
            "ok": True,
            "source": "open-meteo",
            "wind_speed_mps": float(cur.get("wind_speed_10m", defaults["wind_speed_mps"])),
            "wind_dir_deg": float(cur["wind_direction_10m"]) if "wind_direction_10m" in cur else None,
            "humidity": float(cur.get("relative_humidity_2m", defaults["humidity"])),
            "temp_c": float(cur.get("temperature_2m", defaults["temp_c"])),
            "precip_mmph": float(cur.get("precipitation", defaults["precip_mmph"])),
        }
    except Exception:
        return defaults


def fetch_slope_open_elevation(lat: float, lon: float) -> Dict[str, Any]:
    """
    Estimate local terrain slope (deg) by sampling Open-Elevation at a small cross around (lat,lon).
    Falls back to 5° if service fails.
    """
    fallback = {"ok": True, "slope_deg": 5.0, "source": "default"}
    try:
        import requests
        dlat = 0.0008  # ~89 m
        # lon spacing scaled by cos(lat) to get ~89 m
        dlon = 0.0008 / max(0.3, math.cos(math.radians(lat)))
        pts = [
            (lat, lon),
            (lat + dlat, lon), (lat - dlat, lon),
            (lat, lon + dlon), (lat, lon - dlon),
        ]
        locs = "|".join([f"{p[0]:.6f},{p[1]:.6f}" for p in pts])
        url = "https://api.open-elevation.com/api/v1/lookup"
        r = requests.get(url, params={"locations": locs}, timeout=12)
        r.raise_for_status()
        res = r.json().get("results", [])
        if len(res) < 5:
            return fallback
        zn, zs, ze, zw = res[1]["elevation"], res[2]["elevation"], res[3]["elevation"], res[4]["elevation"]
        m_per_deg_lat = 111_320.0
        m_per_deg_lon = 111_320.0 * math.cos(math.radians(lat))
        dz_dy = (zn - zs) / (2 * dlat * m_per_deg_lat)
        dz_dx = (ze - zw) / (2 * dlon * m_per_deg_lon)
        slope_rad = math.atan(math.sqrt(dz_dx**2 + dz_dy**2))
        slope_deg = round(math.degrees(slope_rad), 2)
        return {"ok": True, "slope_deg": slope_deg, "source": "open-elevation"}
    except Exception:
        return fallback


def fetch_nearby_usgs_event(lat: float, lon: float, radius_km: int = 200, days_back: int = 2) -> Dict[str, Any]:
    """
    USGS Earthquake API: get the strongest event near (lat,lon) in the last X days within radius_km.
    Returns magnitude, depth, epicenter, time. If none, ok=False.
    """
    try:
        import requests
        from datetime import datetime, timedelta, timezone
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days_back)
        url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
        params = {
            "format": "geojson",
            "latitude": lat,
            "longitude": lon,
            "maxradiuskm": radius_km,
            "starttime": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "endtime": end.strftime("%Y-%m-%dT%H:%M:%S"),
            "orderby": "magnitude",
            "limit": 1,
        }
        r = requests.get(url, params=params, timeout=12)
        r.raise_for_status()
        j = r.json()
        feats = j.get("features", [])
        if not feats:
            return {"ok": False, "reason": "no_recent_events"}
        f = feats[0]
        props = f.get("properties", {})
        coords = f.get("geometry", {}).get("coordinates", [None, None, None])
        return {
            "ok": True,
            "source": "usgs",
            "magnitude": props.get("mag"),
            "time_ms": props.get("time"),
            "epicenter": {"lon": coords[0], "lat": coords[1], "depth_km": coords[2]},
            "place": props.get("place"),
            "ids": props.get("ids"),
            "url": props.get("url"),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

# =====================================================================================
#                                EXISTING CONTEXT TOOLS
# =====================================================================================

def action_get_population_context(lat: float, lon: float, accident_type: str, year: int = 2020) -> Dict[str, Any]:
    """WorldPop population density at incident location (people/km^2)."""
    try:
        import requests
        from datetime import datetime, timezone
        geometry = {"x": float(lon), "y": float(lat), "spatialReference": {"wkid": 4326}}
        dt_obj = datetime(int(year), 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        epoch_ms = int(dt_obj.timestamp() * 1000)
        params = {
            "f": "json",
            "geometry": json.dumps(geometry),
            "geometryType": "esriGeometryPoint",
            "returnFirstValueOnly": "true",
            "interpolation": "RSP_NearestNeighbor",
            "time": epoch_ms
        }
        url = "https://worldpop.arcgis.com/arcgis/rest/services/WorldPop_Population_Density_1km/ImageServer/getSamples"
        response = requests.post(url, data=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        samples = data.get("samples") or data.get("samplingResults")
        if not samples:
            return {"accident_type": accident_type, "density": None, "units": "people per km^2", "ok": False,
                    "meta": {"error": "No sample returned"}}
        sample = samples[0]
        value = sample.get("value", sample.get("sampleValue"))
        if value is None:
            return {"accident_type": accident_type, "density": None, "units": "people per km^2", "ok": False,
                    "meta": {"error": "No numeric value"}}
        return {"accident_type": accident_type, "density": float(value), "units": "people per km^2",
                "ok": True, "meta": {"year": int(year), "source": "WorldPop_1km", "note": "nearest pixel"}}
    except Exception as e:
        return {"accident_type": accident_type, "density": None, "units": "people per km^2", "ok": False,
                "meta": {"error": str(e)}}


def resource_find_nearest(lat: float, lon: float, radius_m: int = 10000, auto_expand_radius: bool = True,
                          max_radius_m: int = 50000) -> Dict[str, Any]:
    """Find nearest fire/police/ambulance using OSM Overpass."""
    try:
        import requests
        endpoints = [
            "https://overpass-api.de/api/interpreter",
            "https://overpass.kumi.systems/api/interpreter",
            "https://overpass.openstreetmap.fr/api/interpreter",
        ]
        contact = os.getenv("OVERPASS_CONTACT", "changeme@example.com")
        headers = {"User-Agent": f"ca-disaster-tools/1.0 (contact: {contact})", "Accept": "application/json"}
        search_radius = radius_m
        while True:
            query = f"""
            [out:json][timeout:60];
            (
              node["amenity"="fire_station"](around:{search_radius},{lat},{lon});
              way["amenity"="fire_station"](around:{search_radius},{lat},{lon});
              relation["amenity"="fire_station"](around:{search_radius},{lat},{lon});
              node["amenity"="police"](around:{search_radius},{lat},{lon});
              way["amenity"="police"](around:{search_radius},{lat},{lon});
              relation["amenity"="police"](around:{search_radius},{lat},{lon});
              node["emergency"="ambulance_station"](around:{search_radius},{lat},{lon});
              way["emergency"="ambulance_station"](around:{search_radius},{lat},{lon});
              relation["emergency"="ambulance_station"](around:{search_radius},{lat},{lon});
            );
            out center tags;
            """
            data = None
            last_error = None
            for endpoint_url in endpoints:
                try:
                    resp = requests.post(endpoint_url, data={"data": query}, headers=headers, timeout=60)
                    if resp.status_code in (429, 504):
                        time.sleep(2)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                    if isinstance(data, dict) and "elements" in data:
                        break
                except Exception as e:
                    last_error = e
                    time.sleep(1)
            if data is None:
                raise RuntimeError(f"All Overpass mirrors failed: {last_error}")
            elements = data.get("elements", [])
            records = []
            for element in elements:
                tags = element.get("tags", {})
                amenity = tags.get("amenity")
                emergency = tags.get("emergency")
                if amenity == "fire_station":
                    category = "fire_station"
                elif amenity == "police":
                    category = "police"
                elif emergency == "ambulance_station":
                    category = "ambulance_station"
                else:
                    continue
                if "lat" in element and "lon" in element:
                    elem_lat, elem_lon = float(element["lat"]), float(element["lon"])
                else:
                    center = element.get("center", {})
                    if "lat" in center and "lon" in center:
                        elem_lat, elem_lon = float(center["lat"]), float(center["lon"])
                    else:
                        continue
                # Haversine distance
                lat1_r = math.radians(lat); lat2_r = math.radians(elem_lat)
                dlat = math.radians(elem_lat - lat); dlon = math.radians(elem_lon - lon)
                a = math.sin(dlat/2)**2 + math.cos(lat1_r)*math.cos(lat2_r)*math.sin(dlon/2)**2
                distance_m = round(6371000.0 * 2 * math.asin(math.sqrt(a)), 1)
                addr_parts = [tags.get(k) for k in
                              ["addr:housenumber", "addr:street", "addr:city", "addr:state", "addr:postcode"] if tags.get(k)]
                address = ", ".join(addr_parts) if addr_parts else None
                records.append({
                    "id": f"{element.get('type','')}/{element.get('id','')}",
                    "category": category,
                    "name": tags.get("name"),
                    "operator": tags.get("operator"),
                    "address": address,
                    "phone": tags.get("phone") or tags.get("contact:phone"),
                    "website": tags.get("website") or tags.get("contact:website"),
                    "opening_hours": tags.get("opening_hours"),
                    "lat": elem_lat, "lon": elem_lon, "distance_m": distance_m, "raw_tags": tags
                })
            nearest = {}
            for cat in ("fire_station", "ambulance_station", "police"):
                cat_recs = [r for r in records if r["category"] == cat]
                if cat_recs:
                    nearest[cat] = min(cat_recs, key=lambda r: r["distance_m"])
            if nearest or not auto_expand_radius or search_radius >= max_radius_m:
                return {"ok": True, "query_point": {"lat": lat, "lon": lon},
                        "search_radius_m": search_radius, "results": nearest,
                        "counts_in_radius": {
                            "fire_station": sum(1 for r in records if r["category"]=="fire_station"),
                            "ambulance_station": sum(1 for r in records if r["category"]=="ambulance_station"),
                            "police": sum(1 for r in records if r["category"]=="police"),
                            "total": len(records)}}
            search_radius = min(search_radius*2, max_radius_m)
            time.sleep(1)
    except Exception as e:
        return {"ok": False, "query_point": {"lat": lat, "lon": lon}, "error": str(e)}

# =====================================================================================
#                                HAZARD PREDICTIVE MODELS
# =====================================================================================

def simulate_fire_spread(lat: float, lon: float, wind_speed_mps: float, wind_dir_deg: Optional[float],
                         humidity: float, slope_deg: float, forecast_hours: List[float],
                         population_density: Optional[float]) -> Dict[str, Any]:
    """
    Simplified radial growth model for fire. Wind increases spread; humidity reduces base; slope increases rate.
    """
    base_R0 = max(0.01, 0.05 * (1 - humidity / 100.0))  # m/s
    wind_factor = 0.20
    slope_factor = 1.0 + (slope_deg / 90.0) * 0.25
    R = (base_R0 + wind_factor * wind_speed_mps) * slope_factor  # m/s
    forecast = {}
    for h in forecast_hours:
        dist_m = R * h * 3600.0
        area_km2 = math.pi * (dist_m/1000.0) ** 2
        forecast[f"t{int(h)}h"] = round(area_km2, 2)
    max_radius_km = round(R * max(forecast_hours) * 3.6, 2)
    pop = float(population_density) if population_density is not None else 1000.0
    risk_zones = [{"radius_km": r, "population_exposed": int(pop * math.pi * (r**2))} for r in [1,2,5]]
    return {
        "ok": True, "simulation_type": "fire_spread",
        "parameters": {"wind_speed_mps": wind_speed_mps, "wind_dir_deg": wind_dir_deg,
                       "humidity": humidity, "terrain_slope_deg": slope_deg},
        "predicted_area_km2": forecast, "max_radius_km": max_radius_km,
        "direction_of_spread_deg": wind_dir_deg, "risk_zone_estimates": risk_zones,
        "notes": ["First-order fire spread envelope; assumes homogeneous fuels/topography."]
    }


def simulate_gas_plume(lat: float, lon: float, wind_speed_mps: float, wind_dir_deg: Optional[float],
                       temp_c: float, stability_hint: Optional[str]=None,
                       hours: float = 3.0) -> Dict[str, Any]:
    """
    Very simple Gaussian-plume-style footprint proxy:
    radius scales with wind & time; bands represent decreasing concentration.
    """
    # crude “plume length” (km): L ~ k * wind * time; width ~ 0.4 * L
    k = 1.2  # tune by chemical type later
    L_km = round(k * wind_speed_mps * (hours * 3600) / 1000.0, 2)
    W_km = round(0.4 * L_km, 2)
    bands = [
        {"downwind_km": round(0.25*L_km,2), "crosswind_halfwidth_km": round(0.25*W_km,2), "label": "High"},
        {"downwind_km": round(0.50*L_km,2), "crosswind_halfwidth_km": round(0.50*W_km,2), "label": "Medium"},
        {"downwind_km": round(0.85*L_km,2), "crosswind_halfwidth_km": round(0.85*W_km,2), "label": "Low"},
    ]
    return {
        "ok": True, "simulation_type": "gas_plume",
        "parameters": {"wind_speed_mps": wind_speed_mps, "wind_dir_deg": wind_dir_deg,
                       "temp_c": temp_c, "stability": stability_hint, "horizon_h": hours},
        "footprint_bands": bands, "direction_of_spread_deg": wind_dir_deg,
        "notes": ["Proxy plume envelope; use full dispersion model for operations (e.g., ALOHA/CFD)."]
    }


def simulate_flood_spread(lat: float, lon: float, precip_mmph: float, slope_deg: float,
                          hours: float = 3.0) -> Dict[str, Any]:
    """
    Simple flood front proxy: faster progress with more rainfall and steeper slope.
    """
    # baseline front speed (m/s) increases with precipitation and slope
    base = 0.02  # m/s
    v = base + 0.006 * max(0.0, precip_mmph) + 0.0008 * max(0.0, slope_deg)
    dist_km = round(v * hours * 3600 / 1000.0, 2)
    area_km2 = round(math.pi * (dist_km ** 2), 2)
    return {
        "ok": True, "simulation_type": "flood_spread",
        "parameters": {"precip_mmph": precip_mmph, "terrain_slope_deg": slope_deg, "horizon_h": hours},
        "predicted_area_km2": {"t3h": area_km2}, "max_radius_km": dist_km,
        "notes": ["First-order flood-front proxy; for real ops use hydrodynamic models & river basins."]
    }


def estimate_quake_impact(usgs_event: Dict[str, Any], pop_density: Optional[float]) -> Dict[str, Any]:
    """
    Convert a USGS event to simple impact rings using a crude attenuation heuristic from magnitude.
    Not a ShakeMap; just an initial envelope for planning.
    """
    if not usgs_event.get("ok"):
        return {"ok": False, "simulation_type": "earthquake_impact", "reason": "no_recent_event"}
    M = float(usgs_event.get("magnitude", 0.0) or 0.0)
    # crude radii (km) for MMI-like rings (very rough order-of-magnitude envelopes)
    r_strong = round(max(5.0, (10 ** (0.25 * (M - 4.5))) * 15), 1)   # strong shaking
    r_moderate = round(r_strong * 1.8, 1)
    r_light = round(r_strong * 3.0, 1)
    pop = float(pop_density) if pop_density is not None else 1000.0
    rings = [
        {"radius_km": r_strong,   "label": "Strong",   "population_exposed": int(pop * math.pi * (r_strong**2))},
        {"radius_km": r_moderate, "label": "Moderate", "population_exposed": int(pop * math.pi * (r_moderate**2))},
        {"radius_km": r_light,    "label": "Light",    "population_exposed": int(pop * math.pi * (r_light**2))},
    ]
    return {
        "ok": True, "simulation_type": "earthquake_impact",
        "parameters": {"magnitude": M, "event_source": "usgs"},
        "impact_rings": rings,
        "event": usgs_event,
        "notes": ["Very coarse attenuation rings; rely on USGS ShakeMap when available."]
    }

# =====================================================================================
#                                    DISPATCHER
# =====================================================================================

def run_predictive_module(incident: Dict[str, Any],
                          pop_density: Optional[float]) -> Dict[str, Any]:
    """
    Chooses the right predictive model and gathers the specific live inputs it needs.
    """
    lat = incident["lat"]; lon = incident["lon"]
    atype = (incident["accident_type"] or "").lower()

    # Common live data
    wx = fetch_weather_open_meteo(lat, lon)         # wind, humidity, temp, precip
    slope_res = fetch_slope_open_elevation(lat, lon)  # terrain slope

    # FIRE
    if "fire" in atype or "wildfire" in atype or "urban fire" in atype:
        return simulate_fire_spread(
            lat=lat, lon=lon,
            wind_speed_mps=wx.get("wind_speed_mps", 5.0),
            wind_dir_deg=wx.get("wind_dir_deg"),
            humidity=wx.get("humidity", 25.0),
            slope_deg=slope_res.get("slope_deg", 5.0),
            forecast_hours=[1, 3, 6],
            population_density=pop_density
        )

    # GAS / HAZMAT
    if "gas" in atype or "chemical" in atype or "hazmat" in atype:
        # (stability_hint could be inferred from wind+diurnal cycle later)
        return simulate_gas_plume(
            lat=lat, lon=lon,
            wind_speed_mps=wx.get("wind_speed_mps", 5.0),
            wind_dir_deg=wx.get("wind_dir_deg"),
            temp_c=wx.get("temp_c", 22.0),
            stability_hint=None,
            hours=3.0
        )

    # FLOOD / WATER MAIN
    if "flood" in atype or "water main" in atype or "inundation" in atype:
        return simulate_flood_spread(
            lat=lat, lon=lon,
            precip_mmph=wx.get("precip_mmph", 0.0),
            slope_deg=slope_res.get("slope_deg", 5.0),
            hours=3.0
        )

    # EARTHQUAKE
    if "earthquake" in atype or "seismic" in atype:
        usgs_event = fetch_nearby_usgs_event(lat, lon, radius_km=200, days_back=2)
        return estimate_quake_impact(usgs_event, pop_density)

    # TRAFFIC / COLLISION (ripple effects) — placeholder (kept simple)
    if "vehicle" in atype or "collision" in atype or "traffic" in atype:
        # You can add a traffic ETA degradation model here using OSM network + (optional) live speeds
        return {
            "ok": True, "simulation_type": "traffic_ripple",
            "parameters": {"note": "Placeholder model"},
            "notes": ["Add network-based congestion propagation if you have a traffic feed."]
        }

    return {"ok": False, "error": f"No predictive model wired for '{incident['accident_type']}' yet."}

# =====================================================================================
#                                   LETTA CLIENT
# =====================================================================================

def make_client() -> Letta:
    token = os.getenv("LETTA_API_KEY")
    if token:
        return Letta(token=token)
    return Letta(base_url="http://localhost:8283")

def create_agents(client: Letta):
    # Shared incident context block
    incident_block = client.blocks.create(label="incident_context", value="(empty)")

    action_persona = CreateBlock(
        label="persona",
        value=(
            "You are ActionAgent. Coordinate population density assessment.\n"
            "When given incident details (lat, lon, accident_type):\n"
            "1) Acknowledge location & type\n"
            "2) Say you are analyzing population density\n"
            "3) Reply: READY_FOR_POPULATION_DATA\n"
            "Keep responses brief & structured."
        )
    )

    resource_persona = CreateBlock(
        label="persona",
        value=(
            "You are ResourceAgent. Coordinate nearest emergency resources.\n"
            "When given incident details (lat, lon):\n"
            "1) Acknowledge location\n"
            "2) Say you are searching for nearest resources\n"
            "3) Reply: READY_FOR_RESOURCE_DATA\n"
            "Keep responses brief & structured."
        )
    )

    simulation_persona = CreateBlock(
        label="persona",
        value=(
            "You are SimulationAgent. Coordinate predictive hazard analysis (fire, gas, flood, earthquake, traffic).\n"
            "When given incident details and context:\n"
            "1) Acknowledge you will forecast spatial/impact spread over time\n"
            "2) Reply: READY_FOR_MULTI_HAZARD_SIMULATION\n"
            "Keep responses brief & structured."
        )
    )

    planner_persona = CreateBlock(
        label="persona",
        value=(
            "You are PlannerAgent. Create a comprehensive disaster response plan.\n"
            "Inputs:\n"
            "- Incident details (lat, lon, accident_type)\n"
            "- Population density data\n"
            "- Nearest resources data\n"
            "- Simulation results (hazard-specific)\n"
            "Output ONLY valid JSON with:\n"
            "{\n"
            '  "incident": {lat, lon, accident_type},\n'
            '  "situational_awareness": {population_density, units, notes},\n'
            '  "resources": {ambulance_station, fire_station, police},\n'
            '  "simulation": {...hazard-specific result...},\n'
            '  "recommended_steps": [{step_no, action, rationale, priority}],\n'
            '  "comms": {notify: [], missing_info: []}\n'
            "}\n"
            "Handle missing data gracefully (show as 'Not available').\n"
            "Output ONLY JSON; no extra text."
        )
    )

    action_agent = client.agents.create(
        name="ActionAgent", model="openai/gpt-4o-mini", embedding="openai/text-embedding-3-small",
        memory_blocks=[action_persona], block_ids=[incident_block.id], include_base_tools=False, tags=["role:action"]
    )
    resource_agent = client.agents.create(
        name="ResourceAgent", model="openai/gpt-4o-mini", embedding="openai/text-embedding-3-small",
        memory_blocks=[resource_persona], block_ids=[incident_block.id], include_base_tools=False, tags=["role:resource"]
    )
    simulation_agent = client.agents.create(
        name="SimulationAgent", model="openai/gpt-4o-mini", embedding="openai/text-embedding-3-small",
        memory_blocks=[simulation_persona], block_ids=[incident_block.id], include_base_tools=False, tags=["role:simulation"]
    )
    planner_agent = client.agents.create(
        name="PlannerAgent", model="openai/gpt-4o-mini", embedding="openai/text-embedding-3-small",
        memory_blocks=[planner_persona], block_ids=[incident_block.id], include_base_tools=False, tags=["role:planner"]
    )

    return incident_block, action_agent, resource_agent, simulation_agent, planner_agent

# =====================================================================================
#                                   ORCHESTRATION
# =====================================================================================

def run_incident(lat: float, lon: float, accident_type: str):
    print("=" * 80)
    print(f"DISASTER RESPONSE SYSTEM - INCIDENT REPORT (Letta)")
    print(f"Location: {lat}, {lon}")
    print(f"Type: {accident_type}")
    print("=" * 80)

    client = make_client()
    incident_block, action_agent, resource_agent, simulation_agent, planner_agent = create_agents(client)

    # Update incident context
    client.blocks.modify(
        block_id=incident_block.id,
        value=json.dumps({"lat": lat, "lon": lon, "accident_type": accident_type})
    )

    # STEP 1: Population
    print("\n[STEP 1] Running ActionAgent...")
    action_prompt = f"Coordinate population assessment for lat={lat}, lon={lon}, type='{accident_type}'"
    action_resp = client.agents.messages.create(agent_id=action_agent.id,
                                                messages=[{"role": "user", "content": action_prompt}],
                                                request_options={"timeout_in_seconds": 60})
    for m in action_resp.messages:
        if m.message_type == "assistant_message":
            print(f"[ActionAgent] {m.content}")

    print("[EXTERNAL] Fetching population density...")
    pop_res = action_get_population_context(lat, lon, accident_type)
    print(f"[Population Result]\n{json.dumps(pop_res, indent=2)}\n")

    # STEP 2: Resources
    print("\n[STEP 2] Running ResourceAgent...")
    resource_prompt = f"Coordinate resource search for lat={lat}, lon={lon}"
    resource_resp = client.agents.messages.create(agent_id=resource_agent.id,
                                                  messages=[{"role": "user", "content": resource_prompt}],
                                                  request_options={"timeout_in_seconds": 60})
    for m in resource_resp.messages:
        if m.message_type == "assistant_message":
            print(f"[ResourceAgent] {m.content}")

    print("[EXTERNAL] Looking up nearest resources...")
    res_res = resource_find_nearest(lat, lon)
    print(f"[Resources Result]\n{json.dumps(res_res, indent=2)}\n")

    # STEP 3: Multi-hazard Simulation
    print("\n[STEP 3] Running SimulationAgent...")
    sim_prompt = "Run multi-hazard predictive analysis (fire/gas/flood/earthquake/traffic)."
    sim_resp = client.agents.messages.create(agent_id=simulation_agent.id,
                                             messages=[{"role": "user", "content": sim_prompt}],
                                             request_options={"timeout_in_seconds": 60})
    for m in sim_resp.messages:
        if m.message_type == "assistant_message":
            print(f"[SimulationAgent] {m.content}")

    print("[EXTERNAL] Running predictive module...")
    pop_density = pop_res.get("density") if pop_res.get("ok") else None
    incident = {"lat": lat, "lon": lon, "accident_type": accident_type}
    sim_out = run_predictive_module(incident, pop_density)
    print(f"[Simulation Result]\n{json.dumps(sim_out, indent=2)}\n")

    # STEP 4: Planner
    print("\n[STEP 4] Running PlannerAgent...")
    planner_prompt = f"""Create a comprehensive disaster response plan.

Incident Details:
- Latitude: {lat}
- Longitude: {lon}
- Type: {accident_type}

Population Data:
{json.dumps(pop_res, indent=2)}

Resource Data:
{json.dumps(res_res, indent=2)}

Simulation Result:
{json.dumps(sim_out, indent=2)}

Output ONLY JSON as specified earlier."""
    final_resp = client.agents.messages.create(agent_id=planner_agent.id,
                                               messages=[{"role": "user", "content": planner_prompt}],
                                               request_options={"timeout_in_seconds": 120})

    print("\n" + "=" * 80)
    print("FINAL DISASTER RESPONSE PLAN")
    print("=" * 80)
    for m in final_resp.messages:
        if m.message_type == "assistant_message":
            print(m.content)
    print("=" * 80)

# =====================================================================================
#                                        MAIN
# =====================================================================================

if __name__ == "__main__":
    # Change these to test different hazards:
    # "urban fire", "wildfire", "gas leak", "chemical spill", "flood", "earthquake",
    # "multi-vehicle collision"
    run_incident(
        lat=34.052682,
        lon=-118.244909,
        accident_type="urban fire"
    )
