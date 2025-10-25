from typing import Dict, Any, Tuple
from math import radians, sin, cos, asin, sqrt

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    if None in (lat1, lon1, lat2, lon2):
        return 1e12
    R = 6371000.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    return 2*R*asin(sqrt(a))

def fuse_score(basis: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    sources = basis.get("sources", {})
    hazard = basis.get("hazard")
    score = 0.0

    if hazard == "earthquake" and "usgs" in sources:
        return 1.0, sources

    if hazard == "fire":
        has_cam  = "camera" in sources
        has_firm = "firms" in sources
        has_nws  = "nws" in sources

        if has_cam and has_firm:
            f, c = sources["firms"], sources["camera"]
            close = haversine_m(f["lat"], f["lon"], c["lat"], c["lon"]) < 20000
            score = 0.95 if close else 0.90
        elif has_cam:
            score = 0.80            # <-- promotes camera-only
        elif has_firm and has_nws:
            f, n = sources["firms"], sources["nws"]
            close = haversine_m(f["lat"], f["lon"], n["lat"], n["lon"]) < 20000
            score = 0.85 if close else 0.75
        elif has_firm:
            score = 0.60
        elif has_nws:
            score = 0.50
        return score, sources

    if "nws" in sources:
        return 0.60, sources

    return 0.0, sources
