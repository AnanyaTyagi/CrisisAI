# import os
# import asyncio
# import json
# import uuid
# from datetime import datetime, timezone
# from typing import Any, Dict, Set
#
# from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body
# from pydantic import BaseModel
# from dotenv import load_dotenv
#
# # --- package-local imports ---
# from .models import RawEvent, ConfirmedIncident
# from .storage import init_db, upsert_raw_event, insert_incident
# from .fusion import fuse_score
#
# # top of app.py (existing block)
# from .ingestors import (
#     usgs_stream,
#     nws_stream,
#     eonet_fires_stream,
#     gdacs_stream,
#     firms_stream_with_key,
# )
#
# # ========= Handoff (Agent JSON) =========
# from pathlib import Path
# import tempfile
#
# OUTBOX_DIR = Path(os.getenv("AGENT_OUTBOX_DIR", "agent_outbox")).resolve()
# OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
#
# def _atomic_write_json(path: Path, payload: dict):
#     """Write JSON atomically: write to temp file, then rename."""
#     tmp_fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_", suffix=".json")
#     try:
#         with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
#             json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), default=_json_default)
#         Path(tmp_path).replace(path)
#     finally:
#         try:
#             Path(tmp_path).unlink(missing_ok=True)
#         except Exception:
#             pass
#
# def write_agent_handoff(incident: dict, simulation: dict, *, origin: str, handoff_id: str | None = None) -> Path:
#     """
#     Create a JSON handoff file for the agent runner to consume.
#
#     Schema (top-level):
#       id:           unique id for this handoff
#       origin:       'trigger' | 'realtime'
#       created_utc:  ISO string
#       incident:     { lat, lon, accident_type }
#       simulation:   sim block from run_predictive_module (optional/partial OK)
#
#     File name: HANDOFF_<id>.json
#     """
#     hid = handoff_id or f"{origin}-{uuid.uuid4().hex[:10]}"
#     handoff = {
#         "id": hid,
#         "origin": origin,
#         "created_utc": now_iso(),
#         "incident": {
#             "lat": float(incident["lat"]),
#             "lon": float(incident["lon"]),
#             "accident_type": str(incident["accident_type"]),
#         },
#         "simulation": simulation or {},
#     }
#     out_path = OUTBOX_DIR / f"HANDOFF_{hid}.json"
#     _atomic_write_json(out_path, handoff)
#     print(f"[Handoff] wrote {out_path}")
#     return out_path
#
# # Camera ingestion is OPTIONAL
# try:
#     from .camera_ingestor import multi_camera_stream
#     HAS_CAMERA = True
# except Exception:
#     HAS_CAMERA = False
#
# # Import your dispatcher from the project root
# from incident_pipeline import run_predictive_module
#
# load_dotenv()
#
# app = FastAPI(title="Gov Realtime Detection")
# clients: Set[WebSocket] = set()  # active websocket clients
#
#
# # =========================
# # Helpers / Models (local)
# # =========================
#
# def now_iso() -> str:
#     return datetime.now(timezone.utc).isoformat()
#
#
# class WSMsg(BaseModel):
#     type: str
#     data: Dict[str, Any]
#
#
# def _json_default(o):
#     if isinstance(o, datetime):
#         return o.isoformat()
#     raise TypeError(f"Type not serializable: {type(o)}")
#
#
# async def broadcast(msg: Dict[str, Any]):
#     """Send a JSON-safe message to all connected WS clients, with logging."""
#     total = len(clients)
#     msg_type = msg.get("type") if isinstance(msg, dict) else None
#     if total == 0:
#         print(f"[WS] 0 clients connected; dropping message type={msg_type}")
#         return
#
#     delivered = 0
#     dead = []
#     for ws in list(clients):
#         try:
#             await ws.send_text(json.dumps(msg, default=_json_default))
#             delivered += 1
#         except WebSocketDisconnect:
#             dead.append(ws)
#         except Exception as e:
#             print(f"[WS][ERROR] send failed: {e}")
#             dead.append(ws)
#
#     for d in dead:
#         clients.discard(d)
#     print(f"[WS] delivered type={msg_type} to {delivered}/{total} clients")
#
#
# # =========================
# # Manual trigger (for tests)
# # =========================
#
# # @app.post("/trigger")
# # async def trigger(incident: dict = Body(...)):
# #     """
# #     POST JSON like:
# #     {"lat":34.0526,"lon":-118.2449,"accident_type":"urban fire"}
# #     """
# #     sim = run_predictive_module(incident, pop_density=None)
# #     return {"ok": True, "incident": incident, "simulation": sim}
#
#
# @app.post("/trigger")
# async def trigger(incident: dict = Body(...)):
#     """
#     POST JSON like:
#     {"lat":34.0526,"lon":-118.2449,"accident_type":"urban fire"}
#     """
#
#     sim = run_predictive_module(incident, pop_density=None)
#
#     handoff_path = write_agent_handoff(incident, sim, origin="trigger")
#
#     return {
#         "ok": True,
#         "incident": incident,
#         "simulation": sim,
#         "handoff_path": str(handoff_path),
#     }
#
# # =========================
# # Lifecycle
# # =========================
#
# @app.on_event("startup")
# async def on_start():
#     await init_db()
#     asyncio.create_task(run_ingestion())
#
#
# @app.websocket("/ws")
# async def ws_endpoint(ws: WebSocket):
#     await ws.accept()
#     clients.add(ws)
#     print(f"[WS] client connected; now {len(clients)} client(s)")
#     try:
#         while True:
#             await ws.receive_text()  # ignore client messages
#     except WebSocketDisconnect:
#         clients.discard(ws)
#         print(f"[WS] client disconnected; now {len(clients)} client(s)")
#
#
# # (Optional) quick debug endpoint to view recent incidents
# @app.get("/incidents")
# async def list_incidents(limit: int = 20):
#     # returns rows most-recent-first using storage.py’s schema
#     import aiosqlite
#     rows = []
#     async with aiosqlite.connect("realtime.sqlite") as db:
#         async with db.execute(
#             "SELECT id, hazard, when_utc, lat, lon, confidence "
#             "FROM incidents ORDER BY created_at DESC LIMIT ?",
#             (limit,)
#         ) as cur:
#             async for r in cur:
#                 rows.append({
#                     "id": r[0], "hazard": r[1], "when": r[2],
#                     "lat": r[3], "lon": r[4], "confidence": r[5],
#                 })
#     return {"incidents": rows}
#
#
# # =========================
# # Ingestion + Fusion Runner
# # =========================
#
# async def run_ingestion():
#     usgs_secs = int(os.getenv("USGS_POLL_SECS", "15"))
#     nws_secs = int(os.getenv("NWS_POLL_SECS", "60"))
#     firms_secs = int(os.getenv("FIRMS_POLL_SECS", "900"))
#     nws_area = os.getenv("NWS_AREA", "CA")
#     firms_reg = os.getenv("FIRMS_REGION", "north_america")
#
#     q: asyncio.Queue = asyncio.Queue()
#
#     async def feeder(gen):
#         async for e in gen:
#             # persist raw for audit
#             e_for_db = {**e, "payload_json": json.dumps(e.get("payload", {}))}
#             await upsert_raw_event(e_for_db)
#             await q.put(e)
#             # debug line so you can see feed activity
#             print(f"[Ingest] event -> q: src={e.get('source')} hazard={e.get('hazard')} lat={e.get('lat')} lon={e.get('lon')}")
#
#     # Start feed pollers
#     # --- Start feed pollers with toggles ---
#     if os.getenv("USE_USGS", "1") == "1":
#         asyncio.create_task(feeder(usgs_stream(usgs_secs)))
#     else:
#         print("[Init] USGS disabled via USE_USGS=0")
#
#     if os.getenv("USE_NWS", "1") == "1":
#         asyncio.create_task(feeder(nws_stream(nws_area, nws_secs)))
#     else:
#         print("[Init] NWS disabled via USE_NWS=0")
#
#     # NASA EONET (wildfires) — free, no key
#     if os.getenv("USE_EONET", "1") == "1":
#         asyncio.create_task(feeder(eonet_fires_stream(poll_secs=300)))
#     else:
#         print("[Init] EONET disabled via USE_EONET=0")
#
#     # GDACS (global multi-hazard) — free, no key
#     if os.getenv("USE_GDACS", "1") == "1":
#         asyncio.create_task(feeder(gdacs_stream(poll_secs=300)))
#     else:
#         print("[Init] GDACS disabled via USE_GDACS=0")
#
#     # FIRMS with key (optional)
#     firms_key = os.getenv("FIRMS_API_KEY")
#     if os.getenv("USE_FIRMS_KEY", "0") == "1" and firms_key:
#         region = os.getenv("FIRMS_REGION", "USA")
#         asyncio.create_task(feeder(firms_stream_with_key(api_key=firms_key, region=region, poll_secs=900)))
#     elif os.getenv("USE_FIRMS_KEY", "0") == "1" and not firms_key:
#         print("[Init] USE_FIRMS_KEY=1 but no FIRMS_API_KEY set — skipping.")
#
#
#     # Start camera stream only if module exists and enabled
#     if HAS_CAMERA and int(os.getenv("CAMERA_COUNT", "0")) > 0:
#         asyncio.create_task(feeder(multi_camera_stream()))
#
#     # Incident candidates by coarse spatio-temporal key
#     candidates: Dict[str, Dict[str, Any]] = {}
#
#     while True:
#         e = await q.get()
#         raw = RawEvent(**e)
#
#         # Coarse spatial cell (~5 km) for dedupe/fusion
#         cell = f"{round((raw.lat or 0) * 20) / 20:.2f}:{round((raw.lon or 0) * 20) / 20:.2f}"
#
#         # ---- Normalize time to datetime, floor to minute for key ----
#         t = raw.when or raw.received_at
#         if isinstance(t, str):
#             try:
#                 t_dt = datetime.fromisoformat(t)
#             except Exception:
#                 t_dt = datetime.now(timezone.utc)
#         elif isinstance(t, datetime):
#             t_dt = t
#         else:
#             t_dt = datetime.now(timezone.utc)
#
#         # Ensure timezone
#         if t_dt.tzinfo is None:
#             t_dt = t_dt.replace(tzinfo=timezone.utc)
#
#         minute_key = t_dt.replace(second=0, microsecond=0).isoformat(timespec="minutes")
#         key = f"{cell}:{raw.hazard}:{minute_key}"
#
#         # Build / update candidate
#         c = candidates.get(key)
#         if not c:
#             if raw.lat is None or raw.lon is None:
#                 # skip items without a location
#                 continue
#             c = {
#                 "key": key,
#                 "lat": raw.lat,
#                 "lon": raw.lon,
#                 "when": t_dt,              # keep as datetime
#                 "hazard": raw.hazard,
#                 "sources": {}
#             }
#             candidates[key] = c
#
#         # Enrich with source info for fusion
#         c["sources"][raw.source] = {"id": raw.id, "lat": raw.lat, "lon": raw.lon}
#
#         # Fusion / scoring
#         score, fused_from = fuse_score(c)
#         print(f"[Fuse] key={key} sources={list(c['sources'].keys())} hazard={c['hazard']} score={score:.2f}")
#
#         if score >= 0.7:
#             try:
#                 inc_id = f"{raw.hazard}:{uuid.uuid4().hex[:8]}"
#                 confirmed = ConfirmedIncident(
#                     id=inc_id,
#                     lat=c["lat"],
#                     lon=c["lon"],
#                     when=c["when"],  # already a datetime
#                     hazard=c["hazard"],
#                     confidence=score,
#                     fused_from=fused_from
#                 )
#                 print(f"[Promote] {confirmed.id} {confirmed.hazard} @ ({confirmed.lat:.4f},{confirmed.lon:.4f}) score={score:.2f}")
#
#                 # DB insert
#                 try:
#                     await insert_incident({
#                         "id": confirmed.id,
#                         "hazard": confirmed.hazard,
#                         "when": confirmed.when.isoformat(),
#                         "lat": confirmed.lat,
#                         "lon": confirmed.lon,
#                         "confidence": confirmed.confidence,
#                         "fused_from_json": json.dumps(confirmed.fused_from),
#                         "created_at": now_iso()
#                     })
#                     print(f"[DB] inserted {confirmed.id}")
#                 except Exception as e:
#                     print(f"[DB][ERROR] {e}")
#
#                 # Simulation
#                 sim_out: Dict[str, Any] = {}
#                 try:
#                     incident = {"lat": confirmed.lat, "lon": confirmed.lon, "accident_type": confirmed.hazard}
#                     sim_out = run_predictive_module(incident, pop_density=None) or {}
#                     print(f"[Sim] ok for {confirmed.id}")
#                 except Exception as e:
#                     print(f"[Sim][ERROR] {e}")
#
#                 # Websocket broadcast (ensure JSON-safe datetimes)
#                 try:
#                     payload_incident = confirmed.model_dump()
#                     payload_incident["when"] = confirmed.when.isoformat()
#                     await broadcast({
#                         "type": "incident.confirmed",
#                         "data": {
#                             "incident": payload_incident,
#                             "simulation": sim_out
#                         }
#                     })
#                     print(f"[WS] broadcast sent for {confirmed.id}")
#                 except Exception as e:
#                     print(f"[WS][ERROR] {e}")
#
#             finally:
#                 # avoid re-firing
#                 candidates.pop(key, None)

import os
import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body
from pydantic import BaseModel
from dotenv import load_dotenv

# --- package-local imports ---
from .models import RawEvent, ConfirmedIncident
from .storage import init_db, upsert_raw_event, insert_incident
from .fusion import fuse_score

# Feed ingestors
from .ingestors import (
    usgs_stream,
    nws_stream,
    eonet_fires_stream,
    gdacs_stream,
    firms_stream_with_key,
)

# Camera ingestion is OPTIONAL
try:
    from .camera_ingestor import multi_camera_stream
    HAS_CAMERA = True
except Exception:
    HAS_CAMERA = False

# Import your dispatcher from the project root
from incident_pipeline import run_predictive_module

load_dotenv()

app = FastAPI(title="Gov Realtime Detection")
clients: Set[WebSocket] = set()  # active websocket clients


# =========================
# Helpers / Models (local)
# =========================

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class WSMsg(BaseModel):
    type: str
    data: Dict[str, Any]


def _json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f"Type not serializable: {type(o)}")


# ========= Handoff (Agent JSON) =========
from pathlib import Path
import tempfile

OUTBOX_DIR = Path(os.getenv("AGENT_OUTBOX_DIR", "agent_outbox")).resolve()
OUTBOX_DIR.mkdir(parents=True, exist_ok=True)

def _atomic_write_json(path: Path, payload: dict):
    """Write JSON atomically: write to temp file, then rename."""
    tmp_fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), default=_json_default)
        Path(tmp_path).replace(path)
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass

def write_agent_handoff(incident: dict, simulation: dict, *, origin: str, handoff_id: str | None = None) -> Path:
    """
    Create a JSON handoff file for the agent runner to consume.

    Schema (top-level):
      id:           unique id for this handoff
      origin:       'trigger' | 'realtime'
      created_utc:  ISO string
      incident:     { lat, lon, accident_type }
      simulation:   sim block from run_predictive_module (optional/partial OK)

    File name: HANDOFF_<id>.json
    """
    hid = handoff_id or f"{origin}-{uuid.uuid4().hex[:10]}"
    handoff = {
        "id": hid,
        "origin": origin,
        "created_utc": now_iso(),
        "incident": {
            "lat": float(incident["lat"]),
            "lon": float(incident["lon"]),
            "accident_type": str(incident["accident_type"]),
        },
        "simulation": simulation or {},
    }
    out_path = OUTBOX_DIR / f"HANDOFF_{hid}.json"
    _atomic_write_json(out_path, handoff)
    print(f"[Handoff] wrote {out_path}")
    return out_path


async def broadcast(msg: Dict[str, Any]):
    """Send a JSON-safe message to all connected WS clients, with logging."""
    total = len(clients)
    msg_type = msg.get("type") if isinstance(msg, dict) else None
    if total == 0:
        print(f"[WS] 0 clients connected; dropping message type={msg_type}")
        return

    delivered = 0
    dead = []
    for ws in list(clients):
        try:
            await ws.send_text(json.dumps(msg, default=_json_default))
            delivered += 1
        except WebSocketDisconnect:
            dead.append(ws)
        except Exception as e:
            print(f"[WS][ERROR] send failed: {e}")
            dead.append(ws)

    for d in dead:
        clients.discard(d)
    print(f"[WS] delivered type={msg_type} to {delivered}/{total} clients")


# =========================
# Manual trigger (for tests)
# =========================

@app.post("/trigger")
async def trigger(incident: dict = Body(...)):
    """
    POST JSON like:
    {"lat":34.0526,"lon":-118.2449,"accident_type":"urban fire"}
    """
    sim = run_predictive_module(incident, pop_density=None)
    handoff_path = write_agent_handoff(incident, sim, origin="trigger")
    return {
        "ok": True,
        "incident": incident,
        "simulation": sim,
        "handoff_path": str(handoff_path),
    }


# =========================
# Lifecycle
# =========================

@app.on_event("startup")
async def on_start():
    await init_db()
    asyncio.create_task(run_ingestion())


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    print(f"[WS] client connected; now {len(clients)} client(s)")
    try:
        while True:
            await ws.receive_text()  # ignore client messages
    except WebSocketDisconnect:
        clients.discard(ws)
        print(f"[WS] client disconnected; now {len(clients)} client(s)")


# (Optional) quick debug endpoint to view recent incidents
@app.get("/incidents")
async def list_incidents(limit: int = 20):
    # returns rows most-recent-first using storage.py’s schema
    import aiosqlite
    rows = []
    async with aiosqlite.connect("realtime.sqlite") as db:
        async with db.execute(
            "SELECT id, hazard, when_utc, lat, lon, confidence "
            "FROM incidents ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ) as cur:
            async for r in cur:
                rows.append({
                    "id": r[0], "hazard": r[1], "when": r[2],
                    "lat": r[3], "lon": r[4], "confidence": r[5],
                })
    return {"incidents": rows}


# =========================
# Ingestion + Fusion Runner
# =========================

async def run_ingestion():
    usgs_secs = int(os.getenv("USGS_POLL_SECS", "15"))
    nws_secs = int(os.getenv("NWS_POLL_SECS", "60"))
    firms_secs = int(os.getenv("FIRMS_POLL_SECS", "900"))
    nws_area = os.getenv("NWS_AREA", "CA")
    firms_reg = os.getenv("FIRMS_REGION", "north_america")

    q: asyncio.Queue = asyncio.Queue()

    async def feeder(gen):
        async for e in gen:
            # persist raw for audit
            e_for_db = {**e, "payload_json": json.dumps(e.get("payload", {}))}
            await upsert_raw_event(e_for_db)
            await q.put(e)
            # debug line so you can see feed activity
            print(f"[Ingest] event -> q: src={e.get('source')} hazard={e.get('hazard')} lat={e.get('lat')} lon={e.get('lon')}")

    # Start feed pollers with toggles
    if os.getenv("USE_USGS", "1") == "1":
        asyncio.create_task(feeder(usgs_stream(usgs_secs)))
    else:
        print("[Init] USGS disabled via USE_USGS=0")

    if os.getenv("USE_NWS", "1") == "1":
        asyncio.create_task(feeder(nws_stream(nws_area, nws_secs)))
    else:
        print("[Init] NWS disabled via USE_NWS=0")

    # NASA EONET (wildfires) — free, no key
    if os.getenv("USE_EONET", "1") == "1":
        asyncio.create_task(feeder(eonet_fires_stream(poll_secs=300)))
    else:
        print("[Init] EONET disabled via USE_EONET=0")

    # GDACS (global multi-hazard) — free, no key
    if os.getenv("USE_GDACS", "1") == "1":
        asyncio.create_task(feeder(gdacs_stream(poll_secs=300)))
    else:
        print("[Init] GDACS disabled via USE_GDACS=0")

    # FIRMS with key (optional)
    firms_key = os.getenv("FIRMS_API_KEY")
    if os.getenv("USE_FIRMS_KEY", "0") == "1" and firms_key:
        region = os.getenv("FIRMS_REGION", "USA")
        asyncio.create_task(feeder(firms_stream_with_key(api_key=firms_key, region=region, poll_secs=900)))
    elif os.getenv("USE_FIRMS_KEY", "0") == "1" and not firms_key:
        print("[Init] USE_FIRMS_KEY=1 but no FIRMS_API_KEY set — skipping.")

    # Start camera stream only if module exists and enabled
    if HAS_CAMERA and int(os.getenv("CAMERA_COUNT", "0")) > 0:
        asyncio.create_task(feeder(multi_camera_stream()))

    # Incident candidates by coarse spatio-temporal key
    candidates: Dict[str, Dict[str, Any]] = {}

    while True:
        e = await q.get()
        raw = RawEvent(**e)

        # Coarse spatial cell (~5 km) for dedupe/fusion
        cell = f"{round((raw.lat or 0) * 20) / 20:.2f}:{round((raw.lon or 0) * 20) / 20:.2f}"

        # ---- Normalize time to datetime, floor to minute for key ----
        t = raw.when or raw.received_at
        if isinstance(t, str):
            try:
                t_dt = datetime.fromisoformat(t)
            except Exception:
                t_dt = datetime.now(timezone.utc)
        elif isinstance(t, datetime):
            t_dt = t
        else:
            t_dt = datetime.now(timezone.utc)

        # Ensure timezone
        if t_dt.tzinfo is None:
            t_dt = t_dt.replace(tzinfo=timezone.utc)

        minute_key = t_dt.replace(second=0, microsecond=0).isoformat(timespec="minutes")
        key = f"{cell}:{raw.hazard}:{minute_key}"

        # Build / update candidate
        c = candidates.get(key)
        if not c:
            if raw.lat is None or raw.lon is None:
                # skip items without a location
                continue
            c = {
                "key": key,
                "lat": raw.lat,
                "lon": raw.lon,
                "when": t_dt,              # keep as datetime
                "hazard": raw.hazard,
                "sources": {}
            }
            candidates[key] = c

        # Enrich with source info for fusion
        c["sources"][raw.source] = {"id": raw.id, "lat": raw.lat, "lon": raw.lon}

        # Fusion / scoring
        score, fused_from = fuse_score(c)
        print(f"[Fuse] key={key} sources={list(c['sources'].keys())} hazard={c['hazard']} score={score:.2f}")

        if score >= 0.7:
            try:
                inc_id = f"{raw.hazard}:{uuid.uuid4().hex[:8]}"
                confirmed = ConfirmedIncident(
                    id=inc_id,
                    lat=c["lat"],
                    lon=c["lon"],
                    when=c["when"],  # already a datetime
                    hazard=c["hazard"],
                    confidence=score,
                    fused_from=fused_from
                )
                print(f"[Promote] {confirmed.id} {confirmed.hazard} @ ({confirmed.lat:.4f},{confirmed.lon:.4f}) score={score:.2f}")

                # DB insert
                try:
                    await insert_incident({
                        "id": confirmed.id,
                        "hazard": confirmed.hazard,
                        "when": confirmed.when.isoformat(),
                        "lat": confirmed.lat,
                        "lon": confirmed.lon,
                        "confidence": confirmed.confidence,
                        "fused_from_json": json.dumps(confirmed.fused_from),
                        "created_at": now_iso()
                    })
                    print(f"[DB] inserted {confirmed.id}")
                except Exception as e:
                    print(f"[DB][ERROR] {e}")

                # Simulation
                sim_out: Dict[str, Any] = {}
                try:
                    incident = {"lat": confirmed.lat, "lon": confirmed.lon, "accident_type": confirmed.hazard}
                    sim_out = run_predictive_module(incident, pop_density=None) or {}
                    print(f"[Sim] ok for {confirmed.id}")
                    # Write a handoff file for the agent to consume
                    try:
                        write_agent_handoff(incident, sim_out, origin="realtime", handoff_id=confirmed.id)
                    except Exception as e:
                        print(f"[Handoff][ERROR] {e}")
                except Exception as e:
                    print(f"[Sim][ERROR] {e}")

                # Websocket broadcast (ensure JSON-safe datetimes)
                try:
                    payload_incident = confirmed.model_dump()
                    payload_incident["when"] = confirmed.when.isoformat()
                    await broadcast({
                        "type": "incident.confirmed",
                        "data": {
                            "incident": payload_incident,
                            "simulation": sim_out
                        }
                    })
                    print(f"[WS] broadcast sent for {confirmed.id}")
                except Exception as e:
                    print(f"[WS][ERROR] {e}")

            finally:
                # avoid re-firing
                candidates.pop(key, None)

