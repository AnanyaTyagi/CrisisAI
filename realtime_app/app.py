import os
import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body
from pydantic import BaseModel
from dotenv import load_dotenv

# --- relative imports (package-local) ---
from .models import RawEvent, ConfirmedIncident
from .storage import init_db, upsert_raw_event, insert_incident
from .ingestors import usgs_stream, nws_stream, firms_stream
from .fusion import fuse_score

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
clients: set[WebSocket] = set()  # active websocket clients


# =========================
# Helpers / Models (local)
# =========================

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

class WSMsg(BaseModel):
    type: str
    data: Dict[str, Any]


clients = set()  # websockets

# replace your broadcast() with this:
import json
from datetime import datetime
from fastapi import WebSocket, WebSocketDisconnect

def _json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f"Type not serializable: {type(o)}")

async def broadcast(msg: dict):
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
    return {"ok": True, "incident": incident, "simulation": sim}


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

    # Start feed pollers
    asyncio.create_task(feeder(usgs_stream(usgs_secs)))
    asyncio.create_task(feeder(nws_stream(nws_area, nws_secs)))
    asyncio.create_task(feeder(firms_stream(firms_reg, firms_secs)))

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
                print(
                    f"[Promote] {confirmed.id} {confirmed.hazard} @ ({confirmed.lat:.4f},{confirmed.lon:.4f}) score={score:.2f}")

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
                sim_out = {}
                try:
                    incident = {"lat": confirmed.lat, "lon": confirmed.lon, "accident_type": confirmed.hazard}
                    sim_out = run_predictive_module(incident, pop_density=None) or {}
                    print(f"[Sim] ok for {confirmed.id}")
                except Exception as e:
                    print(f"[Sim][ERROR] {e}")

                # Websocket broadcast
                try:
                    await broadcast(WSMsg(
                        type="incident.confirmed",
                        data={"incident": confirmed.model_dump(), "simulation": sim_out}
                    ).model_dump())
                    print(f"[WS] broadcast sent for {confirmed.id}")
                except Exception as e:
                    print(f"[WS][ERROR] {e}")

            finally:
                # avoid re-firing
                candidates.pop(key, None)
