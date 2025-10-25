import os, asyncio, time
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any, List, Optional, Union

import cv2
from ultralytics import YOLO

# =========================
# Helpers / Config
# =========================

def _getenv_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, default))
    except Exception:
        return default

def _getenv_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, default))
    except Exception:
        return default

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _coerce_labels(val: Optional[str]) -> List[str]:
    if not val:
        return ["fire", "smoke"]
    return [x.strip().lower() for x in val.split(",") if x.strip()]

def _to_abs_if_file(path_like: str) -> Union[str, int]:
    """
    - If it's "rtsp://..." keep as-is.
    - If it's a plain integer like "0", return int(0) for webcam.
    - If it's a relative path, convert to absolute.
    """
    if path_like.startswith("rtsp://"):
        return path_like
    # webcam index?
    if path_like.isdigit():
        return int(path_like)
    # absolute file path?
    if os.path.isabs(path_like):
        return path_like
    # make absolute from current working dir
    return os.path.abspath(path_like)

# Optional class-name normalization for common fire/smoke models
_CANON = {"fire", "smoke", "flame"}

# =========================
# Model (load once)
# =========================
_MODEL_PATH = os.getenv("CAMERA_YOLO_MODEL", "best.pt")
try:
    _model = YOLO(_MODEL_PATH)  # can be 'yolov8n.pt' or a fine-tuned path
    print(f"[Camera] YOLO model loaded: {_MODEL_PATH}")
except Exception as e:
    # Let it raise early so the logs show what's wrong
    print(f"[Camera] ERROR loading YOLO model '{_MODEL_PATH}': {e}")
    raise


# =========================
# Camera stream
# =========================

async def camera_stream(rtsp_url: str,
                        lat: float,
                        lon: float,
                        cam_name: str,
                        frame_interval_ms: int = 500,
                        min_conf: float = 0.35,
                        wanted_labels: Optional[List[str]] = None,
                        max_side: int = 960,
                        cooldown_s: int = 30) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Yields fire/smoke detection events:
      {'id','source','received_at','lat','lon','when','hazard','payload'}
    Compatible with feeder() in app.py.
    """
    if wanted_labels is None:
        wanted_labels = ["fire", "smoke"]

    # resolve the URL / file path
    src = _to_abs_if_file(rtsp_url)
    print(f"[Camera] starting: {cam_name} url={src}")

    # Try opening with and without FFMPEG hint
    cap = cv2.VideoCapture(src, cv2.CAP_FFMPEG)
    if not cap.isOpened():
        print(f"[Camera] cannot open with FFMPEG: {cam_name} — retrying with default backend")
        cap.release()
        cap = cv2.VideoCapture(src)

    # Retry loop if still not open
    retry_backoff = 2.0
    while not cap.isOpened():
        print(f"[Camera] cannot open: {cam_name} — will retry in {retry_backoff:.1f}s")
        await asyncio.sleep(retry_backoff)
        cap.release()
        cap = cv2.VideoCapture(src)
        retry_backoff = min(retry_backoff * 1.5, 10.0)

    last_emit_ts = 0.0

    try:
        while True:
            ok, frame = cap.read()
            if not ok or frame is None:
                # try to re-open quickly (useful for network streams)
                print(f"[Camera] read fail: {cam_name} — attempting reopen")
                cap.release()
                await asyncio.sleep(1.0)
                cap = cv2.VideoCapture(src)
                continue

            # resize for speed (keep aspect)
            h, w = frame.shape[:2]
            if max(h, w) > max_side:
                scale = max_side / max(h, w)
                frame = cv2.resize(frame, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_AREA)

            # YOLO inference
            try:
                results = _model.predict(frame, verbose=False, conf=min_conf)
            except Exception as e:
                print(f"[Camera] YOLO error: {cam_name} — {e}")
                await asyncio.sleep(frame_interval_ms / 1000.0)
                continue

            dets = []
            for r in results:
                names = getattr(r, "names", {}) or {}
                boxes = getattr(r, "boxes", None)
                if boxes is None:
                    continue
                for b in boxes:
                    try:
                        cls_idx = int(b.cls[0])
                        conf = float(b.conf[0])
                        # names can be dict {idx:name}
                        label = str(names.get(cls_idx, cls_idx)).lower()
                        if label == "flame":
                            label = "fire"
                        if label not in wanted_labels and label not in _CANON:
                            continue
                        # tiny box filter
                        x1, y1, x2, y2 = map(float, b.xyxy[0].tolist())
                        if (x2 - x1) * (y2 - y1) < 20 * 20:
                            continue
                        dets.append({"label": label, "conf": round(conf, 3), "bbox": [x1, y1, x2, y2]})
                    except Exception:
                        # skip malformed box
                        continue

            print(f"[Camera] dets={len(dets)} min_conf={min_conf} cam={cam_name}")

            # duplicate suppression per camera
            ts = time.time()
            if dets and (ts - last_emit_ts) >= cooldown_s:
                last_emit_ts = ts
                payload = {"camera": cam_name, "detections": dets}
                evt = {
                    "id": f"camera:{cam_name}:{int(ts)}",
                    "source": "camera",
                    "received_at": _now_iso(),
                    "lat": float(lat),
                    "lon": float(lon),
                    "when": _now_iso(),
                    "hazard": "fire",  # treat as fire; fusion can diversify if needed
                    "payload": payload,
                }
                print(f"[Camera] emit -> {evt['id']} detections={len(dets)}")
                yield evt

            await asyncio.sleep(frame_interval_ms / 1000.0)
    finally:
        try:
            cap.release()
        except Exception:
            pass


async def multi_camera_stream() -> AsyncGenerator[Dict[str, Any], None]:
    """
    Launch N cameras from .env (CAMERA_COUNT and CAMERA_i_* variables)
    and multiplex detections into a single async generator.
    """
    count = _getenv_int("CAMERA_COUNT", 0)
    frame_ms = _getenv_int("CAMERA_FRAME_INTERVAL_MS", 500)
    min_conf = _getenv_float("CAMERA_MIN_CONF", 0.35)
    cooldown = _getenv_int("CAMERA_COOLDOWN_S", 30)
    max_side = _getenv_int("CAMERA_MAX_SIDE", 960)
    wanted = _coerce_labels(os.getenv("CAMERA_CLASSES", "fire,smoke"))

    print(f"[Camera] multi: count={count} conf>={min_conf} frame={frame_ms}ms cooldown={cooldown}s max_side={max_side} wanted={wanted}")

    async def _runner(idx: int, q: asyncio.Queue):
        url = os.getenv(f"CAMERA_{idx}_RTSP")
        lat = float(os.getenv(f"CAMERA_{idx}_LAT", "0"))
        lon = float(os.getenv(f"CAMERA_{idx}_LON", "0"))
        name = os.getenv(f"CAMERA_{idx}_NAME", f"CAM_{idx:02d}")
        if not url:
            print(f"[Camera] skip index {idx}: no CAMERA_{idx}_RTSP")
            return
        async for evt in camera_stream(url, lat, lon, name,
                                       frame_interval_ms=frame_ms,
                                       min_conf=min_conf,
                                       wanted_labels=wanted,
                                       max_side=max_side,
                                       cooldown_s=cooldown):
            await q.put(evt)

    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    tasks = [asyncio.create_task(_runner(i, q)) for i in range(count)]

    try:
        while True:
            evt = await q.get()
            yield evt
    finally:
        for t in tasks:
            t.cancel()
