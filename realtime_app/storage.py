import asyncio

import aiosqlite

DB_PATH = "realtime.sqlite"

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS raw_events (
  id TEXT PRIMARY KEY,
  source TEXT,
  when_utc TEXT,
  lat REAL,
  lon REAL,
  payload TEXT,
  received_at TEXT
);
CREATE TABLE IF NOT EXISTS incidents (
  id TEXT PRIMARY KEY,
  hazard TEXT,
  when_utc TEXT,
  lat REAL,
  lon REAL,
  confidence REAL,
  fused_from TEXT,
  created_at TEXT
);
"""

async def _prep(db: aiosqlite.Connection):
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA busy_timeout=7000;")  # 7s
    await db.commit()

async def init_db():
    async with aiosqlite.connect(DB_PATH, timeout=15) as db:
        await _prep(db)
        for stmt in CREATE_SQL.strip().split(";"):
            s = stmt.strip()
            if s:
                await db.execute(s)
        await db.commit()

async def upsert_raw_event(e: dict):
    for _ in range(3):  # simple retry
        try:
            async with aiosqlite.connect(DB_PATH, timeout=15) as db:
                await _prep(db)
                await db.execute("""
                    INSERT OR REPLACE INTO raw_events (id, source, when_utc, lat, lon, payload, received_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    e["id"], e["source"], e.get("when") or "", e.get("lat"), e.get("lon"),
                    e.get("payload_json", "{}"), e.get("received_at")
                ))
                await db.commit()
            return
        except aiosqlite.OperationalError:
            await asyncio.sleep(0.2)

async def insert_incident(i: dict):
    for _ in range(3):
        try:
            async with aiosqlite.connect(DB_PATH, timeout=15) as db:
                await _prep(db)
                await db.execute("""
                    INSERT OR REPLACE INTO incidents (id, hazard, when_utc, lat, lon, confidence, fused_from, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    i["id"], i["hazard"], i["when"], i["lat"], i["lon"],
                    i["confidence"], i["fused_from_json"], i["created_at"]
                ))
                await db.commit()
            return
        except aiosqlite.OperationalError:
            await asyncio.sleep(0.2)
