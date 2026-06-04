"""
main.py
-------
FastAPI backend for the Flosenso MQTT dashboard.

JSON API consumed by the React/Vite frontend. It reuses the existing data layer
(``database.py``), broker config (``mqtt_service.BROKERS``) and helpers
(``utils.py``).

Performance notes:
  * Customer / MAC dropdowns are served from the small ``customers`` table
    (``/api/devices``) instead of scanning every row in ``mqtt_messages``.
  * Message queries use ``database.query_messages_fast`` which builds exact
    predicates so the composite indexes added in ``database.py`` are used.

Run (from the project root, with the venv active)::

    uvicorn backend.main:app --reload --port 8000
"""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path
from typing import Optional

# Make the project root importable so we can reuse database.py / mqtt_service.py.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import database as db
from mqtt_service import BROKERS

try:
    # When run as a package: `uvicorn backend.main:app` from the project root.
    from .mqtt_publish import publish_message
except ImportError:
    # When run from inside backend/: `uvicorn main:app`.
    from mqtt_publish import publish_message

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
DB_PATH = os.environ.get("FLOSENSO_DB_PATH", str(_ROOT / "mqtt_data.db"))

# Comma-separated allowed origins for the browser app (Vite dev server default).
_DEFAULT_ORIGINS = "http://localhost:5173,http://127.0.0.1:5173"
ALLOWED_ORIGINS = [
    o.strip()
    for o in os.environ.get("FLOSENSO_CORS_ORIGINS", _DEFAULT_ORIGINS).split(",")
    if o.strip()
]

# Quick Action commands (mirrors the Streamlit dashboard).
COMMANDS = [
    {"label": "🔵 GET_STATUS", "cmd": "app200req"},
    {"label": "📶 GET_WIFI_STRENGTH", "cmd": "app308"},
    {"label": "📏 CHECK_DISTANCE", "cmd": "app301&T"},
    {"label": "⚙️ GET_SETTINGS", "cmd": "app300"},
    {"label": "📅 GET_SCHEDULES", "cmd": "app204getsdl"},
    {"label": "🔁 RESTART_DEVICE", "cmd": "app302"},
    {"label": "🔄 RESET_LORA", "cmd": "app304"},
    {"label": "🔌 FORCE_MOTOR_ON", "cmd": "app210&MO&05"},
    {"label": "⛔ FORCE_MOTOR_OFF", "cmd": "app210&MF"},
]

app = FastAPI(title="Flosenso MQTT Dashboard API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup() -> None:
    # Ensures tables + (new) indexes exist on the target DB.
    db.init_db(DB_PATH)


# ─────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────
class PublishRequest(BaseModel):
    topic: str
    message: str
    broker: Optional[str] = None
    qos: int = 1
    retain: bool = False


# ─────────────────────────────────────────────
# Brokers / stats
# ─────────────────────────────────────────────
@app.get("/api/brokers")
def brokers():
    return {"brokers": [b.name for b in BROKERS]}


@app.get("/api/stats/broker-counts")
def broker_counts():
    return {"counts": db.get_message_count_by_broker(DB_PATH)}


# ─────────────────────────────────────────────
# Devices (fast: from the customers table, not mqtt_messages)
# ─────────────────────────────────────────────
@app.get("/api/devices")
def devices():
    """
    Customer IDs, MAC IDs and their pairings for the dashboard selectors.

    Sourced from the ``customers`` table. If that table is empty (fresh DB, no
    Flosenso-routed topics seen yet) we fall back to distinct MACs in
    ``mqtt_messages`` so the UI still works.
    """
    pairs = db.list_customer_mac_pairs_from_customers(DB_PATH)
    customers = sorted({p["customer_id"] for p in pairs if p["customer_id"]})
    macs = sorted({db.normalise_mac(p["mac_id"]) for p in pairs if p["mac_id"]})

    if not customers and not macs:
        macs = list(db.list_distinct_mac_ids_from_mqtt_messages(DB_PATH))

    macs_by_customer: dict[str, list[str]] = {}
    for p in pairs:
        cid = p["customer_id"]
        if not cid:
            continue
        macs_by_customer.setdefault(cid, [])
        nm = db.normalise_mac(p["mac_id"]) if p["mac_id"] else ""
        if nm and nm not in macs_by_customer[cid]:
            macs_by_customer[cid].append(nm)
    for cid in macs_by_customer:
        macs_by_customer[cid].sort()

    return {
        "customers": customers,
        "macs": macs,
        "macs_by_customer": macs_by_customer,
    }


# ─────────────────────────────────────────────
# Messages
# ─────────────────────────────────────────────
@app.get("/api/messages")
def messages(
    mac_id: Optional[str] = Query(None),
    customer_id: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    broker: Optional[str] = Query(None),
    payload: Optional[str] = Query(None),
    limit: int = Query(50000, ge=1, le=200000),
):
    if not mac_id and not customer_id and not payload:
        raise HTTPException(
            status_code=400,
            detail="Provide at least a mac_id, customer_id, or payload filter.",
        )
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=400, detail="start_date must be on or before end_date."
        )
    rows = db.query_messages_fast(
        mac_id=mac_id,
        customer_id=customer_id,
        start_date=start_date,
        end_date=end_date,
        broker_name=broker,
        payload_search=payload,
        limit=limit,
        db_path=DB_PATH,
    )
    return {"count": len(rows), "messages": rows}


# ─────────────────────────────────────────────
# Commands / publish
# ─────────────────────────────────────────────
@app.get("/api/commands")
def commands():
    # The full command list. The frontend hides commands per logged-in user.
    return {"commands": COMMANDS}


@app.post("/api/publish")
def publish(body: PublishRequest):
    if not body.topic.strip() or not body.message.strip():
        raise HTTPException(status_code=400, detail="topic and message are required.")
    ok, info = publish_message(
        body.topic.strip(),
        body.message.strip(),
        broker_name=body.broker,
        qos=body.qos,
        retain=body.retain,
    )
    if not ok:
        raise HTTPException(status_code=502, detail=f"Publish failed: {info}")
    return {"ok": True, "broker": info}


@app.get("/api/health")
def health():
    return {"status": "ok"}
