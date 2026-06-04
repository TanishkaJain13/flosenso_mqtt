"""
database.py
-----------
SQLite data layer shared by the ingestion service (``mqtt_service.py``) and the
FastAPI backend (``backend/``):

  - Auto-creation of tables and indexes (``init_db``)
  - MAC allow-list lookups for ingestion
  - Batch message inserts + ``customers`` sync + retention cleanup
  - Fast, index-friendly read helpers for the dashboard API
"""

import sqlite3
import logging
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from utils import normalise_mac

logger = logging.getLogger(__name__)

# Ingestion stores ``timestamp`` using IST wall time (see ``utc_now_str`` in utils).
_IST = timezone(timedelta(hours=5, minutes=30))

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
DB_PATH = "mqtt_data.db"

# Thread-local storage so each thread gets its own connection
_local = threading.local()

# Track last cleanup time to avoid redundant operations
_last_cleanup_time = 0.0
_cleanup_lock = threading.Lock()


# ─────────────────────────────────────────────
# Connection management
# ─────────────────────────────────────────────
def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Return a per-thread SQLite connection (created on first use)."""
    conn = getattr(_local, "connection", None)
    if conn is None:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row          # dict-like row access
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")  # 30 seconds
        conn.execute("PRAGMA cache_size=-64000")
        conn.execute("PRAGMA temp_store=MEMORY")
        _local.connection = conn
        logger.debug("Opened new SQLite connection for thread %s", threading.current_thread().name)
    return conn


@contextmanager
def managed_connection(db_path: str = DB_PATH):
    """Yield a connection and commit/rollback automatically."""
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ─────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────
DDL_USERS = """
CREATE TABLE IF NOT EXISTS users (
    id         INTEGER  PRIMARY KEY AUTOINCREMENT,
    username   TEXT     NOT NULL,
    mac_id     TEXT     NOT NULL UNIQUE,
    created_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now'))
);
"""

DDL_MQTT_MESSAGES = """
CREATE TABLE IF NOT EXISTS mqtt_messages (
    id          INTEGER  PRIMARY KEY AUTOINCREMENT,
    topic       TEXT     NOT NULL,
    mac_id      TEXT     NOT NULL,
    payload     TEXT,
    qos         INTEGER  NOT NULL DEFAULT 0,
    retain      INTEGER  NOT NULL DEFAULT 0,
    broker_name TEXT     NOT NULL,
    timestamp   DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now')),
    received_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now'))
);
"""

DDL_CUSTOMERS = """
CREATE TABLE IF NOT EXISTS customers (
    id           INTEGER  PRIMARY KEY AUTOINCREMENT,
    customer_id  TEXT     NOT NULL,
    mac_id       TEXT     NOT NULL,
    timestamp    DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now')),
    received_at  DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now')),
    UNIQUE (customer_id, mac_id)
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_messages_mac_id       ON mqtt_messages (mac_id);",
    "CREATE INDEX IF NOT EXISTS idx_messages_timestamp    ON mqtt_messages (timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_messages_received_at ON mqtt_messages (received_at);",
    "CREATE INDEX IF NOT EXISTS idx_messages_broker       ON mqtt_messages (broker_name);",
    "CREATE INDEX IF NOT EXISTS idx_users_mac_id       ON users (mac_id);",
    "CREATE INDEX IF NOT EXISTS idx_customers_customer ON customers (customer_id);",
    "CREATE INDEX IF NOT EXISTS idx_customers_mac      ON customers (mac_id);",
    # ── Composite indexes for the dashboard's hot read paths ──────────────
    # Report query: WHERE topic = ? ORDER BY received_at DESC  (was a full SCAN).
    "CREATE INDEX IF NOT EXISTS idx_messages_topic_received ON mqtt_messages (topic, received_at DESC);",
    # Device history: filter by mac_id then ORDER BY timestamp DESC.
    "CREATE INDEX IF NOT EXISTS idx_messages_mac_timestamp  ON mqtt_messages (mac_id, timestamp DESC);",
    # Per-broker history scoping.
    "CREATE INDEX IF NOT EXISTS idx_messages_broker_ts      ON mqtt_messages (broker_name, timestamp DESC);",
]


def _migrate_mqtt_messages_received_at(conn: sqlite3.Connection) -> None:
    """Add ``received_at`` to legacy ``mqtt_messages`` tables (batch insert expects it)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(mqtt_messages)")}
    if "received_at" in cols:
        return
    conn.execute("ALTER TABLE mqtt_messages ADD COLUMN received_at DATETIME DEFAULT NULL")
    conn.execute("UPDATE mqtt_messages SET received_at = timestamp WHERE received_at IS NULL")
    logger.info("Migration: added mqtt_messages.received_at (backfilled from timestamp)")


def init_db(db_path: str = DB_PATH) -> None:
    """
    Create tables and indexes if they do not already exist, and apply lightweight
    migrations. Safe to call repeatedly; runs on backend startup and ingestion start.
    """
    with sqlite3.connect(db_path, check_same_thread=False) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(DDL_USERS)
        conn.execute(DDL_MQTT_MESSAGES)
        conn.execute(DDL_CUSTOMERS)
        _migrate_mqtt_messages_received_at(conn)
        for idx_ddl in DDL_INDEXES:
            try:
                conn.execute(idx_ddl)
            except sqlite3.OperationalError:
                pass  # Already exists or other non-fatal error
    logger.debug("Database initialised at %s", db_path)

    # Backfill ``customers`` from any Flosenso-routed topics already stored.
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        has_flosenso = conn.execute(
            "SELECT 1 FROM mqtt_messages WHERE topic LIKE 'flosenso&%' LIMIT 1"
        ).fetchone()
        conn.close()
        if has_flosenso:
            n_src = backfill_customers_from_mqtt_topics(db_path)
            logger.debug("Synced customers from MQTT topics (%d distinct rows).", n_src)
    except Exception as exc:
        if "locked" in str(exc).lower():
            logger.warning("Database locked during backfill - skipping; will sync during ingestion.")
        else:
            logger.warning("customers backfill on init: %s", exc)


# ─────────────────────────────────────────────
# MAC allow-list (ingestion)
# ─────────────────────────────────────────────
def get_registered_mac_ids(db_path: str = DB_PATH) -> set[str]:
    """
    Return the full set of registered MAC IDs. The ingestion service refreshes
    this periodically to keep its allow-list current without a restart.
    """
    conn = get_connection(db_path)
    rows = conn.execute("SELECT mac_id FROM users").fetchall()
    return {row["mac_id"] for row in rows}


# ─────────────────────────────────────────────
# Device / customer listings (dashboard selectors)
# ─────────────────────────────────────────────
def list_distinct_mac_ids_from_mqtt_messages(db_path: str = DB_PATH) -> list[str]:
    """Distinct non-empty ``mac_id`` values in ``mqtt_messages`` (fallback source)."""
    conn = get_connection(db_path)
    rows = conn.execute(
        """
        SELECT DISTINCT TRIM(mac_id) AS m
        FROM   mqtt_messages
        WHERE  mac_id IS NOT NULL AND TRIM(mac_id) != ''
        ORDER  BY m
        """
    ).fetchall()
    raw = [r["m"] for r in rows if r["m"] and str(r["m"]).strip()]
    return sorted({normalise_mac(m) for m in raw})


def list_customer_mac_pairs_from_customers(db_path: str = DB_PATH) -> list[dict]:
    """
    All (customer_id, mac_id) pairs from the ``customers`` table in one query.

    Lets the frontend build the customer dropdown and per-customer MAC lists from a
    single fast round-trip rather than scanning ``mqtt_messages``.
    """
    with sqlite3.connect(db_path, check_same_thread=False) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT customer_id, mac_id FROM customers ORDER BY customer_id, mac_id"
        ).fetchall()
    return [
        {"customer_id": str(r["customer_id"]).strip(), "mac_id": str(r["mac_id"]).strip()}
        for r in rows
    ]


# ─────────────────────────────────────────────
# customers sync (from Flosenso-routed topics)
# ─────────────────────────────────────────────
def sync_customers_from_flosenso_message_batch(
    messages: list[dict], db_path: str = DB_PATH
) -> None:
    """
    Upsert ``customers`` rows for any message whose ``topic`` is
    ``flosenso&<mac_id>&<customer_id>``. Batched, ``INSERT OR IGNORE``.
    """
    pairs: set[tuple[str, str]] = set()
    for m in messages:
        topic = (m.get("topic") or "").strip()
        parts = topic.split("&")
        if len(parts) != 3 or (parts[0] or "").lower() != "flosenso":
            continue
        topic_mac = (parts[1] or "").strip().upper()
        col_mac = (m.get("mac_id") or "").strip().upper()
        mid = topic_mac or col_mac
        cid = (parts[2] or "").strip()
        if cid and mid:
            pairs.add((cid, mid))
    if not pairs:
        return
    with sqlite3.connect(db_path, check_same_thread=False, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executemany(
            "INSERT OR IGNORE INTO customers (customer_id, mac_id) VALUES (?, ?)",
            list(pairs),
        )
        conn.commit()


def backfill_customers_from_mqtt_topics(db_path: str = DB_PATH) -> int:
    """Populate ``customers`` from existing Flosenso-routed ``mqtt_messages`` rows."""
    with sqlite3.connect(db_path, check_same_thread=False, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT DISTINCT topic, mac_id FROM mqtt_messages WHERE topic LIKE 'flosenso&%'"
        ).fetchall()
    msgs = [{"topic": r["topic"] or "", "mac_id": r["mac_id"] or ""} for r in rows]
    if not msgs:
        return 0
    sync_customers_from_flosenso_message_batch(msgs, db_path)
    return len(msgs)


# ─────────────────────────────────────────────
# Data retention / cleanup
# ─────────────────────────────────────────────
def cleanup_old_data(days: int = 60, db_path: str = DB_PATH) -> int:
    """
    Delete messages and customer associations older than ``days`` (IST wall clock).
    Throttled to run at most once per minute. Returns rows deleted from mqtt_messages.
    """
    global _last_cleanup_time
    now_mono = time.monotonic()
    with _cleanup_lock:
        if now_mono - _last_cleanup_time < 60:
            return 0
        _last_cleanup_time = now_mono

    cutoff = (datetime.now(_IST) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    deleted_count = 0
    try:
        with managed_connection(db_path) as conn:
            cursor = conn.execute("DELETE FROM mqtt_messages WHERE timestamp < ?", (cutoff,))
            deleted_count = cursor.rowcount
            conn.execute("DELETE FROM customers WHERE timestamp < ?", (cutoff,))
        if deleted_count > 0:
            logger.debug("Retention: deleted %d messages older than %d days (cutoff %s)",
                         deleted_count, days, cutoff)
    except Exception as exc:
        logger.error("Retention cleanup failed: %s", exc)
    return deleted_count


# ─────────────────────────────────────────────
# Message ingestion (batch insert)
# ─────────────────────────────────────────────
def batch_insert_messages(messages: list[dict], db_path: str = DB_PATH) -> int:
    """
    Insert message dicts in a single transaction. Each dict must have keys:
    topic, mac_id, payload, qos, retain, broker_name, timestamp. Returns the count.
    """
    if not messages:
        return 0

    sql = """
        INSERT INTO mqtt_messages
            (topic, mac_id, payload, qos, retain, broker_name, timestamp, received_at)
        VALUES
            (:topic, :mac_id, :payload, :qos, :retain, :broker_name, :timestamp,
             strftime('%Y-%m-%d %H:%M:%S', 'now'))
    """
    with managed_connection(db_path) as conn:
        conn.executemany(sql, messages)

    logger.debug("Batch-inserted %d messages", len(messages))
    try:
        sync_customers_from_flosenso_message_batch(messages, db_path)
    except Exception as exc:
        logger.warning("customers sync after mqtt insert: %s", exc)

    cleanup_old_data(days=60, db_path=db_path)
    return len(messages)


# ─────────────────────────────────────────────
# Dashboard query helpers (API)
# ─────────────────────────────────────────────
def query_messages_fast(
    mac_id: Optional[str] = None,
    customer_id: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    broker_name: Optional[str] = None,
    payload_search: Optional[str] = None,
    limit: int = 50000,
    db_path: str = DB_PATH,
) -> list[dict]:
    """
    Index-friendly message query for the API. Builds exact predicates so SQLite
    uses the composite indexes:

      * ``customer_id`` + ``mac_id``  →  ``topic = 'flosenso&<mac>&<customer>'``
        (uses ``idx_messages_topic_received``)
      * ``mac_id`` only               →  ``mac_id = ?`` (``idx_messages_mac_timestamp``)
      * ``broker_name`` only          →  ``idx_messages_broker_ts``

    Dates filter ``received_at``. MAC values are normalised to match ingestion.
    """
    conn = get_connection(db_path)
    where: list[str] = []
    params: list = []

    norm_mac = normalise_mac(mac_id) if mac_id else None

    if norm_mac and customer_id:
        where.append("topic = ?")
        params.append(f"flosenso&{norm_mac}&{customer_id}")
    elif norm_mac:
        where.append("mac_id = ?")
        params.append(norm_mac)
    elif customer_id:
        where.append("topic LIKE ?")
        params.append(f"flosenso&%&{customer_id}")

    if broker_name:
        where.append("broker_name = ?")
        params.append(broker_name)

    if payload_search:
        where.append("payload = ?")
        params.append(payload_search)

    if start_date is not None and end_date is not None:
        # Filter on ``timestamp`` (consistent IST across all rows) — not
        # ``received_at``, which mixes IST (migrated rows) and UTC (new rows).
        where.append("date(timestamp) BETWEEN date(?) AND date(?)")
        params.extend([str(start_date), str(end_date)])

    where_str = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT broker_name, topic, mac_id, payload, qos, retain, timestamp, received_at
        FROM   mqtt_messages
        {where_str}
        ORDER  BY timestamp DESC, id DESC
        LIMIT  ?
    """
    params.append(limit)
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def get_message_count_by_broker(db_path: str = DB_PATH) -> list[dict]:
    """Per-broker message counts for the dashboard stat cards."""
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT broker_name, COUNT(*) AS total FROM mqtt_messages GROUP BY broker_name"
    ).fetchall()
    return [dict(r) for r in rows]
