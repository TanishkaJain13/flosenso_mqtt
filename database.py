"""
database.py
-----------
Handles all SQLite database operations:
  - Auto-creation of tables and indexes
  - User (device) registration
  - Batch message inserts
  - Query helpers for the Streamlit dashboard
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
    """
    Return a per-thread SQLite connection.
    Creates a new connection if the thread does not have one yet.
    """
    conn = getattr(_local, "connection", None)
    if conn is None:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row          # dict-like row access
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000") # 30 seconds
        conn.execute("PRAGMA cache_size=-64000")
        conn.execute("PRAGMA temp_store=MEMORY")
        _local.connection = conn
        logger.debug("Opened new SQLite connection for thread %s", threading.current_thread().name)
    return conn


@contextmanager
def managed_connection(db_path: str = DB_PATH):
    """Context manager that yields a connection and commits/rolls back automatically."""
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

DDL_ADMINS = """
CREATE TABLE IF NOT EXISTS admins (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT    NOT NULL UNIQUE,
    password_hash TEXT    NOT NULL,
    created_at    DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now'))
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
    "CREATE INDEX IF NOT EXISTS idx_messages_payload  ON mqtt_messages (payload);",
]


def _migrate_mqtt_messages_received_at(conn: sqlite3.Connection) -> None:
    """Add ``received_at`` to legacy ``mqtt_messages`` tables (batch insert expects it)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(mqtt_messages)")}
    if "received_at" in cols:
        return
    conn.execute(
        "ALTER TABLE mqtt_messages ADD COLUMN received_at DATETIME DEFAULT NULL"
    )
    conn.execute(
        """
        UPDATE mqtt_messages
        SET    received_at = timestamp
        WHERE  received_at IS NULL
        """
    )
    logger.info("Migration: added mqtt_messages.received_at (backfilled from timestamp)")


def init_db(db_path: str = DB_PATH) -> None:
    """
    Create tables and indexes if they do not already exist.

    Uses a dedicated connection so the target ``db_path`` is always migrated,
    independent of the thread-local pool used elsewhere.
    """
    with sqlite3.connect(db_path, check_same_thread=False) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(DDL_USERS)
        conn.execute(DDL_MQTT_MESSAGES)
        conn.execute(DDL_CUSTOMERS)
        conn.execute(DDL_ADMINS)
        _migrate_mqtt_messages_received_at(conn)
        for idx_ddl in DDL_INDEXES:
            try:
                conn.execute(idx_ddl)
            except sqlite3.OperationalError:
                pass # Already exists or other non-fatal error

        # Ensure admin accounts exist
        import hashlib
        
        # 1. Default admin
        if not conn.execute("SELECT 1 FROM admins WHERE username = 'admin' LIMIT 1").fetchone():
            h = hashlib.sha256("admin123".encode()).hexdigest()
            conn.execute("INSERT INTO admins (username, password_hash) VALUES (?, ?)", ("admin", h))
            logger.info("Created default admin account (admin/admin123)")

        # 2. Flosenso CC user
        cc_user = "flosenso.cc@hipl.co.in"
        if not conn.execute("SELECT 1 FROM admins WHERE username = ? LIMIT 1", (cc_user,)).fetchone():
            cc_h = hashlib.sha256("flosenso@cc123".encode()).hexdigest()
            conn.execute("INSERT INTO admins (username, password_hash) VALUES (?, ?)", (cc_user, cc_h))
            logger.info("Created flosenso cc admin account")
    logger.debug("Database initialised at %s", db_path)
    try:
        # Use a short-lived connection just to check for data
        conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        has_flosenso = conn.execute(
            "SELECT 1 FROM mqtt_messages WHERE topic LIKE 'flosenso&%' LIMIT 1"
        ).fetchone()
        conn.close()
        
        if has_flosenso:
            n_src = backfill_customers_from_mqtt_topics(db_path)
            logger.debug(
                "Synced customers from MQTT topics (%d distinct topic rows).",
                n_src,
            )
    except Exception as exc:
        if "locked" in str(exc).lower():
            logger.warning("Database locked during backfill - skipping. It will sync during message ingestion.")
        else:
            logger.warning("customers backfill on init: %s", exc)


# ─────────────────────────────────────────────
# MAC ID helpers
# ─────────────────────────────────────────────
def get_registered_mac_ids(db_path: str = DB_PATH) -> set[str]:
    """
    Return the full set of registered MAC IDs.
    Called by the ingestion service before every batch insert to refresh the
    allow-list without restarting.
    """
    conn = get_connection(db_path)
    rows = conn.execute("SELECT mac_id FROM users").fetchall()
    return {row["mac_id"] for row in rows}


def is_mac_registered(mac_id: str, db_path: str = DB_PATH) -> bool:
    """Check whether a single MAC ID is registered."""
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT 1 FROM users WHERE mac_id = ? LIMIT 1", (mac_id,)
    ).fetchone()
    return row is not None


# ─────────────────────────────────────────────
# User / device registration
# ─────────────────────────────────────────────
def register_device(username: str, mac_id: str, db_path: str = DB_PATH) -> dict:
    """
    Insert a new device (username + MAC ID) into the users table.

    Returns:
        {"success": True}  on success
        {"success": False, "error": "<reason>"}  on failure
    """
    mac_id = mac_id.strip().upper()
    with managed_connection(db_path) as conn:
        try:
            conn.execute(
                "INSERT INTO users (username, mac_id) VALUES (?, ?)",
                (username.strip(), mac_id),
            )
            logger.info("Registered device: username=%s  mac_id=%s", username, mac_id)
            return {"success": True}
        except sqlite3.IntegrityError:
            return {"success": False, "error": f"MAC ID '{mac_id}' is already registered."}


def delete_device(mac_id: str, db_path: str = DB_PATH) -> dict:
    """
    Delete a device from the users table by MAC ID.

    Returns:
        {"success": True}  on success
        {"success": False, "error": "<reason>"}  on failure
    """
    with managed_connection(db_path) as conn:
        cursor = conn.execute("DELETE FROM users WHERE mac_id = ?", (mac_id,))
        if cursor.rowcount == 0:
            return {"success": False, "error": f"MAC ID '{mac_id}' not found."}
        logger.info("Deleted device: mac_id=%s", mac_id)
        return {"success": True}


def get_all_users(db_path: str = DB_PATH) -> list[dict]:
    """Return all rows from the users table as a list of dicts."""
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT id, username, mac_id, created_at FROM users ORDER BY created_at DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def get_mac_ids_for_user(username: str, db_path: str = DB_PATH) -> list[str]:
    """Return all MAC IDs registered under a given username."""
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT mac_id FROM users WHERE username = ?", (username,)
    ).fetchall()
    return [r["mac_id"] for r in rows]


def get_all_usernames(db_path: str = DB_PATH) -> list[str]:
    """Return a de-duplicated list of registered usernames."""
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT DISTINCT username FROM users ORDER BY username"
    ).fetchall()
    return [r["username"] for r in rows]


def get_username_for_mac(mac_id: str, db_path: str = DB_PATH) -> Optional[str]:
    """Return the username registered for a given MAC ID."""
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT username FROM users WHERE mac_id = ? LIMIT 1", (mac_id,)
    ).fetchone()
    return row["username"] if row else None


def list_customer_ids_from_flosenso_topics(db_path: str = DB_PATH) -> list[str]:
    """
    Distinct customer IDs from ``mqtt_messages.topic`` when the topic has the shape
    ``flosenso&<mac_id>&<customer_id>`` (customer is the third ``&`` segment).
    """
    out: set[str] = set()
    with sqlite3.connect(db_path, check_same_thread=False) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT DISTINCT topic
            FROM   mqtt_messages
            WHERE  topic LIKE 'flosenso&%'
            """
        ).fetchall()
    for r in rows:
        parts = (r["topic"] or "").split("&")
        if len(parts) == 3 and (parts[0] or "").lower() == "flosenso":
            out.add(parts[2].strip())
    return sorted(s for s in out if s)


def list_mac_ids_for_flosenso_customer(
    customer_id: str, db_path: str = DB_PATH
) -> list[str]:
    """
    Distinct ``mqtt_messages.mac_id`` for rows whose ``topic`` matches
    ``flosenso&<mac_id>&<customer_id>`` for the given ``customer_id``.

    If ``mac_id`` is empty for a row, the middle segment of ``topic`` is used.
    """
    cid = (customer_id or "").strip()
    if not cid:
        return []
    out: set[str] = set()
    with sqlite3.connect(db_path, check_same_thread=False) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT DISTINCT topic, mac_id
            FROM   mqtt_messages
            WHERE  topic LIKE 'flosenso&%'
            """,
        ).fetchall()
    for r in rows:
        parts = (r["topic"] or "").split("&")
        if len(parts) != 3 or (parts[0] or "").lower() != "flosenso":
            continue
        if parts[2].strip() != cid:
            continue
        col_mac = (r["mac_id"] or "").strip()
        topic_mac = parts[1].strip()
        raw = col_mac if col_mac else topic_mac
        if raw:
            out.add(normalise_mac(raw))
    return sorted(s for s in out if s)


def mqtt_customer_username_labels(
    customer_ids: list[str], db_path: str = DB_PATH
) -> dict[str, str]:
    """
    Map each topic account id (third ``flosenso&…&`` segment) to a display string:
    registered **username** when any MAC for that id matches ``users``, else the id.

    Scans ``mqtt_messages`` once (not once per id) — safe for large tables / Streamlit.
    """
    want = {(c or "").strip() for c in customer_ids if c and str(c).strip()}
    if not want:
        return {}
    with sqlite3.connect(db_path, check_same_thread=False) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT DISTINCT topic, mac_id
            FROM   mqtt_messages
            WHERE  topic LIKE 'flosenso&%'
            """
        ).fetchall()
    cid_macs: dict[str, set[str]] = {c: set() for c in want}
    for r in rows:
        parts = (r["topic"] or "").split("&")
        if len(parts) != 3 or (parts[0] or "").lower() != "flosenso":
            continue
        cid = parts[2].strip()
        if cid not in want:
            continue
        col_mac = (r["mac_id"] or "").strip()
        topic_mac = parts[1].strip()
        raw = col_mac if col_mac else topic_mac
        if raw:
            cid_macs[cid].add(normalise_mac(raw))
    conn = get_connection(db_path)
    user_rows = conn.execute(
        "SELECT DISTINCT username, mac_id FROM users"
    ).fetchall()
    user_names_for_norm: dict[str, set[str]] = {}
    for r in user_rows:
        nm = normalise_mac(r["mac_id"] or "")
        if not nm:
            continue
        user_names_for_norm.setdefault(nm, set()).add(r["username"])
    out: dict[str, str] = {}
    for cid, macs in cid_macs.items():
        names: set[str] = set()
        for m in macs:
            names.update(user_names_for_norm.get(m, ()))
        if not names:
            out[cid] = cid
            continue
        ordered = sorted(names)
        if len(ordered) == 1:
            out[cid] = ordered[0]
        else:
            out[cid] = f"{ordered[0]} (+{len(ordered) - 1} more)"
    for cid in want:
        out.setdefault(cid, cid)
    return out


def display_username_for_customer_id(customer_id: str, db_path: str = DB_PATH) -> str:
    """
    Label for one MQTT-derived account id (thin wrapper around
    :func:`mqtt_customer_username_labels`).
    """
    cid = (customer_id or "").strip()
    if not cid:
        return ""
    return mqtt_customer_username_labels([cid], db_path).get(cid, cid)


def list_distinct_mac_ids_from_mqtt_messages(db_path: str = DB_PATH) -> list[str]:
    """Distinct non-empty ``mac_id`` values in ``mqtt_messages`` (any topic shape)."""
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


def list_distinct_payloads(db_path: str = DB_PATH) -> list[str]:
    """Return all unique payloads from mqtt_messages."""
    conn = get_connection(db_path)
    rows = conn.execute(
        """
        SELECT DISTINCT payload
        FROM   mqtt_messages
        WHERE  payload IS NOT NULL AND payload != ''
        ORDER  BY payload
        """
    ).fetchall()
    return [r["payload"] for r in rows]


def get_device_info_for_payload(payload: str, db_path: str = DB_PATH) -> list[dict]:
    """
    Find all (mac_id, customer_id) pairs associated with a specific payload.
    Extracts customer_id from topic if available.
    """
    conn = get_connection(db_path)
    rows = conn.execute(
        """
        SELECT DISTINCT mac_id, topic
        FROM   mqtt_messages
        WHERE  payload = ?
        """,
        (payload,),
    ).fetchall()
    
    results = []
    seen = set()
    for r in rows:
        mac = normalise_mac(r["mac_id"] or "")
        topic = (r["topic"] or "").strip()
        
        # Resolve customer ID from topic or database
        parts = topic.split("&")
        cust_id = "Unknown"
        if len(parts) == 3 and parts[0].lower() == "flosenso":
            cust_id = parts[2].strip()
        else:
            # Fallback to database lookup for MAC
            resolved = resolve_flosenso_customer_id_for_mac(mac, db_path)
            if resolved:
                cust_id = resolved
        
        pair = (mac, cust_id)
        if pair not in seen:
            results.append({"mac_id": mac, "customer_id": cust_id})
            seen.add(pair)
    return results


def list_customer_ids_from_customers(db_path: str = DB_PATH) -> list[str]:
    """Distinct ``customer_id`` values from ``customers``, sorted."""
    with sqlite3.connect(db_path, check_same_thread=False) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT DISTINCT customer_id FROM customers ORDER BY customer_id"
        ).fetchall()
    return [str(r["customer_id"]) for r in rows]


def list_mac_ids_for_customer_from_customers(
    customer_id: str, db_path: str = DB_PATH
) -> list[str]:
    """MAC IDs linked to ``customer_id`` in ``customers``, sorted."""
    with sqlite3.connect(db_path, check_same_thread=False) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT mac_id FROM customers
            WHERE customer_id = ?
            ORDER BY mac_id
            """,
            (customer_id,),
        ).fetchall()
    return [str(r["mac_id"]) for r in rows]


def insert_customer_pair(
    customer_id: str, mac_id: str, timestamp: str, received_at: str, db_path: str = DB_PATH
) -> dict:
    """
    Insert one row into ``customers`` if the pair is not already stored.

    Returns:
        {"success": True} (inserted or skipped duplicate),
        or {"success": False, "error": "..."} on validation failure.
    """
    cid = (customer_id or "").strip()
    mid = (mac_id or "").strip().upper()
    if not cid:
        return {"success": False, "error": "Customer ID cannot be empty."}
    if not mid:
        return {"success": False, "error": "MAC ID cannot be empty."}
    with sqlite3.connect(db_path, check_same_thread=False, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        cur = conn.execute(
            "INSERT OR IGNORE INTO customers (customer_id, mac_id, timestamp, received_at) VALUES (?, ?,?,?)",
            (cid, mid, timestamp, received_at),
        )
        conn.commit()
        inserted = cur.rowcount == 1
    if inserted:
        logger.info("Inserted customers row: customer_id=%s mac_id=%s", cid, mid)
    else:
        logger.debug(
            "Skipped customers insert (already stored): customer_id=%s mac_id=%s",
            cid,
            mid,
        )
    return {"success": True, "inserted": inserted}


def sync_customers_from_flosenso_message_batch(
    messages: list[dict], db_path: str = DB_PATH
) -> None:
    """
    For each ingested message whose ``topic`` is ``flosenso&<mac_id>&<customer_id>``,
    upsert into ``customers`` via ``INSERT OR IGNORE`` (unique on pair).
    Batched in one transaction after ``mqtt_messages`` inserts.
    """
    pairs: set[tuple[str, str]] = set()
    for m in messages:
        topic = (m.get("topic") or "").strip()
        parts = topic.split("&")
        if len(parts) != 3:
            continue
        if (parts[0] or "").lower() != "flosenso":
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
    """
    Populate ``customers`` from existing ``mqtt_messages`` rows whose topics look
    like ``flosenso&<mac>&<customer_id>``. Idempotent (``INSERT OR IGNORE``).

    Returns:
        Number of distinct (topic, mac_id) source rows fed into the sync.
    """
    with sqlite3.connect(db_path, check_same_thread=False, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT DISTINCT topic, mac_id
            FROM   mqtt_messages
            WHERE  topic LIKE 'flosenso&%'
            """
        ).fetchall()
    msgs = [{"topic": r["topic"] or "", "mac_id": r["mac_id"] or ""} for r in rows]
    if not msgs:
        return 0
    sync_customers_from_flosenso_message_batch(msgs, db_path)
    return len(msgs)


# ─────────────────────────────────────────────
# Data Retention / Cleanup
# ─────────────────────────────────────────────
def cleanup_old_data(days: int = 30, db_path: str = DB_PATH) -> int:
    """
    Delete messages and customer associations older than the specified number of days.
    Uses IST wall clock for comparison, matching how ingestion writes timestamps.

    Returns:
        Number of rows deleted from mqtt_messages.
    """
    global _last_cleanup_time
    
    # Throttle cleanup to run at most once per minute to avoid overhead
    now_mono = time.monotonic()
    with _cleanup_lock:
        if now_mono - _last_cleanup_time < 60:
            return 0
        _last_cleanup_time = now_mono

    # Calculate cutoff in IST
    cutoff = (datetime.now(_IST) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    
    deleted_count = 0
    try:
        with managed_connection(db_path) as conn:
            # Delete from mqtt_messages
            cursor = conn.execute(
                "DELETE FROM mqtt_messages WHERE timestamp < ?",
                (cutoff,)
            )
            deleted_count = cursor.rowcount
            
            # Also cleanup customers table if needed
            # (only if they were added more than 30 days ago and no longer have recent messages)
            conn.execute(
                "DELETE FROM customers WHERE timestamp < ?",
                (cutoff,)
            )
            
        if deleted_count > 0:
            logger.debug("Data Retention: Deleted %d messages older than %d days (cutoff: %s)", 
                         deleted_count, days, cutoff)
    except Exception as exc:
        logger.error("Data Retention: Cleanup failed: %s", exc)
        
    return deleted_count


# ─────────────────────────────────────────────
# Message ingestion (batch insert)
# ─────────────────────────────────────────────
def batch_insert_messages(messages: list[dict], db_path: str = DB_PATH) -> int:
    """
    Insert a list of message dicts in a single transaction.

    Each dict must have keys:
        topic, mac_id, payload, qos, retain, broker_name, timestamp

    Returns the number of rows inserted.
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
        
    # Trigger 30-day retention cleanup
    cleanup_old_data(days=30, db_path=db_path)
    
    return len(messages)


# ─────────────────────────────────────────────
# Dashboard query helpers
# ─────────────────────────────────────────────
def get_latest_messages(limit: int = 200, db_path: str = DB_PATH) -> list[dict]:
    """Return the most recent N messages for the Live Monitor tab."""
    conn = get_connection(db_path)
    rows = conn.execute(
        """
        SELECT broker_name, topic, mac_id, payload, timestamp
        FROM   mqtt_messages
        ORDER  BY id DESC
        LIMIT  ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def _mac_lookup_variants(mac_id: str) -> list[str]:
    """Distinct MAC strings to match DB / topic (colon case, etc.)."""
    b = (mac_id or "").strip()
    if not b:
        return []
    n = normalise_mac(b)
    out: list[str] = []
    for x in (n, b, b.upper(), n.upper()):
        if x and x not in out:
            out.append(x)
    return out


def resolve_flosenso_customer_id_for_mac(
    mac_id: str, db_path: str = DB_PATH
) -> Optional[str]:
    """
    Third segment of ``flosenso&<mac>&<customer_id>`` for this device.

    Prefers ``customers`` (filled from ingested Flosenso topics). If none,
    uses the most recent matching ``mqtt_messages`` row (by ``mac_id`` or
    topic middle segment).
    """
    variants = _mac_lookup_variants(mac_id)
    if not variants:
        return None
    ph = ",".join("?" * len(variants))
    with sqlite3.connect(db_path, check_same_thread=False) as conn:
        conn.row_factory = sqlite3.Row
        crows = conn.execute(
            f"""
            SELECT DISTINCT TRIM(customer_id) AS cid
            FROM   customers
            WHERE  TRIM(mac_id) IN ({ph})
            """,
            tuple(variants),
        ).fetchall()
    cids = sorted({str(r["cid"]).strip() for r in crows if r["cid"] and str(r["cid"]).strip()})
    if cids:
        return cids[0]

    likes = " OR ".join(["LOWER(topic) LIKE LOWER(?)" for _ in variants])
    topic_params = [f"flosenso&{v}&%" for v in variants]
    with sqlite3.connect(db_path, check_same_thread=False) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            f"""
            SELECT topic FROM mqtt_messages
            WHERE  topic LIKE 'flosenso&%'
              AND (TRIM(mac_id) IN ({ph}) OR ({likes}))
            ORDER  BY datetime(received_at) DESC, id DESC
            LIMIT  1
            """,
            (*variants, *topic_params),
        ).fetchone()
    if not row or not row["topic"]:
        return None
    parts = (row["topic"] or "").split("&")
    if len(parts) == 3 and (parts[0] or "").lower() == "flosenso":
        cand = parts[2].strip()
        if cand:
            return cand
    return None


def get_messages_for_device(
    mac_id: Optional[str] = None,
    hours: Optional[int] = None,
    limit: int = 50000,
    db_path: str = DB_PATH,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    broker_name: Optional[str] = None,
    payload_search: Optional[str] = None,
) -> list[dict]:
    """
    Return messages optionally filtered by MAC ID, time window, broker, and/or payload.

    Args:
        mac_id:  MAC as shown in the UI (matched flexibly against ``mac_id`` / topic).
        hours:   If set (and no start/end dates), only return messages from the last N hours
                 (IST wall clock, consistent with how ingestion writes ``timestamp``).
        limit:   Max rows to return (safety cap).
        start_date, end_date: If both set, filter ``DATE(timestamp)`` to this inclusive range
                 (takes precedence over ``hours``).
        broker_name: If set, only rows for this broker (e.g. ``Broker-1``).
        payload_search: If set, only return messages with this exact payload.
    """
    conn = get_connection(db_path)
    
    where_clauses = []
    params = []
    
    if mac_id:
        variants = _mac_lookup_variants(mac_id)
        if variants:
            ph = ",".join("?" * len(variants))
            topic_clauses = " OR ".join(["LOWER(topic) LIKE LOWER(?)" for _ in variants])
            where_clauses.append(f"(TRIM(mac_id) IN ({ph}) OR {topic_clauses})")
            params.extend(variants)
            params.extend(f"flosenso&{v}&%" for v in variants)
            
    if broker_name:
        where_clauses.append("broker_name = ?")
        params.append(broker_name)
        
    if payload_search:
        where_clauses.append("payload = ?")
        params.append(payload_search)
        
    if start_date is not None and end_date is not None:
        where_clauses.append("date(timestamp) BETWEEN date(?) AND date(?)")
        params.extend([str(start_date), str(end_date)])
    elif hours is not None:
        cutoff = (datetime.now(_IST) - timedelta(hours=hours)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        where_clauses.append("timestamp >= ?")
        params.append(cutoff)
        
    where_str = ""
    if where_clauses:
        where_str = "WHERE " + " AND ".join(where_clauses)
        
    sql = f"""
        SELECT broker_name, topic, mac_id, payload, qos, retain, timestamp, received_at
        FROM   mqtt_messages
        {where_str}
        ORDER  BY timestamp DESC
        LIMIT  ?
    """
    params.append(limit)
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def get_message_count_by_broker(db_path: str = DB_PATH) -> list[dict]:
    """Return per-broker message counts (useful for a small stats panel)."""
    conn = get_connection(db_path)
    rows = conn.execute(
        """
        SELECT broker_name, COUNT(*) AS total
        FROM   mqtt_messages
        GROUP  BY broker_name
        """
    ).fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
# Authentication
# ─────────────────────────────────────────────
def verify_admin_login(username, password, db_path=DB_PATH):
    """
    Check if the username and password match an entry in the admins table.
    Returns True if valid, False otherwise.
    """
    import hashlib
    h = hashlib.sha256(password.encode()).hexdigest()
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT 1 FROM admins WHERE username = ? AND password_hash = ?",
        (username, h)
    ).fetchone()
    return row is not None


def create_admin_user(username, password, db_path=DB_PATH):
    """Create a new admin user."""
    import hashlib
    h = hashlib.sha256(password.encode()).hexdigest()
    try:
        with managed_connection(db_path) as conn:
            conn.execute(
                "INSERT INTO admins (username, password_hash) VALUES (?, ?)",
                (username, h)
            )
        return True
    except sqlite3.IntegrityError:
        return False