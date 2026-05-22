"""
mqtt_service.py
---------------
High-performance MQTT ingestion service.

Architecture (preserved from original service):
  • Two paho-mqtt clients run in background threads (one per broker).
  • on_message callbacks push lightweight dicts onto a shared thread-safe queue.
  • A dedicated writer thread drains the queue in configurable batches and
    performs bulk SQLite inserts for maximum throughput (10 000+ msg/min).
  • Registered MAC IDs are cached and refreshed periodically so the allow-list
    stays current without a restart. Topics ``flosenso&<mac>&<customer_id>`` are
    always stored (bootstrap) so customer/MAC lists can populate before devices
    are added to ``users``.
"""

import logging
import queue
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

import paho.mqtt.client as mqtt

from database import init_db, batch_insert_messages, get_registered_mac_ids
from utils import extract_mac_from_topic, normalise_mac, utc_now_str, setup_logging

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
# setup_logging is now called within main() to prevent import side-effects
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Broker configuration  ← edit these values
# ─────────────────────────────────────────────
@dataclass
class BrokerConfig:
    name:        str
    host:        str
    port:        int   = 8883
    username:    str   = ""
    password:    str   = ""
    use_tls:     bool  = True
    client_id:   str   = ""
    keepalive:   int   = 60


BROKERS: list[BrokerConfig] = [
    BrokerConfig(
        name      = "Broker-1",
        host      = "b9ff4eeb544e4ac18142a019bc399b43.s2.eu.hivemq.cloud",   # ← replace
        port      = 8883,
        username  = "energybotsmqtt",                   # ← replace
        password  = "ebpl2015",                   # ← replace
        use_tls   = True,
        client_id = "flosenso-ingestion-1",
    ),
    BrokerConfig(
        name      = "Broker-2",
        host      = "newflo-789c1e1b.a03.euc1.aws.hivemq.cloud",                # ← replace
        port      = 8883,
        username  = "flosensomqtt",                   # ← replace
        password  = "viPbi6-redvet-jaskod",                   # ← replace
        use_tls   = True,
        client_id = "flosenso-ingestion-2",
    ),
]


# ─────────────────────────────────────────────
# Tuning knobs
# ─────────────────────────────────────────────
BATCH_SIZE          = 5000   # rows per SQLite transaction
BATCH_INTERVAL_SEC  = 1.0    # max seconds between flushes
MAC_CACHE_REFRESH   = 30     # seconds between allow-list refreshes
QUEUE_MAX_SIZE      = 50_000 # back-pressure safety valve
RECONNECT_DELAY_SEC = 5      # seconds before a reconnect attempt


# ─────────────────────────────────────────────
# Shared state
# ─────────────────────────────────────────────
message_queue: queue.Queue[dict] = queue.Queue(maxsize=QUEUE_MAX_SIZE)
_stop_event = threading.Event()

# Allow-list: set of registered MAC IDs, updated periodically
_mac_cache: set[str] = set()
_mac_cache_lock = threading.Lock()


# ─────────────────────────────────────────────
# MAC allow-list refresher
# ─────────────────────────────────────────────
def _mac_refresher_thread() -> None:
    """Refresh the MAC allow-list from the database every MAC_CACHE_REFRESH seconds."""
    global _mac_cache
    while not _stop_event.is_set():
        try:
            fresh = get_registered_mac_ids()
            with _mac_cache_lock:
                _mac_cache = fresh
            logger.debug("MAC allow-list refreshed: %d entries", len(fresh))
        except Exception as exc:
            logger.error("MAC allow-list refresh failed: %s", exc)
        _stop_event.wait(MAC_CACHE_REFRESH)


def _is_mac_allowed(mac_id: str) -> bool:
    with _mac_cache_lock:
        return mac_id in _mac_cache


def _is_flosenso_routed_topic(topic: str) -> bool:
    """
    True when the topic is ``flosenso&<mac_segment>&<customer_id>`` with all
    three segments non-empty. These rows bootstrap ``customers`` / dashboards
    and may be ingested even if the MAC is not yet on the allow-list.
    """
    parts = topic.split("&")
    if len(parts) != 3:
        return False
    if (parts[0] or "").strip().lower() != "flosenso":
        return False
    return bool((parts[1] or "").strip()) and bool((parts[2] or "").strip())


# ─────────────────────────────────────────────
# MQTT client factory
# ─────────────────────────────────────────────
def _build_client(cfg: BrokerConfig) -> mqtt.Client:
    client = mqtt.Client(
        client_id          = cfg.client_id or f"flosenso-{cfg.name}",
        protocol           = mqtt.MQTTv311,
        transport          = "tcp",
        callback_api_version = mqtt.CallbackAPIVersion.VERSION2,
    )

    if cfg.username:
        client.username_pw_set(cfg.username, cfg.password)

    if cfg.use_tls:
        client.tls_set()

    # ── Callbacks ────────────────────────────
    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("[%s] Connected to broker %s:%d", cfg.name, cfg.host, cfg.port)
            client.subscribe("#", qos=1)
            logger.info("[%s] Subscribed to '#'", cfg.name)
        else:
            logger.warning("[%s] Connection refused, reason code=%s", cfg.name, reason_code)

    def on_disconnect(client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            logger.warning("[%s] Unexpected disconnect (rc=%s), will reconnect…", cfg.name, reason_code)

    def on_message(client, userdata, msg):
        try:
            flos = _is_flosenso_routed_topic(msg.topic)
            mac_id = extract_mac_from_topic(msg.topic)
            if not mac_id and flos:
                mac_id = normalise_mac(msg.topic.split("&")[1])
            if not mac_id:
                return  # malformed topic — skip silently

            if not _is_mac_allowed(mac_id) and not flos:
                return  # MAC not registered — ignore (except Flosenso routed topics)

            record = {
                "topic":       msg.topic,
                "mac_id":      mac_id,
                "payload":     msg.payload.decode("utf-8", errors="replace"),
                "qos":         msg.qos,
                "retain":      int(msg.retain),
                "broker_name": cfg.name,
                "timestamp":   utc_now_str(),
            }
            try:
                message_queue.put_nowait(record)
            except queue.Full:
                logger.warning("[%s] Queue full — message dropped", cfg.name)

        except Exception as exc:
            logger.error("[%s] on_message error: %s", cfg.name, exc, exc_info=True)

    def on_log(client, userdata, level, buf):
        logger.debug("[%s] paho: %s", cfg.name, buf)

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message
    client.on_log        = on_log

    return client


# ─────────────────────────────────────────────
# Broker connection thread
# ─────────────────────────────────────────────
def _broker_thread(cfg: BrokerConfig) -> None:
    """
    Runs in its own thread.  Connects to the broker and starts the paho
    network loop.  Retries automatically on disconnect.
    """
    client = _build_client(cfg)

    while not _stop_event.is_set():
        try:
            logger.info("[%s] Connecting to %s:%d …", cfg.name, cfg.host, cfg.port)
            client.connect(cfg.host, cfg.port, keepalive=cfg.keepalive)
            client.loop_forever(retry_first_connection=True)
        except Exception as exc:
            logger.error("[%s] Connection error: %s — retrying in %ds",
                         cfg.name, exc, RECONNECT_DELAY_SEC)
        finally:
            try:
                client.disconnect()
            except Exception:
                pass

        if not _stop_event.is_set():
            _stop_event.wait(RECONNECT_DELAY_SEC)

    logger.info("[%s] Broker thread stopped.", cfg.name)


# ─────────────────────────────────────────────
# Writer thread (queue → SQLite)
# ─────────────────────────────────────────────
def _writer_thread() -> None:
    """
    Drains the shared message queue in batches and writes to SQLite.
    Flushes when BATCH_SIZE is reached or BATCH_INTERVAL_SEC elapses,
    whichever comes first.
    """
    logger.info("Writer thread started (batch_size=%d, interval=%.1fs)",
                BATCH_SIZE, BATCH_INTERVAL_SEC)
    buffer: list[dict] = []
    last_flush = time.monotonic()

    while not _stop_event.is_set() or not message_queue.empty():
        # Drain available messages up to BATCH_SIZE
        while len(buffer) < BATCH_SIZE:
            try:
                record = message_queue.get_nowait()
                buffer.append(record)
            except queue.Empty:
                break

        now = time.monotonic()
        should_flush = (
            len(buffer) >= BATCH_SIZE
            or (buffer and (now - last_flush) >= BATCH_INTERVAL_SEC)
        )

        if should_flush:
            try:
                inserted = batch_insert_messages(buffer)
                logger.debug("Flushed %d messages to DB (queue depth: %d)",
                             inserted, message_queue.qsize())
            except Exception as exc:
                logger.error("Batch insert failed: %s", exc, exc_info=True)
            finally:
                buffer.clear()
                last_flush = now
        else:
            # Avoid busy-waiting when the queue is quiet
            time.sleep(0.05)

    # Final flush on shutdown
    if buffer:
        try:
            batch_insert_messages(buffer)
            logger.info("Final flush: %d messages", len(buffer))
        except Exception as exc:
            logger.error("Final flush error: %s", exc)

    logger.info("Writer thread stopped.")


# ─────────────────────────────────────────────
# Stats logger (optional — logs throughput every 60 s)
# ─────────────────────────────────────────────
def _stats_thread() -> None:
    _prev_qsize = 0
    while not _stop_event.is_set():
        _stop_event.wait(60)
        qsize = message_queue.qsize()
        logger.info("Queue depth: %d  (delta: %+d)", qsize, qsize - _prev_qsize)
        _prev_qsize = qsize


# ─────────────────────────────────────────────
# Graceful shutdown
# ─────────────────────────────────────────────
def _handle_signal(signum, frame):
    logger.info("Shutdown signal received (%s) — stopping …", signum)
    _stop_event.set()


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
def main() -> None:
    setup_logging(level=logging.INFO)
    logger.info("=" * 60)
    logger.info("  Flosenso MQTT Ingestion Service  (dual-broker)")
    logger.info("=" * 60)

    # Streamlit and some hosts run the script off the main thread; signal() requires it.
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)
    else:
        logger.info("Skipping POSIX signal handlers (not on main thread, e.g. Streamlit)")

    # Initialise database (creates tables and indexes if needed)
    init_db()

    # Pre-load MAC allow-list
    global _mac_cache
    _mac_cache = get_registered_mac_ids()
    logger.info("Loaded %d registered MAC IDs", len(_mac_cache))

    threads: list[threading.Thread] = []

    # MAC allow-list refresher
    t_mac = threading.Thread(target=_mac_refresher_thread, name="mac-refresher", daemon=True)
    t_mac.start()
    threads.append(t_mac)

    # Writer thread
    t_writer = threading.Thread(target=_writer_thread, name="db-writer", daemon=False)
    t_writer.start()
    threads.append(t_writer)

    # Stats thread
    t_stats = threading.Thread(target=_stats_thread, name="stats", daemon=True)
    t_stats.start()
    threads.append(t_stats)

    # One thread per broker
    for cfg in BROKERS:
        t = threading.Thread(
            target=_broker_thread,
            args=(cfg,),
            name=f"broker-{cfg.name}",
            daemon=True,
        )
        t.start()
        threads.append(t)
        logger.info("Started broker thread for '%s'", cfg.name)

    # Block main thread until shutdown
    _stop_event.wait()
    logger.info("Waiting for writer thread to flush …")
    t_writer.join(timeout=15)
    logger.info("Service stopped cleanly.")


if __name__ == "__main__":
    main()
