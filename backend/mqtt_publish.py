"""
mqtt_publish.py
---------------
Thin publish helper for the API. Reuses the broker definitions from
``mqtt_service.BROKERS`` (the same brokers the ingestion service listens on) so
Quick Actions / custom commands go out over the configured HiveMQ brokers.
"""

from __future__ import annotations

import ssl

import paho.mqtt.client as mqtt

from mqtt_service import BROKERS


def publish_message(
    topic: str,
    message: str,
    broker_name: str | None = None,
    qos: int = 1,
    retain: bool = False,
) -> tuple[bool, str]:
    """
    Publish ``message`` to ``topic``. If ``broker_name`` is given, only that
    broker is tried; otherwise each configured broker is attempted until one
    succeeds. Returns ``(ok, broker_name_used)``.
    """
    targets = (
        [b for b in BROKERS if b.name == broker_name] if broker_name else list(BROKERS)
    )
    last_err = ""
    for cfg in targets:
        try:
            client = mqtt.Client(
                client_id="flosenso-api-pub",
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                clean_session=True,
            )
            if cfg.username:
                client.username_pw_set(cfg.username, cfg.password)
            if cfg.use_tls:
                client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)
            client.connect(cfg.host, cfg.port, keepalive=30)
            client.loop_start()
            result = client.publish(topic, message, qos=qos, retain=retain)
            result.wait_for_publish(timeout=5)
            client.loop_stop()
            client.disconnect()
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                return True, cfg.name
        except Exception as exc:  # noqa: BLE001 - report to caller
            last_err = f"[{cfg.name}] {exc}"
    return False, last_err
