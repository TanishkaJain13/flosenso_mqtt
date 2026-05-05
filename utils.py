"""
utils.py
--------
Shared utility functions used by both the ingestion service and the dashboard:
  - Topic parsing
  - MAC ID normalisation and validation
  - Timestamp formatting
  - Logging setup
"""

import re
import logging
from datetime import datetime, timedelta, timezone

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────
TOPIC_SEPARATOR = "&"
TIMESTAMP_FMT   = "%Y-%m-%d %H:%M:%S"   # YYYY-MM-DD HH:MM:SS (with seconds)

# Standard colon-separated MAC: 8C:AA:B5:D5:4A:EE
_MAC_RE = re.compile(
    r"^([0-9A-Fa-f]{2}[:\-]){5}([0-9A-Fa-f]{2})$"
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
def setup_logging(level: int = logging.INFO, log_file: str | None = None) -> None:
    """
    Configure root logger with a consistent format.
    Optionally mirror output to a file.
    """
    fmt = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, handlers=handlers)


# ─────────────────────────────────────────────
# Topic parsing
# ─────────────────────────────────────────────
def parse_topic(topic: str) -> dict:
    """
    Parse an MQTT topic that follows the pattern:
        <product_name>&<mac_id>&<device_id>

    Returns a dict with keys:
        product_name, mac_id, device_id, valid

    If the topic does not match the expected format 'valid' is False and
    the other fields are empty strings.
    """
    parts = topic.split(TOPIC_SEPARATOR)
    if len(parts) >= 3:
        return {
            "product_name": parts[0],
            "mac_id":       normalise_mac(parts[1]),
            "device_id":    parts[2],
            "valid":        True,
        }

    logger.debug("Unparseable topic: %s", topic)
    return {"product_name": "", "mac_id": "", "device_id": "", "valid": False}


def extract_mac_from_topic(topic: str) -> str:
    """
    Convenience wrapper: return only the normalised MAC ID from a topic string.
    Returns an empty string if the topic is malformed.
    """
    return parse_topic(topic)["mac_id"]


# ─────────────────────────────────────────────
# MAC ID helpers
# ─────────────────────────────────────────────
def normalise_mac(mac: str) -> str:
    """
    Normalise a MAC address to upper-case colon-separated format.
    E.g.  "8c-aa-b5-d5-4a-ee"  →  "8C:AA:B5:D5:4A:EE"
         "8cAAb5d54aee"         →  "8C:AA:B5:D5:4A:EE"
    Returns the original string (uppercased) if it cannot be normalised.
    """
    mac = mac.strip().upper()

    # Already normalised?
    if _MAC_RE.match(mac):
        return mac.replace("-", ":")

    # Strip any separators and try to rebuild
    stripped = re.sub(r"[:\-\. ]", "", mac)
    if len(stripped) == 12 and all(c in "0123456789ABCDEF" for c in stripped):
        return ":".join(stripped[i : i + 2] for i in range(0, 12, 2))

    logger.warning("Could not normalise MAC address: %s", mac)
    return mac


def validate_mac(mac: str) -> bool:
    """
    Return True if 'mac' is a valid colon- or dash-separated MAC address.
    Accepts both upper and lower case.
    """
    return bool(_MAC_RE.match(mac.strip()))


def validate_and_normalise_mac(mac: str) -> tuple[bool, str]:
    """
    Validate and normalise in one call.

    Returns:
        (True,  normalised_mac)  if valid
        (False, original_input)  if invalid
    """
    normalised = normalise_mac(mac)
    if validate_mac(normalised):
        return True, normalised
    return False, mac.strip()


# ─────────────────────────────────────────────
# Timestamp helpers
# ─────────────────────────────────────────────
def utc_now_str() -> str:
    """Return the current time in IST (UTC+5:30) as a formatted string (YYYY-MM-DD HH:MM:SS)."""
    IST = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(IST).strftime(TIMESTAMP_FMT)


def format_timestamp(dt: datetime | None) -> str:
    """Format a datetime object in IST.  Returns empty string for None."""
    if dt is None:
        return ""
    IST = timezone(timedelta(hours=5, minutes=30))
    if dt.tzinfo is None:
        # Treat naive datetimes as UTC, then convert to IST
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST).strftime(TIMESTAMP_FMT)