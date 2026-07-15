"""Shared helpers for MongoDB connection lifecycle log lines (classic + modern formats).

Message catalog aligned with Percona Server for MongoDB / mongod sources:
- src/mongo/transport/session_manager_common.cpp (Connection accepted/ended/refused)
- src/mongo/transport/asio/asio_session_impl.cpp (Ingress TLS handshake complete)
- src/mongo/db/repl/hello_auth.cpp (Connection not authenticating)
- src/mongo/transport/session_workflow.cpp (Error sending/receiving … remote)
- setParameter enableDetailedConnectionHealthMetricLogLines (default true since 6.3+)
"""

from __future__ import annotations

import re
from typing import Any, Literal, Optional

# Classic mongod (NETWORK component, always when not --quiet)
MSG_CONNECTION_ACCEPTED = "Connection accepted"
MSG_CONNECTION_ENDED = "Connection ended"
MSG_CONNECTION_REFUSED = "Connection refused because there are too many open connections"
LOG_ID_CONNECTION_ACCEPTED = 22943
LOG_ID_CONNECTION_ENDED = 22944
LOG_ID_CONNECTION_REFUSED = 22942

# Modern ingress health metrics (enableDetailedConnectionHealthMetricLogLines)
MSG_INGRESS_HANDSHAKE = "Ingress TLS handshake complete"
MSG_CONNECTION_NOT_AUTH = "Connection not authenticating"
LOG_ID_INGRESS_HANDSHAKE = 6723804
LOG_ID_CONNECTION_NOT_AUTH = 10483900

# Abnormal termination (EXECUTOR component)
MSG_ENDING_CONNECTION_SEND = (
    "Error sending response to client. Ending connection from remote"
)
MSG_ENDING_CONNECTION_RECV = (
    "Error receiving request from client. Ending connection from remote"
)
LOG_ID_ENDING_CONNECTION_SEND = 22989
LOG_ID_ENDING_CONNECTION_RECV = 22988

# Backward-compatible alias
MSG_ENDING_CONNECTION_REMOTE = MSG_ENDING_CONNECTION_SEND

ConnectionLogProfileName = Literal[
    "classic", "ingress_health_metrics", "mixed", "none"
]

PROFILE_NOTES: dict[ConnectionLogProfileName, str] = {
    "classic": (
        "Classic NETWORK log format (Connection accepted / Connection ended) with "
        "connectionCount and remote per event."
    ),
    "ingress_health_metrics": (
        "Modern connection health metrics format "
        "(enableDetailedConnectionHealthMetricLogLines). Global opens are tracked via "
        "'Ingress TLS handshake complete'; per-IP client identity comes from "
        "'Connection not authenticating' (attr.client). Closes may appear as "
        "'Error sending/receiving … Ending connection from remote' instead of "
        "Connection ended."
    ),
    "mixed": "Log contains both classic Connection accepted/ended and ingress health metrics lines.",
    "none": "No recognized connection lifecycle messages found in the sampled log.",
}

_CONN_CTX_RE = re.compile(r"^conn(\d+)$")


def extract_connection_id(entry: dict[str, Any]) -> Optional[int]:
    attr = entry.get("attr") or {}
    conn_id = attr.get("connectionId")
    if conn_id is not None:
        try:
            return int(conn_id)
        except (TypeError, ValueError):
            return None
    ctx = str(entry.get("ctx") or "")
    match = _CONN_CTX_RE.match(ctx)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def extract_connection_ip(entry: dict[str, Any]) -> Optional[str]:
    attr = entry.get("attr") or {}
    for key in ("remote", "client"):
        raw = attr.get(key)
        if isinstance(raw, str) and ":" in raw:
            return raw.split(":")[0]
    return None


def extract_slow_query_client_ip(entry: dict[str, Any]) -> str:
    """Client IP from COMMAND slow query / command log lines (attr.remote or attr.client)."""
    attr = entry.get("attr") or {}
    for key in ("remote", "client"):
        raw = attr.get(key)
        if isinstance(raw, str) and raw.strip():
            if ":" in raw:
                return raw.split(":")[0]
            return raw.strip()
    return "unknown"


def extract_slow_query_app_name(entry: dict[str, Any]) -> Optional[str]:
    """Application name from COMMAND attr.appName when present."""
    attr = entry.get("attr") or {}
    app_name = attr.get("appName")
    if isinstance(app_name, str) and app_name.strip():
        return app_name.strip()
    return None


def extract_tls_handshake_duration_ms(entry: dict[str, Any]) -> Optional[float]:
    if not is_ingress_handshake(entry):
        return None
    raw = (entry.get("attr") or {}).get("durationMillis")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def extract_client_metadata(entry: dict[str, Any]) -> dict[str, Any]:
    """ClientMetadata from hello_auth 'Connection not authenticating' (attr.doc)."""
    if not is_connection_not_authenticating(entry):
        return {}
    doc = (entry.get("attr") or {}).get("doc") or {}
    if not isinstance(doc, dict):
        return {}
    application = doc.get("application") or {}
    driver = doc.get("driver") or {}
    return {
        "application_name": application.get("name") if isinstance(application, dict) else None,
        "driver_name": driver.get("name") if isinstance(driver, dict) else None,
        "driver_version": driver.get("version") if isinstance(driver, dict) else None,
        "platform": doc.get("platform"),
    }


def is_classic_connection_accepted(entry: dict[str, Any]) -> bool:
    return (
        entry.get("c") == "NETWORK"
        and entry.get("msg") == MSG_CONNECTION_ACCEPTED
        and bool(entry.get("attr"))
    )


def is_classic_connection_ended(entry: dict[str, Any]) -> bool:
    return (
        entry.get("c") == "NETWORK"
        and entry.get("msg") == MSG_CONNECTION_ENDED
        and bool(entry.get("attr"))
    )


def is_connection_refused(entry: dict[str, Any]) -> bool:
    return entry.get("msg") == MSG_CONNECTION_REFUSED


def is_ingress_handshake(entry: dict[str, Any]) -> bool:
    return entry.get("msg") == MSG_INGRESS_HANDSHAKE


def is_connection_not_authenticating(entry: dict[str, Any]) -> bool:
    return entry.get("msg") == MSG_CONNECTION_NOT_AUTH


def is_connection_remote_terminated(entry: dict[str, Any]) -> bool:
    msg = entry.get("msg") or ""
    return msg in {MSG_ENDING_CONNECTION_SEND, MSG_ENDING_CONNECTION_RECV}


def is_remote_connection_close(entry: dict[str, Any]) -> bool:
    """Backward-compatible alias for send-side remote termination."""
    return is_connection_remote_terminated(entry)


def is_legacy_end_connection_message(message: str) -> bool:
    return "end connection" in message.lower()


def classify_connection_message(entry: dict[str, Any]) -> Optional[str]:
    """Return a normalized lifecycle bucket for counting / profile detection."""
    if is_classic_connection_accepted(entry):
        return "classic_accepted"
    if is_classic_connection_ended(entry):
        return "classic_ended"
    if is_connection_refused(entry):
        return "connection_refused"
    if is_ingress_handshake(entry):
        return "ingress_handshake"
    if is_connection_not_authenticating(entry):
        return "connection_not_authenticating"
    if is_connection_remote_terminated(entry):
        return "remote_terminated"
    msg = entry.get("msg") or ""
    if is_legacy_end_connection_message(msg) and entry.get("attr"):
        return "remote_terminated"
    return None


def derive_connection_log_profile(counts: dict[str, int]) -> ConnectionLogProfileName:
    has_classic = counts.get("classic_accepted", 0) > 0 or counts.get("classic_ended", 0) > 0
    has_ingress = (
        counts.get("ingress_handshake", 0) > 0
        or counts.get("connection_not_authenticating", 0) > 0
    )
    if has_classic and has_ingress:
        return "mixed"
    if has_ingress:
        return "ingress_health_metrics"
    if has_classic:
        return "classic"
    return "none"


def build_connection_log_profile(counts: dict[str, int]) -> dict[str, Any]:
    name = derive_connection_log_profile(counts)
    return {
        "profile": name,
        "counts": counts,
        "note": PROFILE_NOTES[name],
        "source_parameter": (
            "enableDetailedConnectionHealthMetricLogLines"
            if name in {"ingress_health_metrics", "mixed"}
            else None
        ),
    }
