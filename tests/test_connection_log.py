from pepi.connection_log import (
    MSG_ENDING_CONNECTION_RECV,
    build_connection_log_profile,
    classify_connection_message,
    derive_connection_log_profile,
    extract_client_metadata,
    is_connection_remote_terminated,
)
from pepi.parser import get_connection_parse_stats, parse_connections


def test_classify_modern_ingress_messages() -> None:
    ingress = {"msg": "Ingress TLS handshake complete", "ctx": "conn1"}
    auth = {
        "msg": "Connection not authenticating",
        "attr": {"client": "10.0.0.1:27017", "doc": {"application": {"name": "app1"}}},
    }
    close_send = {
        "msg": "Error sending response to client. Ending connection from remote",
        "attr": {"remote": "10.0.0.1:27017", "connectionId": 1},
    }
    close_recv = {
        "msg": MSG_ENDING_CONNECTION_RECV,
        "attr": {"remote": "10.0.0.1:27017", "connectionId": 2},
    }

    assert classify_connection_message(ingress) == "ingress_handshake"
    assert classify_connection_message(auth) == "connection_not_authenticating"
    assert classify_connection_message(close_send) == "remote_terminated"
    assert classify_connection_message(close_recv) == "remote_terminated"
    assert is_connection_remote_terminated(close_recv) is True
    assert extract_client_metadata(auth)["application_name"] == "app1"


def test_derive_connection_log_profile() -> None:
    assert derive_connection_log_profile({"classic_accepted": 1}) == "classic"
    assert derive_connection_log_profile({"ingress_handshake": 3}) == "ingress_health_metrics"
    assert derive_connection_log_profile({"classic_accepted": 1, "ingress_handshake": 2}) == "mixed"
    assert derive_connection_log_profile({}) == "none"


def test_parse_connections_stores_ingress_profile(tmp_path) -> None:
    import json

    file_path = tmp_path / "ingress.log"
    lines = [
        json.dumps(
            {
                "t": {"$date": "2026-03-06T21:30:00.000Z"},
                "ctx": "conn1",
                "msg": "Ingress TLS handshake complete",
                "attr": {"durationMillis": 2},
            }
        ),
        json.dumps(
            {
                "t": {"$date": "2026-03-06T21:30:01.000Z"},
                "ctx": "conn1",
                "msg": "Connection not authenticating",
                "attr": {"client": "10.0.0.2:12345"},
            }
        ),
    ]
    file_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    parse_connections(str(file_path), sample_percentage=100)
    stats = get_connection_parse_stats(str(file_path))
    profile = stats["connection_log_profile"]

    assert profile["profile"] == "ingress_health_metrics"
    assert profile["source_parameter"] == "enableDetailedConnectionHealthMetricLogLines"
    assert build_connection_log_profile(profile["counts"])["profile"] == "ingress_health_metrics"
