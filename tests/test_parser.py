from pepi.parser import parse_connections, parse_replica_set_config


def test_parse_connections_reads_network_events(sample_log_file) -> None:
    connections, total_opened, total_closed = parse_connections(
        str(sample_log_file),
        sample_percentage=100,
    )

    assert total_opened == 1
    assert total_closed == 1
    assert "127.0.0.1" in connections


def test_parse_replica_set_config_reads_config(sample_log_file) -> None:
    configs = parse_replica_set_config(str(sample_log_file))

    assert len(configs) == 1
    assert configs[0]["config"]["_id"] == "rs0"


def test_parse_connections_reads_modern_ingress_format(tmp_path) -> None:
    import json

    file_path = tmp_path / "modern-connections.log"
    lines = [
        json.dumps(
            {
                "t": {"$date": "2026-03-06T21:30:00.000Z"},
                "ctx": "conn1",
                "msg": "Ingress TLS handshake complete",
            }
        ),
        json.dumps(
            {
                "t": {"$date": "2026-03-06T21:30:01.000Z"},
                "ctx": "conn1",
                "msg": "Connection not authenticating",
                "attr": {"client": "10.0.0.1:27017"},
            }
        ),
        json.dumps(
            {
                "t": {"$date": "2026-03-06T21:30:05.000Z"},
                "ctx": "conn1",
                "msg": "Error sending response to client. Ending connection from remote",
                "attr": {"remote": "10.0.0.1:27017"},
            }
        ),
    ]
    file_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    connections, total_opened, total_closed = parse_connections(
        str(file_path),
        sample_percentage=100,
    )

    assert total_opened == 1
    assert total_closed == 1
    assert connections["10.0.0.1"]["opened"] == 1
    assert connections["10.0.0.1"]["closed"] == 1
