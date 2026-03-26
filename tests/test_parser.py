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
