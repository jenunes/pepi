from pepi.stats import calculate_connection_stats, calculate_query_stats


def test_calculate_query_stats_basic() -> None:
    data = {
        ("test.users", "find", '{"status":"A"}'): {
            "durations": [100, 200, 300],
            "allowDiskUse": False,
            "pattern": '{"status":"A"}',
            "indexes": {"COLLSCAN"},
        }
    }

    stats = calculate_query_stats(data)
    result = stats[("test.users", "find", '{"status":"A"}')]

    assert result["count"] == 3
    assert result["min"] == 100
    assert result["max"] == 300
    assert result["mean"] == 200


def test_calculate_connection_stats_basic() -> None:
    data = {"127.0.0.1": {"durations": [1.0, 3.0]}, "10.0.0.1": {"durations": [2.0]}}

    overall, per_ip = calculate_connection_stats(data)

    assert overall is not None
    assert round(overall["avg"], 2) == 2.0
    assert per_ip["127.0.0.1"]["max"] == 3.0
