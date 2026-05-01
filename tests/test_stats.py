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


def test_calculate_query_stats_includes_scan_ratio() -> None:
    data = {
        ("test.col", "find", '{"x":1}'): {
            "durations": [50, 60],
            "allowDiskUse": False,
            "pattern": '{"x":1}',
            "indexes": {"IXSCAN"},
            "keysExamined": [10, 20],
            "docsExamined": [10, 20],
            "nreturned": [5, 10],
            "hasSortStage": [False, False],
            "usedDisk": [False, False],
            "numYields": [1, 2],
            "reslen": [100, 200],
            "locksPresent": [False, False],
        }
    }
    stats = calculate_query_stats(data)
    result = stats[("test.col", "find", '{"x":1}')]
    assert result["scan_ratio"] == 2.0
    assert result["key_efficiency"] == 1.0
    assert result["in_memory_sort_pct"] == 0.0
    assert result["disk_usage_pct"] == 0.0
    assert result["yield_rate"] == 1.5
    assert result["avg_response_size"] == 150.0


def test_calculate_query_stats_handles_zero_nreturned() -> None:
    data = {
        ("test.col", "find", '{"y":1}'): {
            "durations": [100],
            "allowDiskUse": False,
            "pattern": '{"y":1}',
            "indexes": {"IXSCAN"},
            "keysExamined": [500],
            "docsExamined": [500],
            "nreturned": [0],
            "hasSortStage": [False],
            "usedDisk": [False],
            "numYields": [0],
            "reslen": [0],
            "locksPresent": [False],
        }
    }
    stats = calculate_query_stats(data)
    result = stats[("test.col", "find", '{"y":1}')]
    assert result["scan_ratio"] == 0.0


def test_calculate_connection_stats_basic() -> None:
    data = {"127.0.0.1": {"durations": [1.0, 3.0]}, "10.0.0.1": {"durations": [2.0]}}

    overall, per_ip = calculate_connection_stats(data)

    assert overall is not None
    assert round(overall["avg"], 2) == 2.0
    assert per_ip["127.0.0.1"]["max"] == 3.0
