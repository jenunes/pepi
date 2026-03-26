from pepi.index_advisor import analyze_single_query


def test_analyze_single_query_returns_recommendation_for_collscan() -> None:
    stats = {
        "count": 20,
        "mean": 250,
        "percentile_95": 300,
        "indexes": {"COLLSCAN"},
        "plan_summary": "COLLSCAN",
    }

    recommendation = analyze_single_query(
        namespace="test.users",
        operation="find",
        pattern='{"find":"users","filter":{"status":"A"}}',
        stats=stats,
    )

    assert recommendation is not None
    assert recommendation["namespace"] == "test.users"
    assert recommendation["current_index"] == "COLLSCAN"
