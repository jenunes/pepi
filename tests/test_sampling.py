from pepi.sampling import get_sample_rate, get_sample_rate_from_percentage, should_sample_data


def test_should_sample_data_uses_threshold() -> None:
    assert not should_sample_data(10_000)
    assert should_sample_data(60_000)


def test_get_sample_rate_ranges() -> None:
    assert get_sample_rate(40_000) == 1
    assert get_sample_rate(100_000) == 5
    assert get_sample_rate(300_000) == 10
    assert get_sample_rate(800_000) == 20


def test_get_sample_rate_from_percentage() -> None:
    assert get_sample_rate_from_percentage(50, 1000) == 2
    assert get_sample_rate_from_percentage(100, 1000) == 1
