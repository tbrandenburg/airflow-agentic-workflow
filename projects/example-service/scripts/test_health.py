from service import health


def test_health() -> None:
    assert health() == "healthy"
