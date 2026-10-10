from service import health


def test_deployment_failure() -> None:
    assert health() == "intentional-validation-failure"
