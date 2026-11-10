import pytest
from skewshield.spark import create_spark


@pytest.fixture(scope="session")
def spark():
    session = create_spark("SkewShieldTests", "local[2]", 2, True)
    yield session
    session.stop()
