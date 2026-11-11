import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from src.common.spark import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = get_spark("tests", master="local[2]")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def landing():
    return os.path.join(ROOT, "data", "landing")
