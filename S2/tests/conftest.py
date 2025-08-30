import pathlib
import pytest


@pytest.fixture(scope="session")
def repo_root():
    """Return the root directory of the S2 project."""
    return pathlib.Path(__file__).resolve().parents[1]
