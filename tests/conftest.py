"""Pytest fixtures for ICEA tests."""
import os
import tempfile

import pytest


@pytest.fixture(scope="session")
def test_db_path():
    """Use a temporary DB for API tests so we don't touch the default DB."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        yield path
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


@pytest.fixture(autouse=True)
def set_test_db(test_db_path, monkeypatch):
    """Point store at test DB for tests that use the API (and thus store)."""
    monkeypatch.setenv("ICEA_DB_PATH", test_db_path)
