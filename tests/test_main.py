import main
import pytest

def test_get_latest_keypass_db():
    db = main.get_latest_keypass_db()
    assert db is not None