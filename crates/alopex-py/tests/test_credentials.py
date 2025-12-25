import os

import pytest

from alopex import AlopexError
from alopex._alopex import catalog as _catalog


def test_resolve_credentials_local_path_returns_empty(monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    result = _catalog._resolve_credentials("data/table")
    assert result == {}


def test_resolve_credentials_file_scheme_returns_empty(monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    result = _catalog._resolve_credentials("file:///tmp/data.parquet")
    assert result == {}


def test_resolve_credentials_s3_uses_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "dummy_access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "dummy_secret")
    result = _catalog._resolve_credentials("s3://bucket/path")
    assert result["aws_access_key_id"] == "dummy_access"
    assert result["aws_secret_access_key"] == "dummy_secret"


def test_resolve_credentials_unknown_scheme_raises(monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    with pytest.raises(AlopexError):
        _catalog._resolve_credentials("ftp://example.com/data")
