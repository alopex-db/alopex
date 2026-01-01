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
    monkeypatch.setenv("AWS_REGION", "ap-northeast-1")
    result = _catalog._resolve_credentials("s3://bucket/path")
    assert result["aws_access_key_id"] == "dummy_access"
    assert result["aws_secret_access_key"] == "dummy_secret"
    assert result["aws_region"] == "ap-northeast-1"


def test_resolve_credentials_gcs_uses_env(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/gcs.json")
    result = _catalog._resolve_credentials("gs://bucket/path")
    assert result["service_account_path"] == "/tmp/gcs.json"


def test_resolve_credentials_azure_uses_env(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "account")
    monkeypatch.setenv("AZURE_STORAGE_KEY", "key")
    result = _catalog._resolve_credentials("az://container/path")
    assert result["account_name"] == "account"
    assert result["account_key"] == "key"


def test_resolve_credentials_unknown_scheme_returns_empty(monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    result = _catalog._resolve_credentials("ftp://example.com/data")
    assert result == {}


def test_resolve_credentials_dict_and_storage_options():
    result = _catalog._resolve_credentials(
        "s3://bucket/path",
        credential_provider={"token": "old", "shared": "old"},
        storage_options={"shared": "new", "extra": "1"},
    )
    assert result["token"] == "old"
    assert result["shared"] == "new"
    assert result["extra"] == "1"


def test_resolve_credentials_invalid_provider_raises():
    with pytest.raises(AlopexError):
        _catalog._resolve_credentials("s3://bucket/path", "unsupported")


def test_resolve_credentials_invalid_type_raises():
    with pytest.raises(AlopexError):
        _catalog._resolve_credentials("s3://bucket/path", ["not", "dict"])
