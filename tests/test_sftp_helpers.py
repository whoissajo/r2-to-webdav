import pytest
from botocore.exceptions import ClientError
from paramiko import SFTP_FAILURE, SFTP_NO_SUCH_FILE

from sftp_server import Settings, error_status, key_for


def test_key_for_root_and_nested_paths():
    assert key_for("/") == ""
    assert key_for("") == ""
    assert key_for("media/movie.mp4") == "media/movie.mp4"
    assert key_for("/media/movie.mp4") == "media/movie.mp4"
    assert key_for("media/") == "media"


@pytest.mark.parametrize("path", ["../secret", "media/../../secret", "/../../secret", "bad\\name", "bad\x00name"])
def test_key_for_rejects_unsafe_paths(path):
    with pytest.raises(ValueError):
        key_for(path)


def test_error_status_maps_missing_r2_object():
    exc = ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "HeadObject")
    assert error_status(exc) == SFTP_NO_SUCH_FILE


def test_error_status_uses_failure_for_unknown_error():
    assert error_status(RuntimeError("unknown")) == SFTP_FAILURE


def test_settings_can_be_loaded_from_environment(monkeypatch):
    monkeypatch.setenv("R2_ACCOUNT_ID", "example-account")
    monkeypatch.delenv("R2_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "example-access")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "example-secret")
    monkeypatch.setenv("R2_BUCKET_NAME", "example-bucket")
    monkeypatch.setenv("SFTP_USERNAME", "test-user")
    monkeypatch.setenv("SFTP_PASSWORD", "a-long-test-password-123")
    monkeypatch.setenv("PORT", "2223")

    cfg = Settings.load()
    assert cfg.endpoint_url == "https://example-account.r2.cloudflarestorage.com"
    assert cfg.bucket_name == "example-bucket"
    assert cfg.username == "test-user"
    assert cfg.port == 2223
