import base64

import pytest
from aiohttp.test_utils import TestClient, TestServer

from app import R2WebDAV, Settings, normalise_key, parse_byte_range


def test_normalise_key_blocks_traversal():
    assert normalise_key("/movies/a.mp4") == ("movies/a.mp4", False)
    assert normalise_key("/movies/") == ("movies", True)
    assert normalise_key("/") == ("", True)
    with pytest.raises(ValueError):
        normalise_key("/../secret.txt")
    with pytest.raises(ValueError):
        normalise_key("/foo\\bar")


def test_parse_byte_ranges():
    assert parse_byte_range("bytes=10-19", 100) == (10, 19)
    assert parse_byte_range("bytes=10-", 100) == (10, 99)
    assert parse_byte_range("bytes=-10", 100) == (90, 99)
    with pytest.raises(ValueError):
        parse_byte_range("bytes=100-", 100)
    with pytest.raises(ValueError):
        parse_byte_range("bytes=1-2,4-5", 100)


@pytest.mark.asyncio
async def test_health_and_authentication():
    settings = Settings(
        endpoint_url="https://example.r2.cloudflarestorage.com",
        access_key_id="test-key",
        secret_access_key="test-secret",
        bucket_name="test-bucket",
        username="tester",
        password="a-long-test-password",
    )
    client = TestClient(TestServer(R2WebDAV(settings).create_app()))
    await client.start_server()
    try:
        health = await client.get("/healthz")
        assert health.status == 200
        assert (await health.json())["status"] == "ok"

        unauthenticated = await client.options("/")
        assert unauthenticated.status == 401
        assert "WWW-Authenticate" in unauthenticated.headers

        auth = "Basic " + base64.b64encode(b"tester:a-long-test-password").decode()
        options = await client.options("/", headers={"Authorization": auth})
        assert options.status == 200
        assert options.headers["DAV"] == "1, 2"
        assert "PROPFIND" in options.headers["Allow"]
    finally:
        await client.close()
