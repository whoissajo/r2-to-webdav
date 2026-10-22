import base64
from pathlib import Path

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


class FakePaginator:
    def __init__(self, storage):
        self.storage = storage

    def paginate(self, Bucket, Prefix="", Delimiter=None):
        contents = []
        common = set()
        for key, value in sorted(self.storage.objects.items()):
            if not key.startswith(Prefix):
                continue
            relative = key[len(Prefix):]
            if Delimiter and "/" in relative.rstrip("/"):
                first = relative.split("/", 1)[0]
                common.add(Prefix + first + "/")
                continue
            contents.append({
                "Key": key,
                "Size": len(value["Body"]),
                "ETag": value["ETag"],
                "LastModified": value["LastModified"],
            })
        yield {"Contents": contents, "CommonPrefixes": [{"Prefix": p} for p in sorted(common)]}


class FakeS3:
    def __init__(self):
        from datetime import datetime, timezone
        self.objects = {}
        self._now = datetime.now(timezone.utc)

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return FakePaginator(self)

    def list_objects_v2(self, Bucket, Prefix="", MaxKeys=1000, Delimiter=None):
        page = next(self.get_paginator("list_objects_v2").paginate(Bucket=Bucket, Prefix=Prefix, Delimiter=Delimiter))
        return {
            "Contents": page.get("Contents", [])[:MaxKeys],
            "CommonPrefixes": page.get("CommonPrefixes", [])[:MaxKeys],
        }

    def head_object(self, Bucket, Key):
        from botocore.exceptions import ClientError
        value = self.objects.get(Key)
        if value is None:
            raise ClientError({"Error": {"Code": "NoSuchKey"}, "ResponseMetadata": {"HTTPStatusCode": 404}}, "HeadObject")
        return {
            "ContentLength": len(value["Body"]),
            "ETag": value["ETag"],
            "LastModified": value["LastModified"],
            "ContentType": value["ContentType"],
        }

    def put_object(self, Bucket, Key, Body=b"", ContentType="application/octet-stream"):
        import hashlib
        body = Body if isinstance(Body, bytes) else Body.read()
        self.objects[Key] = {
            "Body": body,
            "ETag": '"' + hashlib.md5(body).hexdigest() + '"',
            "LastModified": self._now,
            "ContentType": ContentType,
        }

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None, Config=None):
        self.put_object(Bucket, Key, Path(Filename).read_bytes(), (ExtraArgs or {}).get("ContentType", "application/octet-stream"))

    def get_object(self, Bucket, Key, Range=None):
        from io import BytesIO
        data = self.objects[Key]["Body"]
        if Range:
            value = Range.removeprefix("bytes=")
            start, end = value.split("-")
            data = data[int(start):int(end) + 1]
        return {"Body": BytesIO(data)}

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)

    def delete_objects(self, Bucket, Delete):
        for value in Delete["Objects"]:
            self.objects.pop(value["Key"], None)

    def copy(self, CopySource, Bucket, Key, Config=None):
        source = self.objects[CopySource["Key"]]
        self.objects[Key] = dict(source)


@pytest.mark.asyncio
async def test_put_get_range_propfind_and_mkcol_delete(tmp_path):
    from xml.etree import ElementTree as ET
    from pathlib import Path

    settings = Settings(
        endpoint_url="https://example.r2.cloudflarestorage.com",
        access_key_id="test-key",
        secret_access_key="test-secret",
        bucket_name="test-bucket",
        username="tester",
        password="a-long-test-password",
        tmp_dir=str(tmp_path),
    )
    service = R2WebDAV(settings)
    service.s3 = FakeS3()
    client = TestClient(TestServer(service.create_app()))
    await client.start_server()
    try:
        auth = {"Authorization": "Basic " + base64.b64encode(b"tester:a-long-test-password").decode()}
        uploaded = await client.put("/hello.txt", data=b"hello world", headers=auth)
        assert uploaded.status == 201

        head_root = await client.head("/", headers=auth)
        assert head_root.status == 200
        assert head_root.headers["DAV"] == "1, 2"
        assert "PROPFIND" in head_root.headers["Allow"]

        browser_root = await client.get("/", headers=auth)
        assert browser_root.status == 200
        assert "hello.txt" in await browser_root.text()

        partial = await client.get("/hello.txt", headers={**auth, "Range": "bytes=1-4"})
        assert partial.status == 206
        assert await partial.read() == b"ello"
        assert partial.headers["Content-Range"] == "bytes 1-4/11"

        listing = await client.request("PROPFIND", "/", headers={**auth, "Depth": "1"})
        assert listing.status == 207
        document = ET.fromstring(await listing.read())
        ns = {"d": "DAV:"}
        file_size = document.find(".//d:response[d:href='/hello.txt']/d:propstat/d:prop/d:getcontentlength", ns)
        assert file_size is not None and file_size.text == "11"

        created = await client.request("MKCOL", "/folder/", headers=auth)
        assert created.status == 201
        folder = await client.request("PROPFIND", "/folder/", headers={**auth, "Depth": "0"})
        assert folder.status == 207
        deleted = await client.delete("/folder/", headers=auth)
        assert deleted.status == 204
        assert "folder/" not in service.s3.objects
    finally:
        await client.close()
