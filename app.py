"""Authenticated WebDAV gateway for a Cloudflare R2 bucket.

Files are stored directly in R2. The container's temporary directory is only
used to spool uploads before the S3-compatible multipart uploader runs.
"""
from __future__ import annotations

import asyncio
import base64
import binascii
import hmac
import logging
import mimetypes
import os
import re
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import formatdate, parsedate_to_datetime
from typing import Any
from urllib.parse import quote, unquote, urlsplit
from xml.etree import ElementTree as ET

import boto3
from aiohttp import web
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from boto3.s3.transfer import TransferConfig
from defusedxml import ElementTree as SafeET

LOG = logging.getLogger("r2_to_webdav")
DAV_NS = "DAV:"
ET.register_namespace("D", DAV_NS)
CHUNK_SIZE = 1024 * 1024
NOT_FOUND_CODES = {"404", "NoSuchKey", "NoSuchBucket", "NotFound"}
ALLOW = "OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, PROPFIND, COPY, MOVE, LOCK, UNLOCK"


@dataclass(frozen=True)
class Settings:
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    bucket_name: str
    username: str
    password: str
    host: str = "0.0.0.0"
    port: int = 8080
    region: str = "auto"
    tmp_dir: str = "/tmp"
    max_upload_mb: int = 0  # 0 means unlimited; disk space remains the practical limit.
    max_propfind_items: int = 10000
    lock_timeout: int = 3600
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        account_id = os.getenv("R2_ACCOUNT_ID", "").strip()
        endpoint = os.getenv("R2_ENDPOINT_URL", "").strip()
        if not endpoint and account_id:
            endpoint = f"https://{account_id}.r2.cloudflarestorage.com"

        required = {
            "R2_ENDPOINT_URL or R2_ACCOUNT_ID": endpoint,
            "R2_ACCESS_KEY_ID": os.getenv("R2_ACCESS_KEY_ID", "").strip(),
            "R2_SECRET_ACCESS_KEY": os.getenv("R2_SECRET_ACCESS_KEY", "").strip(),
            "R2_BUCKET_NAME": os.getenv("R2_BUCKET_NAME", "").strip(),
            "WEBDAV_USERNAME": os.getenv("WEBDAV_USERNAME", ""),
            "WEBDAV_PASSWORD": os.getenv("WEBDAV_PASSWORD", ""),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise RuntimeError("Missing required environment variable(s): " + ", ".join(missing))
        password = required["WEBDAV_PASSWORD"]
        if len(password) < 12:
            raise RuntimeError("WEBDAV_PASSWORD must be at least 12 characters long")

        try:
            port = int(os.getenv("PORT", "8080"))
            max_upload_mb = int(os.getenv("MAX_UPLOAD_MB", "0"))
            max_items = int(os.getenv("MAX_PROPFIND_ITEMS", "10000"))
            lock_timeout = int(os.getenv("LOCK_TIMEOUT", "3600"))
        except ValueError as exc:
            raise RuntimeError("PORT, MAX_UPLOAD_MB, MAX_PROPFIND_ITEMS, and LOCK_TIMEOUT must be integers") from exc
        if not 1 <= port <= 65535:
            raise RuntimeError("PORT must be between 1 and 65535")
        if max_upload_mb < 0 or max_items < 1 or lock_timeout < 60:
            raise RuntimeError("Invalid MAX_UPLOAD_MB, MAX_PROPFIND_ITEMS, or LOCK_TIMEOUT setting")

        return cls(
            endpoint_url=endpoint,
            access_key_id=required["R2_ACCESS_KEY_ID"],
            secret_access_key=required["R2_SECRET_ACCESS_KEY"],
            bucket_name=required["R2_BUCKET_NAME"],
            username=required["WEBDAV_USERNAME"],
            password=password,
            host=os.getenv("HOST", "0.0.0.0"),
            port=port,
            region=os.getenv("R2_REGION", "auto").strip() or "auto",
            tmp_dir=os.getenv("TMP_DIR", "/tmp").strip() or "/tmp",
            max_upload_mb=max_upload_mb,
            max_propfind_items=max_items,
            lock_timeout=lock_timeout,
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )


def normalise_key(path: str) -> tuple[str, bool]:
    """Convert a URL path to an R2 key and remember whether it ends in '/'."""
    path = path or "/"
    if "\x00" in path or "\\" in path:
        raise ValueError("Invalid path")
    is_collection = path.endswith("/") or path in ("", "/")
    segments = path.split("/")
    if any(part in (".", "..") for part in segments):
        raise ValueError("Path traversal is not allowed")
    key = "/".join(part for part in segments if part)
    return key, is_collection


def parse_byte_range(header: str, total: int) -> tuple[int, int] | None:
    """Parse a single HTTP bytes range; return inclusive start/end or None."""
    if not header or not header.startswith("bytes=") or "," in header:
        raise ValueError("Unsupported or malformed byte range")
    spec = header[6:].strip()
    match = re.fullmatch(r"(\d*)-(\d*)", spec)
    if not match or (not match.group(1) and not match.group(2)) or total <= 0:
        raise ValueError("Invalid byte range")
    first, last = match.groups()
    if not first:
        suffix = int(last)
        if suffix <= 0:
            raise ValueError("Invalid suffix range")
        start, end = max(0, total - suffix), total - 1
    else:
        start = int(first)
        end = int(last) if last else total - 1
        if start >= total or end < start:
            raise ValueError("Range not satisfiable")
        end = min(end, total - 1)
    return start, end


class HTTP423Locked(web.HTTPClientError):
    """aiohttp does not define the WebDAV-specific 423 status itself."""
    status_code = 423
    reason = "Locked"


def _dav(tag: str) -> str:
    return f"{{{DAV_NS}}}{tag}"


def _xml_bytes(root: ET.Element) -> bytes:
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _http_date(value: datetime | None) -> str:
    if value is None:
        value = datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return formatdate(value.timestamp(), usegmt=True)


def _etag(value: Any) -> str:
    if not value:
        return '""'
    value = str(value)
    return value if value.startswith('"') else f'"{value}"'


def _is_not_found(exc: ClientError) -> bool:
    error = exc.response.get("Error", {})
    return str(error.get("Code", "")) in NOT_FOUND_CODES or exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404


class R2WebDAV:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.s3 = boto3.client(
            "s3",
            endpoint_url=settings.endpoint_url,
            aws_access_key_id=settings.access_key_id,
            aws_secret_access_key=settings.secret_access_key,
            region_name=settings.region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 5, "mode": "standard"},
                connect_timeout=10,
                read_timeout=120,
            ),
        )
        self.transfer_config = TransferConfig(
            multipart_threshold=64 * 1024 * 1024,
            multipart_chunksize=16 * 1024 * 1024,
            max_concurrency=4,
            use_threads=True,
        )
        self.locks: dict[str, dict[str, Any]] = {}
        os.makedirs(settings.tmp_dir, exist_ok=True)

    async def io(self, function, *args, **kwargs):
        return await asyncio.to_thread(function, *args, **kwargs)

    def key_from_request(self, request: web.Request) -> tuple[str, bool]:
        # aiohttp returns the decoded, routed path; decode exactly once.
        path = request.path or "/"
        try:
            return normalise_key(path)
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc

    async def head_object(self, key: str) -> dict[str, Any] | None:
        try:
            return await self.io(self.s3.head_object, Bucket=self.settings.bucket_name, Key=key)
        except ClientError as exc:
            if _is_not_found(exc):
                return None
            raise

    async def path_exists(self, key: str, collection: bool) -> bool:
        if key == "":
            return True
        if not collection:
            return await self.head_object(key) is not None
        marker = await self.head_object(key.rstrip("/") + "/")
        if marker is not None:
            return True
        response = await self.io(
            self.s3.list_objects_v2,
            Bucket=self.settings.bucket_name,
            Prefix=key.rstrip("/") + "/",
            MaxKeys=1,
        )
        return bool(response.get("Contents") or response.get("CommonPrefixes"))

    async def directory_children(self, prefix: str) -> list[dict[str, Any]]:
        prefix = prefix.rstrip("/")
        prefix = f"{prefix}/" if prefix else ""
        children: dict[str, dict[str, Any]] = {}
        paginator = self.s3.get_paginator("list_objects_v2")
        # Delimiter makes the response a single directory level, as WebDAV Depth: 1 requires.
        for page in paginator.paginate(Bucket=self.settings.bucket_name, Prefix=prefix, Delimiter="/"):
            for item in page.get("Contents", []):
                child_key = item.get("Key", "")
                if child_key == prefix or not child_key:
                    continue
                relative = child_key[len(prefix):]
                if not relative:
                    continue
                # A key ending in slash is a directory marker, not an ordinary file.
                is_dir = child_key.endswith("/")
                name = relative.rstrip("/")
                if not name or "/" in name:
                    continue
                children[name] = {"key": child_key.rstrip("/"), "is_dir": is_dir, "meta": item}
            for item in page.get("CommonPrefixes", []):
                child_key = item.get("Prefix", "").rstrip("/")
                name = child_key[len(prefix):].rstrip("/")
                if name:
                    children[name] = {"key": child_key, "is_dir": True, "meta": {}}
            if len(children) > self.settings.max_propfind_items:
                raise web.HTTPInsufficientStorage(text="Directory has too many entries for one PROPFIND response")
        return [children[name] for name in sorted(children, key=str.casefold)]

    async def delete_tree(self, key: str) -> None:
        prefix = key.rstrip("/") + "/"
        paginator = self.s3.get_paginator("list_objects_v2")
        batch: list[dict[str, str]] = []
        for page in paginator.paginate(Bucket=self.settings.bucket_name, Prefix=prefix):
            for item in page.get("Contents", []):
                batch.append({"Key": item["Key"]})
                if len(batch) == 1000:
                    await self.io(self.s3.delete_objects, Bucket=self.settings.bucket_name, Delete={"Objects": batch, "Quiet": True})
                    batch = []
        if batch:
            await self.io(self.s3.delete_objects, Bucket=self.settings.bucket_name, Delete={"Objects": batch, "Quiet": True})
        # The exact key without a slash may be a separate file (for example `foo`
        # can coexist with the collection `foo/`), so don't remove it here.

    def provided_lock_tokens(self, request: web.Request) -> set[str]:
        # WebDAV If headers put lock tokens inside angle brackets.
        return set(re.findall(r"<((?:opaquelocktoken|urn:uuid):[^>]+)>", request.headers.get("If", ""), flags=re.I))

    def active_locks(self) -> list[tuple[str, dict[str, Any]]]:
        now = time.time()
        expired = [key for key, lock in self.locks.items() if lock["expires"] <= now]
        for key in expired:
            self.locks.pop(key, None)
        return list(self.locks.items())

    def lock_applies(self, lock_key: str, lock: dict[str, Any], key: str) -> bool:
        if lock_key == key:
            return True
        return bool(lock["depth"] == "infinity" and lock_key and key.startswith(lock_key.rstrip("/") + "/"))

    def check_write_lock(self, request: web.Request, key: str) -> None:
        tokens = self.provided_lock_tokens(request)
        for lock_key, lock in self.active_locks():
            if self.lock_applies(lock_key, lock, key) and lock["token"] not in tokens:
                raise HTTP423Locked(text="The resource is locked; provide its WebDAV lock token in the If header")

    def href(self, key: str, is_dir: bool) -> str:
        quoted = quote(key, safe="/!$&'()*+,;=:@-._~")
        href = "/" + quoted
        if is_dir and not href.endswith("/"):
            href += "/"
        return href

    async def prop_response(self, key: str, is_dir: bool, meta: dict[str, Any] | None = None) -> ET.Element:
        meta = meta or {}
        response = ET.Element(_dav("response"))
        ET.SubElement(response, _dav("href")).text = self.href(key, is_dir)
        propstat = ET.SubElement(response, _dav("propstat"))
        prop = ET.SubElement(propstat, _dav("prop"))
        name = key.rstrip("/").split("/")[-1] if key else ""
        ET.SubElement(prop, _dav("displayname")).text = name
        resource_type = ET.SubElement(prop, _dav("resourcetype"))
        if is_dir:
            ET.SubElement(resource_type, _dav("collection"))
        size = 0 if is_dir else int(meta.get("Size", meta.get("ContentLength", 0)))
        ET.SubElement(prop, _dav("getcontentlength")).text = str(size)
        mime = "httpd/unix-directory" if is_dir else (meta.get("ContentType") or mimetypes.guess_type(name)[0] or "application/octet-stream")
        ET.SubElement(prop, _dav("getcontenttype")).text = mime
        modified = meta.get("LastModified")
        ET.SubElement(prop, _dav("getlastmodified")).text = _http_date(modified)
        if meta.get("ETag"):
            ET.SubElement(prop, _dav("getetag")).text = _etag(meta["ETag"])
        # Advertising DAV class 2 is backed by the LOCK/UNLOCK handlers below.
        supported = ET.SubElement(prop, _dav("supportedlock"))
        entry = ET.SubElement(supported, _dav("lockentry"))
        scope = ET.SubElement(entry, _dav("lockscope"))
        ET.SubElement(scope, _dav("exclusive"))
        locktype = ET.SubElement(entry, _dav("locktype"))
        ET.SubElement(locktype, _dav("write"))
        ET.SubElement(propstat, _dav("status")).text = "HTTP/1.1 200 OK"
        return response

    async def health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "service": "r2-to-webdav"})

    async def options(self, request: web.Request) -> web.Response:
        return web.Response(status=200, headers={"Allow": ALLOW, "DAV": "1, 2", "MS-Author-Via": "DAV", "Accept-Ranges": "bytes"})

    async def propfind(self, request: web.Request) -> web.Response:
        key, is_collection = self.key_from_request(request)
        if key == "":
            is_collection = True
        if not await self.path_exists(key, is_collection):
            raise web.HTTPNotFound(text="Resource not found")
        depth = request.headers.get("Depth", "1").strip().lower()
        if depth == "infinity":
            raise web.HTTPForbidden(text="Depth: infinity is disabled to protect large buckets")
        if depth not in ("0", "1"):
            raise web.HTTPBadRequest(text="Depth must be 0, 1, or infinity")

        response_root = ET.Element(_dav("multistatus"))
        if is_collection:
            marker_key = key.rstrip("/") + "/" if key else ""
            marker = await self.head_object(marker_key) if marker_key else None
            await self.prop_response_for_append(response_root, key, True, marker or {})
            if depth == "1":
                for child in await self.directory_children(key):
                    meta = child.get("meta") or {}
                    if child["is_dir"] and not meta:
                        marker = await self.head_object(child["key"] + "/")
                        meta = marker or {}
                    await self.prop_response_for_append(response_root, child["key"], child["is_dir"], meta)
        else:
            meta = await self.head_object(key)
            if meta is None:
                raise web.HTTPNotFound(text="Resource not found")
            await self.prop_response_for_append(response_root, key, False, meta)
        return web.Response(body=_xml_bytes(response_root), status=207, content_type="application/xml", charset="utf-8")

    async def prop_response_for_append(self, root: ET.Element, key: str, is_dir: bool, meta: dict[str, Any]) -> None:
        item = await self.prop_response(key, is_dir, meta)
        root.append(item)

    async def get_object(self, request: web.Request) -> web.StreamResponse:
        key, is_collection = self.key_from_request(request)
        if key == "" or is_collection:
            raise web.HTTPMethodNotAllowed(request.method, ["OPTIONS", "PROPFIND", "MKCOL", "PUT", "DELETE"])
        try:
            meta = await self.io(self.s3.head_object, Bucket=self.settings.bucket_name, Key=key)
        except ClientError as exc:
            if _is_not_found(exc):
                raise web.HTTPNotFound(text="File not found") from exc
            raise

        etag = _etag(meta.get("ETag"))
        if_none_match = request.headers.get("If-None-Match")
        if if_none_match and (if_none_match.strip() == "*" or etag in [item.strip() for item in if_none_match.split(",")]):
            return web.Response(status=304, headers={"ETag": etag, "Last-Modified": _http_date(meta.get("LastModified"))})
        if not if_none_match and request.headers.get("If-Modified-Since") and meta.get("LastModified"):
            try:
                since = parsedate_to_datetime(request.headers["If-Modified-Since"])
                modified = meta["LastModified"].replace(microsecond=0)
                if since.tzinfo is None:
                    since = since.replace(tzinfo=timezone.utc)
                if modified <= since:
                    return web.Response(status=304, headers={"ETag": etag, "Last-Modified": _http_date(meta.get("LastModified"))})
            except (TypeError, ValueError, OverflowError):
                pass

        total = int(meta.get("ContentLength", 0))
        status = 200
        range_header = request.headers.get("Range")
        if range_header and request.headers.get("If-Range") not in (etag, _http_date(meta.get("LastModified"))):
            if request.headers.get("If-Range"):
                range_header = None
        byte_range = None
        if range_header:
            try:
                byte_range = parse_byte_range(range_header, total)
            except ValueError as exc:
                raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{total}"}, text="Invalid byte range") from exc
            status = 206

        headers = {
            "Content-Type": meta.get("ContentType") or mimetypes.guess_type(key)[0] or "application/octet-stream",
            "Content-Length": str((byte_range[1] - byte_range[0] + 1) if byte_range else total),
            "Last-Modified": _http_date(meta.get("LastModified")),
            "ETag": etag,
            "Accept-Ranges": "bytes",
        }
        if byte_range:
            headers["Content-Range"] = f"bytes {byte_range[0]}-{byte_range[1]}/{total}"
        if request.method == "HEAD":
            return web.Response(status=status, headers=headers)

        args: dict[str, Any] = {"Bucket": self.settings.bucket_name, "Key": key}
        if byte_range:
            args["Range"] = f"bytes={byte_range[0]}-{byte_range[1]}"
        obj = await self.io(self.s3.get_object, **args)
        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)
        body = obj["Body"]
        try:
            while True:
                chunk = await self.io(body.read, CHUNK_SIZE)
                if not chunk:
                    break
                await response.write(chunk)
            await response.write_eof()
        except (ConnectionResetError, asyncio.CancelledError):
            LOG.debug("Client closed download connection for %s", key)
            raise
        finally:
            await self.io(body.close)
        return response

    async def put_object(self, request: web.Request) -> web.Response:
        key, is_collection = self.key_from_request(request)
        if not key:
            raise web.HTTPForbidden(text="Cannot replace the bucket root")
        if is_collection:
            raise web.HTTPConflict(text="PUT target must not end with a slash")
        self.check_write_lock(request, key)
        if request.headers.get("If-None-Match") == "*" and await self.path_exists(key, False):
            raise web.HTTPPreconditionFailed(text="Resource already exists")
        if_match = request.headers.get("If-Match")
        if if_match:
            old = await self.head_object(key)
            if old is None or _etag(old.get("ETag")) != if_match:
                raise web.HTTPPreconditionFailed(text="ETag precondition failed")

        limit = self.settings.max_upload_mb * 1024 * 1024
        content_length = request.content_length
        if limit and content_length is not None and content_length > limit:
            raise web.HTTPRequestEntityTooLarge(max_size=limit, actual_size=content_length)
        old_meta = await self.head_object(key)
        if old_meta is None and await self.path_exists(key, True):
            raise web.HTTPConflict(text="A collection already exists at this path")
        content_type = request.headers.get("Content-Type") or mimetypes.guess_type(key)[0] or "application/octet-stream"
        temp_path = None
        size = 0
        try:
            with tempfile.NamedTemporaryFile(prefix="r2-webdav-", dir=self.settings.tmp_dir, delete=False) as tmp:
                temp_path = tmp.name
                async for chunk in request.content.iter_chunked(CHUNK_SIZE):
                    size += len(chunk)
                    if limit and size > limit:
                        raise web.HTTPRequestEntityTooLarge(max_size=limit, actual_size=size)
                    await asyncio.to_thread(tmp.write, chunk)
                await asyncio.to_thread(tmp.flush)
            await self.io(
                self.s3.upload_file,
                temp_path,
                self.settings.bucket_name,
                key,
                ExtraArgs={"ContentType": content_type},
                Config=self.transfer_config,
            )
        finally:
            if temp_path:
                try:
                    os.unlink(temp_path)
                except FileNotFoundError:
                    pass
        uploaded = await self.head_object(key)
        headers = {}
        if uploaded and uploaded.get("ETag"):
            headers["ETag"] = _etag(uploaded["ETag"])
        return web.Response(status=204 if old_meta else 201, headers=headers)

    async def mkcol(self, request: web.Request) -> web.Response:
        key, _ = self.key_from_request(request)
        if not key:
            raise web.HTTPMethodNotAllowed(request.method, ["OPTIONS", "PROPFIND"])
        key = key.rstrip("/")
        self.check_write_lock(request, key)
        if await self.path_exists(key, False) or await self.path_exists(key, True):
            raise web.HTTPMethodNotAllowed(request.method, ["OPTIONS", "PROPFIND", "GET", "PUT", "DELETE"])
        if request.can_read_body:
            body = await request.content.read(1)
            if body:
                raise web.HTTPUnsupportedMediaType(text="MKCOL request bodies are not supported")
        await self.io(self.s3.put_object, Bucket=self.settings.bucket_name, Key=key + "/", Body=b"", ContentType="application/octet-stream")
        return web.Response(status=201)

    async def delete(self, request: web.Request) -> web.Response:
        key, is_collection = self.key_from_request(request)
        if not key:
            raise web.HTTPForbidden(text="Deleting the entire bucket root is disabled")
        self.check_write_lock(request, key)
        if is_collection:
            if not await self.path_exists(key, True):
                raise web.HTTPNotFound(text="Collection not found")
            await self.delete_tree(key)
        else:
            meta = await self.head_object(key)
            if meta is not None:
                await self.io(self.s3.delete_object, Bucket=self.settings.bucket_name, Key=key)
            elif await self.path_exists(key, True):
                await self.delete_tree(key)
            else:
                raise web.HTTPNotFound(text="Resource not found")
        return web.Response(status=204)

    async def destination(self, request: web.Request) -> tuple[str, bool]:
        raw = request.headers.get("Destination")
        if not raw:
            raise web.HTTPBadRequest(text="Destination header is required")
        parsed = urlsplit(raw)
        if parsed.netloc and parsed.hostname and parsed.hostname.lower() != request.host.split(":", 1)[0].lower():
            raise web.HTTPForbidden(text="Cross-host COPY/MOVE is not allowed")
        path = unquote(parsed.path or "/")
        try:
            return normalise_key(path)
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc

    async def copy(self, request: web.Request) -> web.Response:
        return await self.copy_or_move(request, False)

    async def move(self, request: web.Request) -> web.Response:
        return await self.copy_or_move(request, True)

    async def copy_or_move(self, request: web.Request, move: bool) -> web.Response:
        src_key, src_collection = self.key_from_request(request)
        dst_key, dst_collection = await self.destination(request)
        if not src_key or not dst_key:
            raise web.HTTPForbidden(text="COPY/MOVE on the bucket root is disabled")
        src_collection = src_collection or src_key.endswith("/")
        # Some clients omit the trailing slash on collection URLs. If no file exists
        # at that key but a prefix does, treat it as a collection.
        if not src_collection and await self.head_object(src_key) is None and await self.path_exists(src_key, True):
            src_collection = True
        dst_collection = dst_collection or dst_key.endswith("/") or src_collection
        if src_key == dst_key:
            raise web.HTTPForbidden(text="Source and destination are identical")
        if src_collection and (dst_key.startswith(src_key.rstrip("/") + "/") or src_key.startswith(dst_key.rstrip("/") + "/")):
            raise web.HTTPForbidden(text="Cannot copy/move a collection into itself or its parent")
        # Validate the source before any overwrite can remove destination data.
        source_meta = None
        if src_collection:
            if not await self.path_exists(src_key, True):
                raise web.HTTPNotFound(text="Source collection not found")
        else:
            source_meta = await self.head_object(src_key)
            if source_meta is None:
                raise web.HTTPNotFound(text="Source file not found")
        if move:
            self.check_write_lock(request, src_key)
        self.check_write_lock(request, dst_key)
        overwrite = request.headers.get("Overwrite", "T").upper() != "F"
        dst_exists = await self.path_exists(dst_key, dst_collection)
        if dst_collection and await self.head_object(dst_key) is not None:
            dst_exists = True
        if dst_exists and not overwrite:
            raise web.HTTPPreconditionFailed(text="Destination already exists and Overwrite is F")
        # An overwrite replaces the destination resource, including stale children.
        if dst_exists and overwrite:
            if dst_collection:
                await self.delete_tree(dst_key)
                # Remove a colliding file key as part of replacing this destination.
                await self.io(self.s3.delete_object, Bucket=self.settings.bucket_name, Key=dst_key)
            else:
                await self.io(self.s3.delete_object, Bucket=self.settings.bucket_name, Key=dst_key)

        if src_collection:
            src_prefix = src_key.rstrip("/") + "/"
            dst_prefix = dst_key.rstrip("/") + "/"
            paginator = self.s3.get_paginator("list_objects_v2")
            copied_any = False
            for page in paginator.paginate(Bucket=self.settings.bucket_name, Prefix=src_prefix):
                for item in page.get("Contents", []):
                    old_key = item["Key"]
                    relative = old_key[len(src_prefix):]
                    new_key = dst_prefix + relative
                    await self.io(self.s3.copy, {"Bucket": self.settings.bucket_name, "Key": old_key}, self.settings.bucket_name, new_key, Config=self.transfer_config)
                    copied_any = True
            if not copied_any:
                await self.io(self.s3.put_object, Bucket=self.settings.bucket_name, Key=dst_prefix, Body=b"", ContentType="application/octet-stream")
            if move:
                await self.delete_tree(src_key)
        else:
            await self.io(self.s3.copy, {"Bucket": self.settings.bucket_name, "Key": src_key}, self.settings.bucket_name, dst_key, Config=self.transfer_config)
            if move:
                await self.io(self.s3.delete_object, Bucket=self.settings.bucket_name, Key=src_key)
        return web.Response(status=204 if dst_exists else 201)

    def lock_xml(self, key: str, lock: dict[str, Any], is_dir: bool) -> bytes:
        root = ET.Element(_dav("prop"))
        discovery = ET.SubElement(root, _dav("lockdiscovery"))
        active = ET.SubElement(discovery, _dav("activelock"))
        scope = ET.SubElement(active, _dav("lockscope")); ET.SubElement(scope, _dav(lock["scope"]))
        locktype = ET.SubElement(active, _dav("locktype")); ET.SubElement(locktype, _dav("write"))
        depth = ET.SubElement(active, _dav("depth")); depth.text = lock["depth"]
        owner = ET.SubElement(active, _dav("owner")); owner.text = lock.get("owner", "")
        timeout = ET.SubElement(active, _dav("timeout")); timeout.text = f"Second-{max(0, int(lock['expires'] - time.time()))}"
        token = ET.SubElement(active, _dav("locktoken")); ET.SubElement(token, _dav("href")).text = lock["token"]
        root_href = ET.SubElement(active, _dav("lockroot")); ET.SubElement(root_href, _dav("href")).text = self.href(key, is_dir)
        return _xml_bytes(root)

    async def lock(self, request: web.Request) -> web.Response:
        key, is_collection = self.key_from_request(request)
        if not key:
            raise web.HTTPForbidden(text="Locking the bucket root is disabled")
        is_collection = is_collection or key.endswith("/")
        key = key.rstrip("/")
        raw = await request.read()
        timeout_header = request.headers.get("Timeout", "Second-3600")
        match = re.search(r"Second-(\d+)", timeout_header, flags=re.I)
        timeout = min(self.settings.lock_timeout, int(match.group(1))) if match else self.settings.lock_timeout
        timeout = max(60, timeout)

        if not raw:  # Refresh an existing lock.
            tokens = self.provided_lock_tokens(request)
            for lock_key, lock in self.active_locks():
                if lock_key == key and lock["token"] in tokens:
                    lock["expires"] = time.time() + timeout
                    return web.Response(status=200, body=self.lock_xml(key, lock, is_collection), content_type="application/xml", headers={"Lock-Token": f"<{lock['token']}>", "Timeout": f"Second-{timeout}", "DAV": "1, 2"})
            raise web.HTTPPreconditionFailed(text="No matching lock token was supplied")

        try:
            root = SafeET.fromstring(raw)
        except Exception as exc:
            raise web.HTTPBadRequest(text="Malformed LOCK XML") from exc
        scope = "exclusive"
        depth = request.headers.get("Depth", "infinity").lower()
        if depth not in ("0", "infinity"):
            raise web.HTTPBadRequest(text="LOCK Depth must be 0 or infinity")
        lockscope = root.find(f".//{_dav('lockscope')}")
        if lockscope is not None and lockscope.find(_dav("shared")) is not None:
            scope = "shared"
        if scope == "shared":
            # The implementation intentionally supports exclusive locks only.
            raise web.HTTPNotImplemented(text="Shared WebDAV locks are not supported")
        if not await self.path_exists(key, is_collection):
            raise web.HTTPNotFound(text="Lock target does not exist")
        self.check_write_lock(request, key)
        lock = {
            "token": f"opaquelocktoken:{uuid.uuid4()}",
            "expires": time.time() + timeout,
            "depth": depth,
            "scope": scope,
            "owner": "",
        }
        owner = root.find(f".//{_dav('owner')}")
        if owner is not None:
            lock["owner"] = " ".join(text.strip() for text in owner.itertext() if text.strip())[:512]
        self.locks[key] = lock
        return web.Response(status=200, body=self.lock_xml(key, lock, is_collection), content_type="application/xml", headers={"Lock-Token": f"<{lock['token']}>", "Timeout": f"Second-{timeout}", "DAV": "1, 2"})

    async def unlock(self, request: web.Request) -> web.Response:
        key, _ = self.key_from_request(request)
        key = key.rstrip("/")
        token_header = request.headers.get("Lock-Token", "").strip()
        token = token_header[1:-1] if token_header.startswith("<") and token_header.endswith(">") else token_header
        if not token:
            raise web.HTTPBadRequest(text="Lock-Token header is required")
        lock = self.locks.get(key)
        if not lock or not hmac.compare_digest(lock["token"], token):
            raise web.HTTPConflict(text="Lock token does not match")
        del self.locks[key]
        return web.Response(status=204)

    @web.middleware
    async def auth_middleware(self, request: web.Request, handler):
        if request.path == "/healthz":
            return await handler(request)
        header = request.headers.get("Authorization", "")
        username = password = None
        if header.startswith("Basic "):
            try:
                decoded = base64.b64decode(header[6:].strip(), validate=True).decode("utf-8")
                username, password = decoded.split(":", 1)
            except (binascii.Error, UnicodeDecodeError, ValueError):
                pass
        good_user = username is not None and hmac.compare_digest(username, self.settings.username)
        good_password = password is not None and hmac.compare_digest(password, self.settings.password)
        if not (good_user and good_password):
            return web.Response(status=401, text="Authentication required", headers={"WWW-Authenticate": 'Basic realm="R2 WebDAV", charset="UTF-8"'})
        try:
            return await handler(request)
        except web.HTTPException:
            raise
        except (ClientError, BotoCoreError) as exc:
            LOG.exception("R2 request failed")
            return web.Response(status=502, text="R2 storage request failed; check server logs")
        except Exception:
            LOG.exception("Unhandled WebDAV request error")
            return web.Response(status=500, text="Internal server error")

    def create_app(self) -> web.Application:
        app = web.Application(middlewares=[self.auth_middleware], client_max_size=0)
        app.router.add_get("/healthz", self.health)
        app.router.add_route("OPTIONS", "/", self.options)
        app.router.add_route("OPTIONS", "/{path:.*}", self.options)
        app.router.add_route("PROPFIND", "/", self.propfind)
        app.router.add_route("PROPFIND", "/{path:.*}", self.propfind)
        app.router.add_route("GET", "/", self.get_object)
        app.router.add_route("GET", "/{path:.*}", self.get_object)
        app.router.add_route("HEAD", "/", self.get_object)
        app.router.add_route("HEAD", "/{path:.*}", self.get_object)
        app.router.add_route("PUT", "/{path:.*}", self.put_object)
        app.router.add_route("MKCOL", "/{path:.*}", self.mkcol)
        app.router.add_route("DELETE", "/{path:.*}", self.delete)
        app.router.add_route("COPY", "/{path:.*}", self.copy)
        app.router.add_route("MOVE", "/{path:.*}", self.move)
        app.router.add_route("LOCK", "/{path:.*}", self.lock)
        app.router.add_route("UNLOCK", "/{path:.*}", self.unlock)
        return app


def main() -> None:
    settings = Settings.from_env()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    service = R2WebDAV(settings)
    LOG.info("Starting R2 WebDAV on %s:%s for bucket %s", settings.host, settings.port, settings.bucket_name)
    web.run_app(service.create_app(), host=settings.host, port=settings.port, access_log=LOG)


if __name__ == "__main__":
    main()
