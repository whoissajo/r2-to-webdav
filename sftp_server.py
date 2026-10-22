"""Standalone SFTP server backed by a Cloudflare R2 bucket.

Install:
    python -m pip install paramiko boto3 botocore

Edit the configuration constants below, or provide the matching environment
variables. Files are stored in R2; temporary local disk is used for uploads.
"""
from __future__ import annotations

import errno
import hmac
import logging
import mimetypes
import os
import posixpath
import socket
import stat
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import boto3
import paramiko
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from paramiko import (
    SFTPAttributes,
    SFTP_FAILURE,
    SFTP_NO_SUCH_FILE,
    SFTP_OK,
    SFTP_OP_UNSUPPORTED,
    SFTP_PERMISSION_DENIED,
)

LOG = logging.getLogger("r2_sftp")

# =====================================================================
# CONFIGURATION: paste your credentials here, or override via environment.
# Do not commit real credentials to a public repository.
# =====================================================================
R2_ACCOUNT_ID = "PASTE_CLOUDFLARE_ACCOUNT_ID_HERE"
R2_ENDPOINT_URL = ""  # Optional; when empty, ACCOUNT_ID builds the endpoint.
R2_ACCESS_KEY_ID = "PASTE_R2_ACCESS_KEY_ID_HERE"
R2_SECRET_ACCESS_KEY = "PASTE_R2_SECRET_ACCESS_KEY_HERE"
R2_BUCKET_NAME = "PASTE_R2_BUCKET_NAME_HERE"
R2_REGION = "auto"

SFTP_USERNAME = "sftp"
SFTP_PASSWORD = "CHANGE_THIS_TO_A_LONG_RANDOM_PASSWORD"

HOST = "0.0.0.0"
PORT = 2222
TMP_DIR = "/tmp/r2-sftp"
HOST_KEY_PATH = "/data/ssh_host_rsa_key"
MAX_UPLOAD_MB = 0              # 0 = unlimited; free temporary disk is still required.
MAX_DIRECTORY_ENTRIES = 10000
LOG_LEVEL = "INFO"


def setting(env_name: str, code_value: str) -> str:
    """Environment configuration wins; otherwise use the editable constant."""
    return os.getenv(env_name, code_value).strip()


@dataclass(frozen=True)
class Settings:
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    bucket_name: str
    username: str
    password: str
    host: str
    port: int
    region: str
    tmp_dir: str
    host_key_path: str
    max_upload_mb: int
    max_directory_entries: int
    log_level: str

    @classmethod
    def load(cls) -> "Settings":
        account_id = setting("R2_ACCOUNT_ID", R2_ACCOUNT_ID)
        endpoint = setting("R2_ENDPOINT_URL", R2_ENDPOINT_URL)
        if not endpoint and account_id and not account_id.startswith("PASTE_"):
            endpoint = f"https://{account_id}.r2.cloudflarestorage.com"

        values = {
            "R2_ENDPOINT_URL or R2_ACCOUNT_ID": endpoint,
            "R2_ACCESS_KEY_ID": setting("R2_ACCESS_KEY_ID", R2_ACCESS_KEY_ID),
            "R2_SECRET_ACCESS_KEY": setting("R2_SECRET_ACCESS_KEY", R2_SECRET_ACCESS_KEY),
            "R2_BUCKET_NAME": setting("R2_BUCKET_NAME", R2_BUCKET_NAME),
            "SFTP_USERNAME": setting("SFTP_USERNAME", SFTP_USERNAME),
            "SFTP_PASSWORD": setting("SFTP_PASSWORD", SFTP_PASSWORD),
        }
        missing = [
            key for key, value in values.items()
            if not value
            or value.startswith("PASTE_")
            or value.startswith("CHANGE_THIS")
        ]
        if missing:
            raise RuntimeError(
                "Configure these values at the top of sftp_server.py or through "
                "environment variables: " + ", ".join(missing)
            )

        try:
            port = int(setting("PORT", str(PORT)))
            max_upload_mb = int(setting("MAX_UPLOAD_MB", str(MAX_UPLOAD_MB)))
            max_entries = int(setting("MAX_DIRECTORY_ENTRIES", str(MAX_DIRECTORY_ENTRIES)))
        except ValueError as exc:
            raise RuntimeError("PORT, MAX_UPLOAD_MB, and MAX_DIRECTORY_ENTRIES must be integers") from exc

        if len(values["SFTP_PASSWORD"]) < 12:
            raise RuntimeError("SFTP_PASSWORD must be at least 12 characters long")
        if not 1 <= port <= 65535:
            raise RuntimeError("PORT must be between 1 and 65535")
        if max_upload_mb < 0 or max_entries < 1:
            raise RuntimeError("MAX_UPLOAD_MB or MAX_DIRECTORY_ENTRIES is invalid")

        return cls(
            endpoint_url=endpoint,
            access_key_id=values["R2_ACCESS_KEY_ID"],
            secret_access_key=values["R2_SECRET_ACCESS_KEY"],
            bucket_name=values["R2_BUCKET_NAME"],
            username=values["SFTP_USERNAME"],
            password=values["SFTP_PASSWORD"],
            host=setting("HOST", HOST),
            port=port,
            region=setting("R2_REGION", R2_REGION) or "auto",
            tmp_dir=setting("TMP_DIR", TMP_DIR) or "/tmp/r2-sftp",
            host_key_path=setting("HOST_KEY_PATH", HOST_KEY_PATH),
            max_upload_mb=max_upload_mb,
            max_directory_entries=max_entries,
            log_level=setting("LOG_LEVEL", LOG_LEVEL).upper(),
        )


def make_s3_client(cfg: Settings):
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint_url,
        aws_access_key_id=cfg.access_key_id,
        aws_secret_access_key=cfg.secret_access_key,
        region_name=cfg.region,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    )


def key_for(path: str) -> str:
    """Map an SFTP path to an R2 object key without permitting traversal."""
    if not isinstance(path, str) or "\x00" in path or "\\" in path:
        raise ValueError("Invalid SFTP path")
    if any(part == ".." for part in path.split("/")):
        raise ValueError("Parent path segments are not allowed")
    cleaned = posixpath.normpath("/" + path.lstrip("/"))
    if cleaned == "/":
        return ""
    return cleaned.lstrip("/")


def error_status(exc: BaseException) -> int:
    if isinstance(exc, (FileNotFoundError, KeyError)):
        return SFTP_NO_SUCH_FILE
    if isinstance(exc, PermissionError):
        return SFTP_PERMISSION_DENIED
    if isinstance(exc, (ValueError, OSError)) and getattr(exc, "errno", None) in (
        errno.EACCES, errno.EPERM,
    ):
        return SFTP_PERMISSION_DENIED
    if isinstance(exc, ClientError):
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound", "NoSuchBucket", "NoSuchVersion"}:
            return SFTP_NO_SUCH_FILE
        if code in {"403", "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"}:
            return SFTP_PERMISSION_DENIED
    LOG.debug("SFTP operation failed", exc_info=exc)
    return SFTP_FAILURE


def attr_for(*, name: Optional[str], is_dir: bool, size: int = 0, modified=None):
    attrs = SFTPAttributes()
    if name is not None:
        attrs.filename = name
    attrs.st_mode = (stat.S_IFDIR | 0o755) if is_dir else (stat.S_IFREG | 0o644)
    attrs.st_size = 0 if is_dir else int(size)
    if modified is None:
        attrs.st_mtime = int(time.time())
    elif isinstance(modified, datetime):
        attrs.st_mtime = int(modified.replace(tzinfo=modified.tzinfo or timezone.utc).timestamp())
    else:
        attrs.st_mtime = int(modified)
    attrs.st_atime = attrs.st_mtime
    return attrs


class R2ReadHandle(paramiko.SFTPHandle):
    """Read object contents in S3 byte ranges, so downloads do not spool locally."""

    def __init__(self, flags: int, s3, bucket: str, key: str, size: int, modified):
        super().__init__(flags)
        self.s3 = s3
        self.bucket = bucket
        self.key = key
        self.size = size
        self.modified = modified

    def read(self, offset: int, length: int):
        if offset >= self.size or length <= 0:
            return b""
        end = min(self.size - 1, offset + length - 1)
        body = None
        try:
            response = self.s3.get_object(
                Bucket=self.bucket,
                Key=self.key,
                Range=f"bytes={offset}-{end}",
            )
            body = response["Body"]
            return body.read()
        except Exception as exc:
            return error_status(exc)
        finally:
            if body is not None:
                body.close()

    def stat(self):
        return attr_for(name=None, is_dir=False, size=self.size, modified=self.modified)


class R2WriteHandle(paramiko.SFTPHandle):
    """Stage a writable file on disk and upload it to R2 when the client closes it."""

    def __init__(self, flags: int, s3, cfg: Settings, key: str, file_obj, existed: bool):
        super().__init__(flags)
        self.s3 = s3
        self.cfg = cfg
        self.key = key
        self.writefile = file_obj
        self.existed = existed
        self.failed = False
        self.closed = False
        self.lock = threading.Lock()

    def write(self, offset: int, data: bytes):
        with self.lock:
            if self.failed or self.closed:
                return SFTP_FAILURE
            try:
                if self.flags & os.O_APPEND:
                    self.writefile.seek(0, os.SEEK_END)
                else:
                    self.writefile.seek(offset)
                self.writefile.write(data)
                if self.cfg.max_upload_mb:
                    self.writefile.seek(0, os.SEEK_END)
                    size = self.writefile.tell()
                    if size > self.cfg.max_upload_mb * 1024 * 1024:
                        self.failed = True
                        LOG.warning("Rejected upload exceeding MAX_UPLOAD_MB for %s", self.key)
                        return SFTP_FAILURE
                self.writefile.flush()
                return SFTP_OK
            except Exception as exc:
                LOG.warning("Could not stage SFTP write for %s: %s", self.key, exc)
                self.failed = True
                return error_status(exc)

    def stat(self):
        try:
            self.writefile.flush()
            self.writefile.seek(0, os.SEEK_END)
            return attr_for(name=None, is_dir=False, size=self.writefile.tell())
        except Exception as exc:
            return error_status(exc)

    def close(self):
        with self.lock:
            if self.closed:
                return SFTP_OK if not self.failed else SFTP_FAILURE
            self.closed = True
            result = SFTP_OK
            try:
                if self.failed:
                    result = SFTP_FAILURE
                else:
                    self.writefile.flush()
                    self.writefile.seek(0)
                    content_type = mimetypes.guess_type(self.key)[0] or "application/octet-stream"
                    transfer = TransferConfig(
                        multipart_threshold=8 * 1024 * 1024,
                        multipart_chunksize=8 * 1024 * 1024,
                        max_concurrency=4,
                        use_threads=True,
                    )
                    self.s3.upload_fileobj(
                        self.writefile,
                        self.cfg.bucket_name,
                        self.key,
                        ExtraArgs={"ContentType": content_type},
                        Config=transfer,
                    )
                    LOG.info("Uploaded %s to R2", self.key)
            except Exception as exc:
                LOG.exception("Could not upload %s to R2", self.key)
                result = error_status(exc)
            finally:
                try:
                    self.writefile.close()
                except Exception:
                    pass
            return result


class PasswordAuth(paramiko.ServerInterface):
    def __init__(self, cfg: Settings):
        self.cfg = cfg

    def get_allowed_auths(self, username):
        return "password"

    def check_auth_password(self, username, password):
        valid_user = hmac.compare_digest(username or "", self.cfg.username)
        valid_password = hmac.compare_digest(password or "", self.cfg.password)
        return paramiko.AUTH_SUCCESSFUL if valid_user and valid_password else paramiko.AUTH_FAILED

    def check_channel_request(self, kind, chanid):
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED


class R2SFTPInterface(paramiko.SFTPServerInterface):
    def __init__(self, server, cfg: Settings, s3):
        super().__init__(server)
        self.cfg = cfg
        self.s3 = s3
        self.bucket = cfg.bucket_name

    def canonicalize(self, path):
        try:
            key = key_for(path)
            return "/" + key if key else "/"
        except Exception:
            return "/"

    def _head(self, key: str):
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    def _directory_exists(self, key: str) -> bool:
        if not key:
            return True
        marker = self._head(key.rstrip("/") + "/")
        if marker is not None:
            return True
        response = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix=key.rstrip("/") + "/", MaxKeys=1
        )
        return bool(response.get("KeyCount", 0))

    def _path_attr(self, key: str, name: Optional[str] = None):
        if not key:
            return attr_for(name=name, is_dir=True)
        obj = self._head(key)
        if obj is not None:
            return attr_for(
                name=name,
                is_dir=key.endswith("/"),
                size=obj.get("ContentLength", 0),
                modified=obj.get("LastModified"),
            )
        marker = self._head(key.rstrip("/") + "/")
        if marker is not None:
            return attr_for(name=name, is_dir=True, modified=marker.get("LastModified"))
        response = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix=key.rstrip("/") + "/", MaxKeys=1
        )
        if response.get("KeyCount", 0):
            return attr_for(name=name, is_dir=True)
        return None

    def stat(self, path):
        try:
            key = key_for(path)
            attrs = self._path_attr(key)
            return attrs if attrs is not None else SFTP_NO_SUCH_FILE
        except Exception as exc:
            return error_status(exc)

    def lstat(self, path):
        return self.stat(path)

    def list_folder(self, path):
        try:
            key = key_for(path)
            attrs = self._path_attr(key)
            if attrs is None:
                return SFTP_NO_SUCH_FILE
            if not stat.S_ISDIR(attrs.st_mode):
                return SFTP_FAILURE

            prefix = key.rstrip("/") + "/" if key else ""
            output = {}
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(
                Bucket=self.bucket, Prefix=prefix, Delimiter="/",
                PaginationConfig={"PageSize": 1000},
            ):
                for item in page.get("CommonPrefixes", []):
                    child_key = item["Prefix"].rstrip("/")
                    name = child_key.rsplit("/", 1)[-1]
                    output[name] = attr_for(name=name, is_dir=True)
                for item in page.get("Contents", []):
                    obj_key = item["Key"]
                    if obj_key == prefix:
                        continue
                    relative = obj_key[len(prefix):]
                    if not relative or "/" in relative:
                        continue
                    name = relative
                    output[name] = attr_for(
                        name=name,
                        is_dir=obj_key.endswith("/"),
                        size=item.get("Size", 0),
                        modified=item.get("LastModified"),
                    )
                if len(output) > self.cfg.max_directory_entries:
                    LOG.warning("Directory listing limit reached for %s", path)
                    return SFTP_FAILURE
            return list(output.values())
        except Exception as exc:
            return error_status(exc)

    def open(self, path, flags, attr):
        file_obj = None
        try:
            key = key_for(path)
            if not key:
                return SFTP_PERMISSION_DENIED
            if path.endswith("/"):
                return SFTP_FAILURE

            existing = self._head(key)
            is_dir = self._directory_exists(key) if existing is None else False
            if is_dir:
                return SFTP_FAILURE

            write_requested = (flags & os.O_WRONLY) or (flags & os.O_RDWR)
            if not write_requested:
                if existing is None:
                    return SFTP_NO_SUCH_FILE
                return R2ReadHandle(
                    flags, self.s3, self.bucket, key,
                    int(existing.get("ContentLength", 0)),
                    existing.get("LastModified"),
                )

            if (flags & os.O_EXCL) and (flags & os.O_CREAT) and existing is not None:
                return SFTP_FAILURE
            if existing is None and not (flags & os.O_CREAT):
                return SFTP_NO_SUCH_FILE

            os.makedirs(self.cfg.tmp_dir, exist_ok=True)
            file_obj = tempfile.TemporaryFile(mode="w+b", dir=self.cfg.tmp_dir)

            # Preserve old bytes for non-truncating random-access writes.
            if existing is not None and not (flags & os.O_TRUNC):
                self.s3.download_fileobj(self.bucket, key, file_obj)
                file_obj.seek(0, os.SEEK_END)
            elif flags & os.O_APPEND:
                file_obj.seek(0, os.SEEK_END)
            else:
                file_obj.seek(0)

            return R2WriteHandle(flags, self.s3, self.cfg, key, file_obj, existing is not None)
        except Exception as exc:
            if file_obj is not None:
                try:
                    file_obj.close()
                except Exception:
                    pass
            return error_status(exc)

    def remove(self, path):
        try:
            key = key_for(path)
            if not key:
                return SFTP_PERMISSION_DENIED
            attrs = self._path_attr(key)
            if attrs is None:
                return SFTP_NO_SUCH_FILE
            if stat.S_ISDIR(attrs.st_mode):
                return SFTP_FAILURE
            self.s3.delete_object(Bucket=self.bucket, Key=key)
            return SFTP_OK
        except Exception as exc:
            return error_status(exc)

    def mkdir(self, path, attr):
        try:
            key = key_for(path)
            if not key:
                return SFTP_FAILURE
            if self._path_attr(key) is not None:
                return SFTP_FAILURE
            parent = posixpath.dirname(key)
            if parent and not self._directory_exists(parent):
                return SFTP_NO_SUCH_FILE
            self.s3.put_object(
                Bucket=self.bucket, Key=key.rstrip("/") + "/", Body=b"",
                ContentType="application/x-directory",
            )
            return SFTP_OK
        except Exception as exc:
            return error_status(exc)

    def rmdir(self, path):
        try:
            key = key_for(path)
            if not key:
                return SFTP_PERMISSION_DENIED
            if not self._directory_exists(key):
                return SFTP_NO_SUCH_FILE
            prefix = key.rstrip("/") + "/"
            page = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=2)
            keys = [obj["Key"] for obj in page.get("Contents", []) if obj["Key"] != prefix]
            if keys or page.get("IsTruncated"):
                return SFTP_FAILURE
            marker = self._head(prefix)
            if marker is not None:
                self.s3.delete_object(Bucket=self.bucket, Key=prefix)
            return SFTP_OK
        except Exception as exc:
            return error_status(exc)

    def rename(self, oldpath, newpath):
        return self._move(oldpath, newpath, overwrite=False)

    def posix_rename(self, oldpath, newpath):
        return self._move(oldpath, newpath, overwrite=True)

    def _move(self, oldpath, newpath, overwrite: bool):
        try:
            src = key_for(oldpath)
            dst = key_for(newpath)
            if not src or not dst:
                return SFTP_PERMISSION_DENIED
            if src == dst:
                return SFTP_OK
            if dst.startswith(src.rstrip("/") + "/"):
                return SFTP_FAILURE

            src_attr = self._path_attr(src)
            if src_attr is None:
                return SFTP_NO_SUCH_FILE
            dst_attr = self._path_attr(dst)
            if dst_attr is not None:
                if not overwrite:
                    return SFTP_FAILURE
                if stat.S_ISDIR(dst_attr.st_mode):
                    # Do not replace a non-empty destination directory.
                    if not self.rmdir("/" + dst):
                        return SFTP_FAILURE
                else:
                    self.s3.delete_object(Bucket=self.bucket, Key=dst)

            parent = posixpath.dirname(dst)
            if parent and not self._directory_exists(parent):
                return SFTP_NO_SUCH_FILE

            if stat.S_ISDIR(src_attr.st_mode):
                source_prefix = src.rstrip("/") + "/"
                destination_prefix = dst.rstrip("/") + "/"
                paginator = self.s3.get_paginator("list_objects_v2")
                old_keys = []
                for page in paginator.paginate(Bucket=self.bucket, Prefix=source_prefix):
                    for obj in page.get("Contents", []):
                        old_key = obj["Key"]
                        suffix = old_key[len(source_prefix):]
                        new_key = destination_prefix + suffix
                        self.s3.copy_object(
                            Bucket=self.bucket,
                            Key=new_key,
                            CopySource={"Bucket": self.bucket, "Key": old_key},
                        )
                        old_keys.append(old_key)
                for start in range(0, len(old_keys), 1000):
                    batch = old_keys[start:start + 1000]
                    if batch:
                        self.s3.delete_objects(
                            Bucket=self.bucket,
                            Delete={"Objects": [{"Key": item} for item in batch], "Quiet": True},
                        )
            else:
                self.s3.copy_object(
                    Bucket=self.bucket,
                    Key=dst,
                    CopySource={"Bucket": self.bucket, "Key": src},
                )
                self.s3.delete_object(Bucket=self.bucket, Key=src)
            return SFTP_OK
        except Exception as exc:
            LOG.exception("SFTP rename/move failed")
            return error_status(exc)

    def chattr(self, path, attr):
        # R2 objects do not have POSIX permissions/owners. Accept chmod/utime
        # requests as no-ops so basic file managers do not fail on them.
        try:
            if self._path_attr(key_for(path)) is None:
                return SFTP_NO_SUCH_FILE
            return SFTP_OK
        except Exception as exc:
            return error_status(exc)

    def readlink(self, path):
        return SFTP_OP_UNSUPPORTED

    def symlink(self, target_path, path):
        return SFTP_OP_UNSUPPORTED


def load_or_create_host_key(path: str):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    if os.path.isfile(path):
        return paramiko.RSAKey.from_private_key_file(path)
    key = paramiko.RSAKey.generate(3072)
    key.write_private_key_file(path)
    os.chmod(path, 0o600)
    LOG.warning("Generated a new SSH host key at %s; persist this file to keep client fingerprints stable", path)
    return key


def serve_client(client_socket, client_address, cfg: Settings, host_key, s3):
    transport = None
    try:
        transport = paramiko.Transport(client_socket)
        transport.add_server_key(host_key)
        transport.set_subsystem_handler(
            "sftp", paramiko.SFTPServer, R2SFTPInterface, cfg, s3
        )
        transport.start_server(server=PasswordAuth(cfg))
        while transport.is_active():
            time.sleep(0.5)
    except (EOFError, OSError, paramiko.SSHException) as exc:
        LOG.info("SFTP client %s disconnected: %s", client_address, exc)
    except Exception:
        LOG.exception("Unexpected error for SFTP client %s", client_address)
    finally:
        if transport is not None:
            transport.close()
        else:
            try:
                client_socket.close()
            except Exception:
                pass


def main():
    cfg = Settings.load()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    os.makedirs(cfg.tmp_dir, exist_ok=True)
    host_key = load_or_create_host_key(cfg.host_key_path)
    s3 = make_s3_client(cfg)

    # Fail early rather than accepting logins that cannot reach the bucket.
    s3.head_bucket(Bucket=cfg.bucket_name)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((cfg.host, cfg.port))
        server.listen(128)
        LOG.info("SFTP listening on %s:%s; R2 bucket=%s", cfg.host, cfg.port, cfg.bucket_name)
        try:
            while True:
                client_socket, client_address = server.accept()
                client_socket.settimeout(None)
                thread = threading.Thread(
                    target=serve_client,
                    args=(client_socket, client_address, cfg, host_key, s3),
                    daemon=True,
                )
                thread.start()
        except KeyboardInterrupt:
            LOG.info("SFTP server shutting down")


if __name__ == "__main__":
    main()
