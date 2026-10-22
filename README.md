# R2 to WebDAV

A Docker-ready Python WebDAV server that exposes a Cloudflare R2 bucket as a normal WebDAV drive. Objects are stored in R2; upload staging uses temporary disk only and does not create a local mirror.

## Features

- Basic authentication with a configurable username and password.
- Cloudflare R2 using its S3-compatible API and boto3.
- WebDAV `OPTIONS`, `PROPFIND` (Depth 0/1), `GET`, `HEAD`, `PUT`, `DELETE`, `MKCOL`, `COPY`, `MOVE`, `LOCK`, and `UNLOCK`.
- Streaming downloads and HTTP byte ranges for seeking in video players.
- Uploads are spooled to temporary disk, then uploaded with boto3's multipart transfer support; large uploads are not buffered entirely in RAM.
- Directory listing and recursive directory deletion/copy/move.
- Health endpoint at `/healthz` (no authentication); all storage routes require Basic authentication.
- Non-root Docker user and health check.

## 1. Create R2 credentials

In Cloudflare, create an R2 API token with **Object Read & Write** permissions for the bucket you want to expose. Copy the Access Key ID and Secret Access Key. Find the Cloudflare account ID and the target bucket name.

## 2. Configure

```bash
cp .env.example .env
```

Edit `.env` and set `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME`, `WEBDAV_USERNAME`, and `WEBDAV_PASSWORD`. The password must be at least 12 characters. Keep `.env` private; never commit real credentials. You may use `R2_ENDPOINT_URL` instead of `R2_ACCOUNT_ID` if needed.

## 3. Run with Docker Compose

```bash
docker compose up -d --build
docker compose logs -f
```

The WebDAV endpoint is `http://YOUR_SERVER:8080/`. The health endpoint is `http://YOUR_SERVER:8080/healthz`.

For a quick local check:

```bash
curl -i -u 'webdav:your-long-password' -X PROPFIND -H 'Depth: 1' http://127.0.0.1:8080/
curl -i -u 'webdav:your-long-password' -T ./example.txt http://127.0.0.1:8080/example.txt
curl -i -u 'webdav:your-long-password' http://127.0.0.1:8080/example.txt
```

Use the credentials configured in `.env`, not the example values above.

## 4. Connect a client

- **WebDAV URL:** `https://your-domain.example/` (recommended when behind a TLS reverse proxy)
- **Username:** `WEBDAV_USERNAME`
- **Password:** `WEBDAV_PASSWORD`

On Android, add a WebDAV/WebDAVS connection in your file manager or media app. Use HTTPS in production. If you terminate TLS at a reverse proxy, forward WebDAV methods and headers (`Destination`, `Depth`, `Overwrite`, `If`, `Lock-Token`, `Timeout`, `Range`, and `Authorization`) and allow large request bodies/long uploads.

## SFTP server backed by the same R2 bucket

The SFTP service is separate from WebDAV: it uses SSH/SFTP on TCP port 2222 and can be deployed independently. It reads and writes the configured R2 bucket directly. Reads are fetched using byte ranges; uploads are staged on temporary disk and sent to R2 when the client closes the file.

### Docker Compose

1. Copy the SFTP environment template:

   ```bash
   cp .env.sftp.example .env.sftp
   ```

2. Edit .env.sftp with the R2 credentials, a username, and a strong password.
3. Start only the SFTP service:

   ```bash
   docker compose -f docker-compose.sftp.yml up -d --build
   docker compose -f docker-compose.sftp.yml logs -f
   ```

4. Connect your SFTP client to sftp://YOUR_SERVER_IP:2222 using the configured username and password.

The Docker Compose configuration has its own container, port, dependencies, and persistent SSH host-key volume; it does not alter the WebDAV service. Keep the /data volume between restarts so clients see the same SSH host fingerprint. This server requires a host/provider that supports inbound TCP connections; HTTP-only platforms cannot expose SFTP.

### Run directly with Python

Install the SFTP-only dependencies and start the single-file server:

```bash
python -m pip install -r requirements-sftp.txt
python sftp_server.py
```

You may paste credentials into the constants at the top of sftp_server.py, or set the matching environment variables. Environment variables take precedence over constants. Required settings are R2_ACCOUNT_ID (or R2_ENDPOINT_URL), R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, SFTP_USERNAME, and SFTP_PASSWORD. The password must be at least 12 characters.

### SFTP notes

- Use a bucket-scoped R2 API token with Object Read & Write permissions.
- SFTP encrypts authentication and file transfer through SSH. Keep the host key file persistent to prevent fingerprint changes.
- Folder names are represented by R2 key prefixes and optional zero-byte directory markers. R2 does not natively store POSIX owners, groups, permissions, or symbolic links; chmod/utime requests are accepted as no-ops and symlinks are unsupported.
- Uploads need temporary disk space approximately equal to the file being written. Reads use ranged R2 requests, avoiding a full local download for each read session.
- Renaming a folder copies its objects to the new prefix and then deletes the originals, so a folder rename is not atomic. Avoid modifying that folder concurrently during rename.

## Environment variables

| Variable | Required | Default | Meaning |
|---|---:|---|---|
| `R2_ACCOUNT_ID` | One of account ID or endpoint | — | Cloudflare account ID used to build the R2 endpoint |
| `R2_ENDPOINT_URL` | One of account ID or endpoint | derived from account ID | Full S3-compatible R2 endpoint |
| `R2_ACCESS_KEY_ID` | Yes | — | R2 S3 access key |
| `R2_SECRET_ACCESS_KEY` | Yes | — | R2 S3 secret key |
| `R2_BUCKET_NAME` | Yes | — | Bucket exposed at WebDAV root |
| `R2_REGION` | No | `auto` | S3 signing region |
| `WEBDAV_USERNAME` | Yes | — | Basic-auth username |
| `WEBDAV_PASSWORD` | Yes | — | Basic-auth password, minimum 12 characters |
| `PORT` | No | `8080` | Container/app port |
| `HOST_PORT` | No | `8080` | Host port published by Docker Compose (not read by the app) |
| `HOST` | No | `0.0.0.0` | Bind address inside container |
| `MAX_UPLOAD_MB` | No | `0` | Optional upload limit; `0` disables the app-level limit |
| `TMP_DIR` | No | `/tmp/r2-webdav` | Temporary upload staging directory |
| `MAX_PROPFIND_ITEMS` | No | `10000` | Safety cap on a single directory listing |
| `LOCK_TIMEOUT` | No | `3600` | Maximum granted lock duration in seconds; minimum 60 |
| `LOG_LEVEL` | No | `INFO` | Python log level |

## Notes and limitations

- **Use HTTPS on any public network.** HTTP Basic authentication is not encrypted by itself. Put this service behind Caddy, Nginx, Traefik, or another TLS reverse proxy; do not expose the plain HTTP port directly to the public internet.
- The configured bucket is exposed as the WebDAV root. Anyone with these WebDAV credentials can access every object permitted by the R2 token, so use a token scoped to the intended bucket.
- Uploads stage to temporary disk before they are transferred to R2. Make sure Docker's temporary volume has sufficient free space for the largest upload and that the host has room. The app does not keep a second permanent copy.
- Locks are held in memory by one server process and are lost on restart. Run one replica unless you replace the lock store with a shared implementation.
- `PROPFIND` supports Depth `0` and `1`; Depth `infinity` is rejected to prevent accidental full-bucket scans.
- R2 does not provide a native filesystem. Folder names are represented by object prefixes and optional zero-byte directory markers. Renaming/moving a folder requires copying its objects before deleting the source, so it is not atomic.
- Very large objects that exceed an S3-compatible single-copy limit rely on boto3's managed copy transfer. Test the largest object sizes you plan to use before production use.

## Run tests

Install the development dependencies and run the unit/smoke tests:

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

## Single-file mode (credentials inside Python)

If you prefer one Python file rather than environment variables and Docker, use [standalone_webdav.py](./standalone_webdav.py). It contains the WebDAV implementation and all settings in the same file.

1. Install Python 3.10+ and the dependencies:

   ```bash
   python -m pip install aiohttp boto3 botocore defusedxml
   ```

2. Open standalone_webdav.py and edit the configuration constants near the top: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, WEBDAV_USERNAME, and WEBDAV_PASSWORD.
3. Run it:

   ```bash
   python standalone_webdav.py
   ```

4. Connect to http://YOUR_SERVER:8080/ with the WebDAV username and password you configured. Use HTTPS via a reverse proxy before exposing it publicly.

The file deliberately contains placeholders rather than real keys. Avoid committing your actual R2 secret key or WebDAV password to GitHub; if credentials are exposed, revoke and replace them.

## Run without Docker

```bash
python -m venv .venv
. .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp .env.example .env
# Export the values from .env into your shell, then:
python app.py
```

## Troubleshooting

- **401 Unauthorized:** check the WebDAV username/password; make sure your reverse proxy preserves `Authorization`.
- **502 from WebDAV:** inspect `docker compose logs -f` and confirm the R2 token, endpoint, bucket, and permissions.
- **Upload fails partway:** check `df -h`, the temporary Docker volume/host disk, proxy body-size limits, idle timeout, and container logs.
- **Client lists but cannot upload:** verify that the R2 token has Object Read & Write permissions and that the proxy forwards `PUT`, `MKCOL`, `MOVE`, `COPY`, `LOCK`, and `UNLOCK`.
