"""A Tapis v3 stub: the calls `tapipy` 25.4.0 makes for the `tapis` adapter.

It serves the authenticator's device code and token endpoints and the Systems,
Apps, Jobs and Files endpoints, with the envelope Tapis uses. Files live on the
stub's own disk: a system's ``rootDir`` is a real directory on the machine the
stub runs on, so a job really runs relay's launcher. Tests drive the parts a
person or TACC would: approving a device code, holding or forcing a job state,
refusing a credential or a refresh. Every request is recorded.
"""

# FastAPI registers the route functions through their decorators.
# pyright: reportUnusedFunction=false

from __future__ import annotations

import base64
import hashlib
import os
import secrets
import subprocess
import tempfile
import threading
import time
import uuid as uuidlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path
from typing import Any

import jwt
from fastapi import FastAPI, Request, Response

from .files_routes import add_file_routes
from .replies import envelope as _envelope
from .replies import error as _error
from .runner import JobRunner, StubJob

SECRET = "tapis-stub-signing-key-not-a-real-secret-0000"
VERIFICATION_BASE = "https://tapis-stub.invalid"
DEVICE_CODE_TTL_S = 600
DEVICE_TOKEN_TTL_S = 30 * 24 * 3600
REFRESH_TOKEN_TTL_S = 365 * 24 * 3600


@dataclass
class DeviceGrant:
    device_code: str
    user_code: str
    client_id: str
    expires_at: float
    status: str = "Created"
    username: str | None = None


class StubState:
    """Everything the stub knows. Tests read and drive it directly or over HTTP."""

    def __init__(self, tenant_id: str = "tacc") -> None:
        self.tenant_id = tenant_id
        self.lock = threading.RLock()
        self.requests: list[dict[str, Any]] = []
        self.devices: dict[str, DeviceGrant] = {}
        self.revoked: set[str] = set()
        self.refused_refresh = False
        self.systems: dict[str, dict[str, Any]] = {}
        self.credentials: dict[tuple[str, str], dict[str, Any]] = {}
        self.refuse_credential: str | None = None
        self.apps: dict[tuple[str, str], dict[str, Any]] = {}
        self.jobs: dict[str, StubJob] = {}
        self.hold = False
        # Send BATCH jobs to the machine's real Slurm with sbatch, and cancel with scancel.
        self.slurm = False
        self.access_ttl_s = DEVICE_TOKEN_TTL_S
        # Hosts the stub treats as itself: no SSH check, jobs run on this machine.
        self.local_hosts = {"localhost"}

    # tokens

    def issue(self, username: str, client_id: str, *, refresh: bool) -> dict[str, Any]:
        now = int(time.time())
        access_exp = now + self.access_ttl_s
        claims = {
            "jti": str(uuidlib.uuid4()),
            "iss": f"{VERIFICATION_BASE}/v3/tokens",
            "sub": f"{username}@{self.tenant_id}",
            "tapis/tenant_id": self.tenant_id,
            "tapis/token_type": "access",
            "tapis/username": username,
            "tapis/account_type": "user",
            "tapis/client_id": client_id,
            "exp": access_exp,
        }
        access = jwt.encode(claims, SECRET, algorithm="HS256")
        result: dict[str, Any] = {
            "access_token": {
                "access_token": access,
                "expires_at": datetime.fromtimestamp(access_exp, UTC).isoformat(),
                "expires_in": self.access_ttl_s,
                "jti": claims["jti"],
            }
        }
        if refresh:
            refresh_exp = now + REFRESH_TOKEN_TTL_S
            rclaims = {
                "jti": str(uuidlib.uuid4()),
                "tapis/token_type": "refresh",
                "tapis/tenant_id": self.tenant_id,
                "tapis/username": username,
                "tapis/client_id": client_id,
                "exp": refresh_exp,
            }
            result["refresh_token"] = {
                "refresh_token": jwt.encode(rclaims, SECRET, algorithm="HS256"),
                "expires_at": datetime.fromtimestamp(refresh_exp, UTC).isoformat(),
                "expires_in": REFRESH_TOKEN_TTL_S,
                "jti": rclaims["jti"],
            }
        return result

    def user_for(self, token: str | None) -> str | None:
        if not token or token in self.revoked:
            return None
        try:
            claims = jwt.decode(token, SECRET, algorithms=["HS256"])
        except jwt.PyJWTError:
            return None
        if claims.get("tapis/token_type") != "access":
            return None
        return str(claims["tapis/username"])

    def approve(self, user_code: str, username: str) -> bool:
        """Do what the person does on TACC's page: enter the code and sign in."""
        with self.lock:
            for grant in self.devices.values():
                if grant.user_code == user_code:
                    grant.status = "Entered"
                    grant.username = username
                    return True
        return False

    def host_path(self, system_id: str, path: str) -> Path:
        system = self.systems[system_id]
        root = str(system.get("rootDir") or "/")
        return Path(root.rstrip("/") + "/" + path.lstrip("/"))

    def requests_to(self, prefix: str, method: str | None = None) -> list[dict[str, Any]]:
        with self.lock:
            return [
                item
                for item in self.requests
                if item["path"].startswith(prefix) and (method is None or item["method"] == method)
            ]


def _ssh_login_check(system: dict[str, Any], login: str, private_key: str) -> str | None:
    """Log in once with the key, as Tapis's credential check does. Return an error or None."""
    with tempfile.TemporaryDirectory(prefix="tapis-stub-") as scratch:
        key_path = Path(scratch) / "key"
        key_path.write_text(private_key, encoding="utf-8")
        key_path.chmod(0o600)
        argv = [
            "ssh",
            "-F",
            "/dev/null",
            "-i",
            str(key_path),
            "-p",
            str(system.get("port") or 22),
            "-o",
            "BatchMode=yes",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
            f"{login}@{system['host']}",
            "true",
        ]
        done = subprocess.run(argv, capture_output=True, text=True, check=False)
        if done.returncode != 0:
            return f"SSH login as {login} failed: {done.stderr.strip() or done.returncode}"
    return None


def create_app(state: StubState) -> FastAPI:
    """Build the stub's FastAPI app over ``state``."""
    app = FastAPI(title="tapis-stub", openapi_url=None, docs_url=None, redoc_url=None)
    runner = JobRunner(state)

    @app.middleware("http")
    async def record(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        body = await request.body()
        entry: dict[str, Any] = {
            "method": request.method,
            "path": request.url.path,
            "query": dict(request.query_params),
            "headers": {k.lower(): v for k, v in request.headers.items()},
            "body": body.decode("utf-8", "replace") if body else "",
            "at": time.time(),
        }
        with state.lock:
            state.requests.append(entry)
        response = await call_next(request)
        entry["status"] = response.status_code
        return response

    def user(request: Request) -> str | None:
        return state.user_for(request.headers.get("x-tapis-token"))

    # authenticator

    @app.post("/v3/oauth2/device/code")
    async def device_code(request: Request) -> Response:
        data = await request.json()
        client_id = str(data.get("client_id") or "")
        if not client_id:
            return _error(400, "Required client_id parameter missing.")
        grant = DeviceGrant(
            device_code=secrets.token_urlsafe(30)[:40],
            user_code="".join(secrets.choice("ABCDEFGHJKLMNPQRSTUVWXYZ") for _ in range(8)),
            client_id=client_id,
            expires_at=time.time() + DEVICE_CODE_TTL_S,
        )
        with state.lock:
            state.devices[grant.device_code] = grant
        expires = datetime.fromtimestamp(grant.expires_at, UTC)
        return _envelope(
            {
                "user_code": grant.user_code,
                "device_code": grant.device_code,
                "verification_uri": f"{VERIFICATION_BASE}/v3/oauth2/device?client_id={client_id}",
                # The real authenticator returns the expiry time here, not seconds.
                "expires_in": format_datetime(expires, usegmt=True),
                "client_id": client_id,
            }
        )

    @app.post("/v3/oauth2/tokens")
    async def tokens(request: Request) -> Response:
        data = await request.json()
        grant_type = data.get("grant_type")
        client_id = str(data.get("client_id") or "")
        client_key = str(data.get("client_key") or "")
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("basic "):
            raw = base64.b64decode(auth.split(" ", 1)[1]).decode()
            client_id, _, client_key = raw.partition(":")
        if grant_type == "device_code":
            code = str(data.get("device_code") or "")
            with state.lock:
                grant = state.devices.get(code)
                if grant is None:
                    return _error(400, "device code not valid.")
                if grant.status != "Entered":
                    return _error(400, "device code not ready.")
                if time.time() > grant.expires_at + 6:
                    del state.devices[code]
                    return _error(400, "device code has expired and is now deleted.")
                if grant.client_id != client_id:
                    return _error(
                        400,
                        "Client IDs do not match between device code and passed in client_id",
                    )
                del state.devices[code]
            return _envelope(
                state.issue(grant.username or "anonymous", client_id, refresh=True),
                "Token created successfully.",
                201,
            )
        if grant_type == "refresh_token":
            if not client_key:
                return _error(400, "Invalid client credentials.")
            if state.refused_refresh:
                return _error(400, "Invalid refresh token.")
            token = str(data.get("refresh_token") or "")
            try:
                claims = jwt.decode(token, SECRET, algorithms=["HS256"])
            except jwt.PyJWTError:
                return _error(400, "Invalid refresh token.")
            if token in state.revoked or claims.get("tapis/token_type") != "refresh":
                return _error(400, "Invalid refresh token.")
            return _envelope(
                state.issue(str(claims["tapis/username"]), client_id, refresh=True),
                "Token created successfully.",
                201,
            )
        return _error(400, "Invalid grant_type")

    @app.post("/v3/oauth2/tokens/revoke")
    async def revoke(request: Request) -> Response:
        data = await request.json()
        with state.lock:
            state.revoked.add(str(data.get("token") or ""))
        return _envelope(None, "Token successfully revoked.")

    # systems

    @app.get("/v3/systems/{system_id}")
    async def get_system(system_id: str, request: Request) -> Response:
        if user(request) is None:
            return _error(401, "Invalid token")
        system = state.systems.get(system_id)
        if system is None:
            return _error(404, f"System not found: {system_id}")
        return _envelope(system)

    @app.post("/v3/systems")
    async def create_system(request: Request) -> Response:
        owner = user(request)
        if owner is None:
            return _error(401, "Invalid token")
        data = await request.json()
        system_id = str(data["id"])
        if system_id in state.systems:
            return _error(409, f"System already exists: {system_id}")
        system = dict(data)
        system.setdefault("rootDir", "/")
        system["owner"] = owner
        system.pop("authnCredential", None)
        with state.lock:
            state.systems[system_id] = system
        return _envelope({"url": f"/v3/systems/{system_id}"}, "System created", 201)

    @app.post("/v3/systems/{system_id}/delete")
    async def delete_system(system_id: str, request: Request) -> Response:
        if user(request) is None:
            return _error(401, "Invalid token")
        with state.lock:
            existed = state.systems.pop(system_id, None) is not None
            for key in [key for key in state.credentials if key[0] == system_id]:
                del state.credentials[key]
        return _envelope({"changes": int(existed)})

    @app.post("/v3/systems/credential/{system_id}/user/{username}")
    async def create_credential(system_id: str, username: str, request: Request) -> Response:
        if user(request) is None:
            return _error(401, "Invalid token")
        system = state.systems.get(system_id)
        if system is None:
            return _error(404, f"System not found: {system_id}")
        data = await request.json()
        tms = request.query_params.get("createTmsKeys", "false").lower() == "true"
        skip = request.query_params.get("skipCredentialCheck", "false").lower() == "true"
        if state.refuse_credential is not None:
            return _error(400, state.refuse_credential)
        login = str(data.get("loginUser") or system.get("effectiveUserId") or username)
        credential: dict[str, Any] = {"loginUser": login, "tms": tms}
        if tms:
            credential["tmsFingerprint"] = hashlib.sha256(os.urandom(16)).hexdigest()[:16]
        else:
            private_key = str(data.get("privateKey") or "")
            public_key = str(data.get("publicKey") or "")
            if not private_key or not public_key:
                return _error(400, "privateKey and publicKey are required for PKI_KEYS")
            credential["publicKey"] = public_key
            credential["privateKeySha256"] = hashlib.sha256(private_key.encode()).hexdigest()
            if not skip and str(system.get("host")) not in state.local_hosts:
                failure = _ssh_login_check(system, login, private_key)
                if failure is not None:
                    return _error(400, f"Credential check failed: {failure}")
        with state.lock:
            state.credentials[(system_id, username)] = credential
        return _envelope(None, "Credential created", 201)

    @app.delete("/v3/systems/credential/{system_id}/user/{username}")
    async def remove_credential(system_id: str, username: str, request: Request) -> Response:
        if user(request) is None:
            return _error(401, "Invalid token")
        with state.lock:
            existed = state.credentials.pop((system_id, username), None) is not None
        return _envelope({"changes": int(existed)})

    @app.post("/v3/systems/credential/{system_id}/user/{username}/check")
    async def check_credential(system_id: str, username: str, request: Request) -> Response:
        if user(request) is None:
            return _error(401, "Invalid token")
        if (system_id, username) not in state.credentials:
            return _error(400, f"No credential for {username} on {system_id}")
        return _envelope({"valid": True}, "Credential check succeeded")

    # apps

    @app.post("/v3/apps")
    async def create_app_version(request: Request) -> Response:
        owner = user(request)
        if owner is None:
            return _error(401, "Invalid token")
        data = await request.json()
        key = (str(data["id"]), str(data["version"]))
        if key in state.apps:
            return _error(409, f"App already exists: {key[0]} {key[1]}")
        with state.lock:
            state.apps[key] = dict(data) | {"owner": owner}
        return _envelope({"url": f"/v3/apps/{key[0]}/{key[1]}"}, "App created", 201)

    @app.get("/v3/apps/{app_id}/{version}")
    async def get_app(app_id: str, version: str, request: Request) -> Response:
        if user(request) is None:
            return _error(401, "Invalid token")
        found = state.apps.get((app_id, version))
        if found is None:
            return _error(404, f"App not found: {app_id} {version}")
        return _envelope(found)

    # jobs

    def job_view(job: StubJob) -> dict[str, Any]:
        return job.request | {
            "uuid": job.uuid,
            "status": job.status,
            "lastMessage": job.last_message,
            "owner": job.owner,
            "created": job.created,
            "remoteJobId": job.remote_job_id,
            "condition": None,
        }

    @app.post("/v3/jobs/submit")
    async def submit(request: Request) -> Response:
        owner = user(request)
        if owner is None:
            return _error(401, "Invalid token")
        data = await request.json()
        app_key = (str(data.get("appId")), str(data.get("appVersion")))
        if app_key not in state.apps:
            return _error(400, f"App not found: {app_key[0]} {app_key[1]}")
        merged = dict(data)
        merged.setdefault("execSystemId", state.apps[app_key]["jobAttributes"]["execSystemId"])
        system = state.systems.get(str(merged["execSystemId"]), {})
        if str(merged.get("cmdPrefix") or "").strip() and not system.get("enableCmdPrefix"):
            return _error(
                400,
                "JOBS_CMD_PREFIX_NOT_ENABLED_FOR_SYSTEM: command prefixes are not "
                f"enabled on system {merged['execSystemId']}",
            )
        job = StubJob(uuid=f"{uuidlib.uuid4()}-007", request=merged, owner=owner)
        job.history.append("PENDING")
        with state.lock:
            state.jobs[job.uuid] = job
        runner.start(job)
        return _envelope(job_view(job), "Job submitted")

    @app.get("/v3/jobs/search")
    async def search(request: Request) -> Response:
        owner = user(request)
        if owner is None:
            return _error(401, "Invalid token")
        filters = {
            key.removesuffix(".eq"): value
            for key, value in request.query_params.items()
            if key.endswith(".eq")
        }
        found = [
            job_view(job)
            for job in list(state.jobs.values())
            if job.owner == owner
            and all(str(job_view(job).get(key)) == value for key, value in filters.items())
        ]
        return _envelope(found)

    @app.get("/v3/jobs/{job_uuid}/status")
    async def job_status(job_uuid: str, request: Request) -> Response:
        if user(request) is None:
            return _error(401, "Invalid token")
        job = state.jobs.get(job_uuid)
        if job is None:
            return _error(404, f"Job not found: {job_uuid}")
        return _envelope({"status": job.status, "condition": None})

    @app.get("/v3/jobs/{job_uuid}")
    async def get_job(job_uuid: str, request: Request) -> Response:
        if user(request) is None:
            return _error(401, "Invalid token")
        job = state.jobs.get(job_uuid)
        if job is None:
            return _error(404, f"Job not found: {job_uuid}")
        return _envelope(job_view(job))

    @app.post("/v3/jobs/{job_uuid}/cancel")
    async def cancel(job_uuid: str, request: Request) -> Response:
        if user(request) is None:
            return _error(401, "Invalid token")
        job = state.jobs.get(job_uuid)
        if job is None:
            return _error(404, f"Job not found: {job_uuid}")
        runner.cancel(job)
        return _envelope({"message": "cancel requested"})

    add_file_routes(app, state, user)

    # controls a test drives, standing in for the person and for TACC

    @app.post("/_stub/device/approve")
    async def approve(request: Request) -> Response:
        data = await request.json()
        ok = state.approve(str(data["user_code"]), str(data.get("username") or "testuser"))
        return _envelope({"approved": ok}, status=200 if ok else 404)

    @app.post("/_stub/jobs/{job_uuid}/release")
    async def release(job_uuid: str) -> Response:
        job = state.jobs.get(job_uuid)
        if job is None:
            return _error(404, "no such job")
        job.released.set()
        return _envelope({"released": True})

    @app.post("/_stub/jobs/{job_uuid}/status")
    async def force_status(job_uuid: str, request: Request) -> Response:
        data = await request.json()
        job = state.jobs.get(job_uuid)
        if job is None:
            return _error(404, "no such job")
        with state.lock:
            job.forced = True
            job.status = str(data["status"])
            job.history.append(job.status)
            job.last_message = str(data.get("lastMessage") or job.last_message)
        job.released.set()
        return _envelope(job_view(job))

    return app
