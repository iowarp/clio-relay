"""A small client for the DOE IRI Facility API.

It covers the calls relay needs: submit, status and cancel under ``/compute``,
the file calls under ``/filesystem``, and polling under ``/task``. Version 1
passes file arguments as query parameters; version 2 posts them as a JSON body
and takes an ``Idempotency-Key`` on submit. Errors arrive as RFC 9457 problem
documents and become relay refusals.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any, Literal, cast

import httpx

from clio_relay.jobs.refusals import Refusal, RefusalReason

ApiVersion = Literal["v1", "v2"]
TokenFn = Callable[[], Awaitable[str]]
FILE_SIZE_LIMIT = 5 * 1024 * 1024

# v1 uses these methods for the file calls; v2 posts them all.
_V1_METHODS = {"rm": "DELETE", "mkdir": "POST", "upload": "POST"}


def problem_text(response: httpx.Response) -> str:
    """Return the readable part of a problem document, or the body."""
    try:
        body: Any = response.json()
    except (json.JSONDecodeError, ValueError):
        return response.text[:500] or response.reason_phrase
    if isinstance(body, dict):
        doc = cast(dict[str, Any], body)
        parts = [str(doc[key]) for key in ("title", "detail") if doc.get(key)]
        if parts:
            return ": ".join(parts)
    return str(cast(object, body))[:500]


class IriClient:
    """The IRI Facility API at ``base_url``, with bearer tokens from ``token``."""

    def __init__(
        self,
        base_url: str,
        version: ApiVersion,
        token: TokenFn,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        poll_s: float = 1.0,
        max_poll_s: float = 10.0,
        stall_s: float = 600.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.version = version
        self.token = token
        self.transport = transport
        self.poll_s = poll_s
        self.max_poll_s = max_poll_s
        self.stall_s = stall_s

    def url(self, path: str) -> str:
        """Return the full URL of an API path such as ``/compute/resources``."""
        return f"{self.base_url}/api/{self.version}{path}"

    async def request(
        self,
        method: str,
        path_or_url: str,
        *,
        doing: str,
        not_found: RefusalReason = RefusalReason.OPERATION_NOT_FOUND,
        failed: RefusalReason = RefusalReason.UNREACHABLE,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send one request and turn an error status into a refusal."""
        if path_or_url.startswith(("http://", "https://")):
            url = path_or_url
        elif path_or_url.startswith("/api/"):
            url = f"{self.base_url}{path_or_url}"
        else:
            url = self.url(path_or_url)
        headers = dict(kwargs.pop("headers", {}) or {})
        headers["Authorization"] = f"Bearer {await self.token()}"
        headers.setdefault("Accept", "application/json")
        try:
            async with httpx.AsyncClient(transport=self.transport, timeout=60.0) as client:
                response = await client.request(method, url, headers=headers, **kwargs)
        except httpx.HTTPError as exc:
            raise Refusal(
                RefusalReason.UNREACHABLE, f"the IRI API did not answer while {doing}: {exc}"
            ) from exc
        if response.status_code < 400:
            return response
        text = problem_text(response)
        status = response.status_code
        if status == 401:
            raise Refusal(
                RefusalReason.NEEDS_SIGN_IN,
                f"the IRI API refused the token while {doing}: {text}",
                http_status=status,
            )
        if status == 404:
            raise Refusal(not_found, f"the IRI API found nothing while {doing}: {text}")
        if status == 422:
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"the IRI API rejected the request while {doing}: {text}",
                http_status=status,
            )
        reason = RefusalReason.UNREACHABLE if status >= 500 else failed
        raise Refusal(reason, f"the IRI API failed while {doing}: {text}", http_status=status)

    @staticmethod
    def _json(response: httpx.Response) -> dict[str, Any]:
        body: Any = response.json()
        return cast(dict[str, Any], body) if isinstance(body, dict) else {"items": body}

    async def submit(
        self, resource_id: str, body: dict[str, Any], *, idempotency_key: str
    ) -> dict[str, Any]:
        """Submit a job and return the API's ``Job``."""
        headers = {"Idempotency-Key": idempotency_key} if self.version == "v2" else {}
        response = await self.request(
            "POST",
            f"/compute/job/{resource_id}",
            doing="submitting the job",
            failed=RefusalReason.SUBMIT_FAILED,
            not_found=RefusalReason.INVALID_CONFIG,
            json=body,
            headers=headers,
        )
        return self._json(response)

    async def job(self, resource_id: str, job_id: str) -> dict[str, Any]:
        """Return one job's ``Job``, including finished jobs."""
        response = await self.request(
            "GET",
            f"/compute/status/{resource_id}/{job_id}",
            doing=f"reading job {job_id}",
            params={"historical": "true"},
        )
        return self._json(response)

    async def cancel(self, resource_id: str, job_id: str) -> None:
        """Ask the API to cancel a job."""
        await self.request(
            "DELETE",
            f"/compute/cancel/{resource_id}/{job_id}",
            doing=f"cancelling job {job_id}",
            failed=RefusalReason.NOT_CANCELLABLE,
        )

    async def resources(self) -> list[dict[str, Any]]:
        """Return the compute resources the API lists."""
        response = await self.request("GET", "/compute/resources", doing="listing resources")
        body: Any = response.json()
        items: Any = body if isinstance(body, list) else []
        return [cast(dict[str, Any], item) for item in cast(list[Any], items)]

    async def file_task(
        self,
        command: str,
        resource_id: str,
        args: dict[str, Any],
        *,
        content: bytes | None = None,
    ) -> dict[str, Any]:
        """Run one file call and return its task's result once the task completes."""
        path = f"/filesystem/{command}/{resource_id}"
        doing = f"{command} {args.get('path', '')}".strip()
        kwargs: dict[str, Any] = {}
        if command == "upload":
            method = "POST"
            kwargs["params"] = {"path": args["path"]}
            kwargs["files"] = {"file": ("upload", content or b"", "application/octet-stream")}
        elif self.version == "v2":
            method = "POST"
            kwargs["json"] = args
        else:
            method = _V1_METHODS.get(command, "GET")
            if method == "POST":
                kwargs["json"] = args
            else:
                kwargs["params"] = {
                    key: (str(value).lower() if isinstance(value, bool) else value)
                    for key, value in args.items()
                }
        response = await self.request(
            method, path, doing=doing, not_found=RefusalReason.PATH_NOT_FOUND, **kwargs
        )
        submitted = self._json(response)
        task_id = str(submitted.get("task_id", ""))
        task_uri = str(submitted.get("task_uri") or f"/task/{task_id}")
        return await self.wait_task(task_id, task_uri, doing=doing)

    async def wait_task(self, task_id: str, task_uri: str, *, doing: str) -> dict[str, Any]:
        """Poll a task until it ends. A task whose status stops changing is a stall."""
        delay = self.poll_s
        last_status = ""
        changed_at = time.monotonic()
        while True:
            response = await self.request("GET", task_uri, doing=f"reading task {task_id}")
            task = self._json(response)
            status = str(task.get("status", ""))
            result: Any = task.get("result")
            result_dict = cast(dict[str, Any], result) if isinstance(result, dict) else {}
            if status == "completed":
                return result_dict
            if status in ("failed", "canceled"):
                output = str(result_dict.get("output", status))
                lowered = output.lower()
                if "no such file" in lowered or "not found" in lowered:
                    raise Refusal(RefusalReason.PATH_NOT_FOUND, f"{doing}: {output}")
                raise Refusal(
                    RefusalReason.UNREACHABLE, f"the IRI task for {doing} {status}: {output}"
                )
            if status != last_status:
                last_status, changed_at = status, time.monotonic()
            elif time.monotonic() - changed_at > self.stall_s:
                raise Refusal(
                    RefusalReason.UNREACHABLE,
                    f"the IRI task for {doing} stayed {status} for {self.stall_s:.0f}s",
                    task_id=task_id,
                )
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.max_poll_s)
