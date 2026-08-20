"""Linux secret-memory gating and the gated broker child-environment channel.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231).
`enforce_linux_secret_memory_gate` is individually replaced by the test
suite via `monkeypatch.setattr` on the facade module, so
`consume_broker_child_environment`'s call to it goes through the live facade
module (`clio_relay.process_containment`) instead of a bare name.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping
from contextlib import suppress
from typing import cast

from clio_relay import process_containment as _pc
from clio_relay.process_containment_types import (
    BROKER_CHILD_ENVIRONMENT_SCHEMA,
    BROKER_CREDENTIAL_FD_ENV,
    BROKER_PROTOCOL_MAX_BYTES,
    BROKER_READY_FD_ENV,
    _reject_broker_duplicate_keys,
    _ResourceModule,
)


def enforce_linux_secret_memory_gate() -> None:
    """Disable core dumps and same-UID tracing before secret material exists."""
    if not sys.platform.startswith("linux"):
        raise RuntimeError("secure JARVIS runtime signing requires Linux PR_SET_DUMPABLE")
    import ctypes

    resource_module = cast(_ResourceModule, __import__("resource"))

    try:
        resource_module.setrlimit(resource_module.RLIMIT_CORE, (0, 0))
        core_limits = resource_module.getrlimit(resource_module.RLIMIT_CORE)
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"could not disable secret-bearing core dumps: {exc}") from exc
    if core_limits != (0, 0):
        raise RuntimeError(f"secret-bearing core dump limits remained enabled: {core_limits}")
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    prctl.restype = ctypes.c_int
    pr_set_dumpable = 4
    pr_get_dumpable = 3
    if prctl(pr_set_dumpable, 0, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise RuntimeError(f"could not disable secret-bearing process dumps: errno {error_number}")
    dumpable = prctl(pr_get_dumpable, 0, 0, 0, 0)
    if dumpable != 0:
        error_number = ctypes.get_errno()
        raise RuntimeError(
            f"secret-bearing process remained dumpable: state {dumpable}, errno {error_number}"
        )


def broker_child_environment_payload(environment: Mapping[str, str]) -> str:
    """Encode validated child-only environment values for the gated broker pipe."""
    validated: dict[str, str] = {}
    for name, value in environment.items():
        if not name or "=" in name or "\x00" in name or "\x00" in value:
            raise RuntimeError("broker child environment contained an invalid entry")
        validated[name] = value
    payload = json.dumps(
        {
            "schema_version": BROKER_CHILD_ENVIRONMENT_SCHEMA,
            "environment": validated,
        },
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    if not validated or len(payload.encode("utf-8")) > BROKER_PROTOCOL_MAX_BYTES:
        raise RuntimeError("broker child environment payload was empty or exceeded its byte limit")
    return payload


def consume_broker_child_environment() -> bool:
    """Consume child-only environment values only after disabling Linux process inspection."""
    credential_fd_text = os.environ.get(BROKER_CREDENTIAL_FD_ENV)
    ready_fd_text = os.environ.get(BROKER_READY_FD_ENV)
    if credential_fd_text is None and ready_fd_text is None:
        return False
    if credential_fd_text is None or ready_fd_text is None:
        raise RuntimeError("broker child environment descriptors were incomplete")
    _pc.enforce_linux_secret_memory_gate()
    os.environ.pop(BROKER_CREDENTIAL_FD_ENV, None)
    os.environ.pop(BROKER_READY_FD_ENV, None)
    try:
        credential_fd = int(credential_fd_text)
        ready_fd = int(ready_fd_text)
    except ValueError:
        raise RuntimeError("broker child environment descriptors were invalid") from None
    payload = bytearray()
    try:
        while True:
            chunk = os.read(credential_fd, min(4096, BROKER_PROTOCOL_MAX_BYTES + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
            if len(payload) > BROKER_PROTOCOL_MAX_BYTES:
                raise RuntimeError("broker child environment payload exceeded its byte limit")
    except OSError:
        raise RuntimeError("broker child environment payload could not be read") from None
    finally:
        with suppress(OSError):
            os.close(credential_fd)
    try:
        decoded = cast(
            object,
            json.loads(
                payload.decode("utf-8"),
                object_pairs_hook=_reject_broker_duplicate_keys,
            ),
        )
        if not isinstance(decoded, dict):
            raise ValueError
        raw_document = cast(dict[object, object], decoded)
        if set(raw_document) != {"schema_version", "environment"}:
            raise ValueError
        document = cast(dict[str, object], raw_document)
        if document.get("schema_version") != BROKER_CHILD_ENVIRONMENT_SCHEMA or not isinstance(
            document.get("environment"), dict
        ):
            raise ValueError
        environment = cast(dict[object, object], document["environment"])
        if not environment:
            raise ValueError
        validated: dict[str, str] = {}
        for raw_name, raw_value in environment.items():
            if (
                not isinstance(raw_name, str)
                or not raw_name
                or "=" in raw_name
                or "\x00" in raw_name
                or not isinstance(raw_value, str)
                or "\x00" in raw_value
            ):
                raise ValueError
            validated[raw_name] = raw_value
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError):
        raise RuntimeError("broker child environment payload was invalid") from None
    os.environ.update(validated)
    try:
        if os.write(ready_fd, b"1") != 1:
            raise RuntimeError("broker child environment acknowledgement was incomplete")
    except OSError:
        raise RuntimeError("broker child environment acknowledgement failed") from None
    finally:
        with suppress(OSError):
            os.close(ready_fd)
    return True
