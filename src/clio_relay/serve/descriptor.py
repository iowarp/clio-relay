"""The view descriptor relay passes on in task progress (RL43).

Vendored, byte for byte, from clio-jarvis so the package that writes a
descriptor, relay and clio-kit's ``open_view`` share one schema.
"""

from __future__ import annotations

import hashlib
import json
from functools import cache
from importlib import resources
from typing import Any

from jsonschema import Draft202012Validator

SCHEMA_ID = "clio.view-descriptor.v1"
SOURCE = {
    "repository": "iowarp/clio-jarvis",
    "path": "schemas/view-descriptor.v1.json",
    "commit": "a6aba08",
    "release": "v0.1.0 (2d12dbb)",
    "sha256": "8773b9ed5905a05b9a4719a39a973fe5b07df168a367e3d78e83236772c1b076",
}


def schema_bytes() -> bytes:
    """Return the vendored schema file, with the line endings it was published with."""
    data = (
        resources.files("clio_relay.serve")
        .joinpath("schemas", "view-descriptor.v1.json")
        .read_bytes()
    )
    return data.replace(b"\r\n", b"\n")


@cache
def schema() -> dict[str, Any]:
    """Return the schema, after checking it is the vendored bytes."""
    data = schema_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != SOURCE["sha256"]:
        raise RuntimeError(f"the vendored view descriptor schema changed: {digest}")
    return json.loads(data)


def problems(descriptor: Any) -> list[str]:
    """Return what is wrong with a descriptor, or nothing."""
    validator: Any = Draft202012Validator(schema())
    errors: list[Any] = list(validator.iter_errors(descriptor))
    return [
        f"{'/'.join(str(part) for part in error.absolute_path) or '(root)'}: {error.message}"
        for error in sorted(errors, key=lambda e: [str(part) for part in e.absolute_path])
    ]
