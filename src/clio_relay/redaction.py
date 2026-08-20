"""Redact credentials and secrets from validation evidence (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). Runtime ownership
tokens are intentionally durable -- cleanup must be able to authenticate a
process after the originating CLI exits -- but they are capabilities and
must never be copied into a written report, a CLI response, or a redacted
CLI invocation echo. This module is the one redaction owner:
:func:`sensitive_key` decides which keys carry a secret,
:func:`collect_sensitive_values`/:func:`redact_sensitive_value` walk a JSON
document to strip every occurrence of a secret value (not just the key that
named it -- the same value can leak back out through free-form evidence
text), :func:`redacted_invocation` scrubs a CLI argv echo, and
:func:`redact_url` strips userinfo/query/fragment from a URL before it is
recorded as evidence. :func:`redact_sensitive_values` is the whole-document
entry point :func:`~clio_relay.validation_report.write_validation_report`
and the GACT/HTTP surfaces call before a report or response ever leaves the
process.
"""

from __future__ import annotations

from typing import cast
from urllib.parse import urlsplit, urlunsplit


def sensitive_key(key: object) -> bool:
    """Return whether a mapping key names a credential or capability value."""
    if not isinstance(key, str):
        return False
    normalized = key.strip().casefold().replace("-", "_").replace(".", "_")
    if normalized in {
        "authorization",
        "credential",
        "credentials",
        "password",
        "private_key",
        "secret",
        "secret_key",
        "token",
    }:
        return True
    return normalized.endswith(
        (
            "_authorization",
            "_credential",
            "_credentials",
            "_password",
            "_private_key",
            "_secret",
            "_secret_key",
            "_token",
        )
    )


def collect_sensitive_values(value: object, output: set[str]) -> None:
    """Collect every secret string value reachable under a sensitive key."""
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        for key, nested in mapping.items():
            if sensitive_key(key) and isinstance(nested, str) and nested:
                output.add(nested)
            else:
                collect_sensitive_values(nested, output)
        return
    if isinstance(value, list):
        for nested in cast(list[object], value):
            collect_sensitive_values(nested, output)
        return
    if isinstance(value, tuple):
        for nested in cast(tuple[object, ...], value):
            collect_sensitive_values(nested, output)


def redact_sensitive_value(value: object, sensitive_values: set[str]) -> object:
    """Return a copy of ``value`` with every known secret value replaced."""
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        return {
            str(key): (
                "<redacted>"
                if sensitive_key(key)
                else redact_sensitive_value(nested, sensitive_values)
            )
            for key, nested in mapping.items()
        }
    if isinstance(value, list):
        return [
            redact_sensitive_value(nested, sensitive_values) for nested in cast(list[object], value)
        ]
    if isinstance(value, tuple):
        return [
            redact_sensitive_value(nested, sensitive_values)
            for nested in cast(tuple[object, ...], value)
        ]
    if isinstance(value, str):
        redacted = value
        for sensitive in sorted(sensitive_values, key=len, reverse=True):
            redacted = redacted.replace(sensitive, "<redacted>")
        return redacted
    return value


def redacted_invocation(arguments: list[str]) -> list[str]:
    """Return a CLI argv echo with credential flags and their values scrubbed."""
    sensitive = {
        "--api-token",
        "--password",
        "--secret",
        "--token",
        "--transport-secret-key",
        "--transport-token",
    }
    redacted: list[str] = []
    hide_next = False
    for argument in arguments:
        if hide_next:
            redacted.append("<redacted>")
            hide_next = False
            continue
        flag, separator, _value = argument.partition("=")
        if flag in sensitive:
            redacted.append(f"{flag}=<redacted>" if separator else flag)
            hide_next = not separator
            continue
        redacted.append(argument)
    return redacted


def redact_url(value: str) -> str:
    """Strip userinfo, query, and fragment from a URL before it is recorded."""
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https", "ssh", "git"}:
        return value
    hostname = parsed.hostname or ""
    netloc = hostname
    if parsed.port is not None:
        netloc = f"{hostname}:{parsed.port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path, "", ""))


def redact_sensitive_values(value: object) -> object:
    """Return a JSON-compatible copy with capability and credential values removed.

    Runtime ownership tokens are intentionally durable because cleanup must be able
    to authenticate a process after the originating CLI exits. They are capabilities,
    however, and must never be copied into reports or routine CLI responses. Values
    found under a sensitive key are also removed from free-form strings elsewhere in
    the document so command/evidence text cannot accidentally disclose the same
    credential.
    """
    sensitive_values: set[str] = set()
    collect_sensitive_values(value, sensitive_values)
    return redact_sensitive_value(value, sensitive_values)
