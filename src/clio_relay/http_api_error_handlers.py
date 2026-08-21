"""Global FastAPI exception handlers registered onto the ``http_api`` app.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``http_api.py``. None of these four handlers referenced any of
``create_app()``'s local closures -- FastAPI calls them as
``(Request, Exception) -> Response`` regardless of which module defines
them -- so this is an unmodified, atomic move, with one deliberate
exception: the module logger is constructed from the hardcoded name
``"clio_relay.http_api"`` instead of ``__name__`` (which would resolve to
``"clio_relay.http_api_error_handlers"`` here). ``logging.getLogger`` caches
loggers by name, so this is the *same* Logger object the pre-split
``http_api.py`` used -- ``tests/test_door_errors.py``'s
``caplog.at_level("ERROR", logger="clio_relay.http_api")`` /
``record.name == "clio_relay.http_api"`` assertions keep observing the
identical emitted records regardless of which file's code calls
``logger.exception(...)``.
"""

from __future__ import annotations

import logging

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from clio_relay import door_error_adapters, door_errors

logger = logging.getLogger("clio_relay.http_api")

_FALLBACK_PROBLEM_DOCUMENT: dict[str, object] = {
    "type": "urn:clio-relay:error:internal_error",
    "title": "Internal error",
    "status": 500,
    "detail": "relay encountered an internal error.",
    "schema_version": door_errors.SCHEMA_VERSION,
    "reason": "internal_error",
    "retryable": False,
    "truncation": None,
}


async def _relay_unhandled_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    """Translate a novel route failure through the one door owner.

    Deliberate faults use the more-specific handler below. The guard keeps a
    door defect from replacing a novel exception with Starlette's bare body.
    """
    try:
        fault = door_errors.classify(exc)
        document = door_error_adapters.as_http_problem(fault)
        status_code = fault.http_status
    except Exception:
        logger.exception(
            "clio-relay: door_errors could not classify/render %s; "
            "falling back to the hardcoded internal_error document",
            type(exc).__name__,
        )
        document = _FALLBACK_PROBLEM_DOCUMENT
        status_code = 500
    return JSONResponse(document, status_code=status_code, media_type="application/problem+json")


async def _relay_http_problem_handler(
    _request: Request,
    exc: Exception,
) -> JSONResponse:
    """Serve one deliberate preclassified route fault as error.v1.

    Starlette's handler contract is (Request, Exception); registration pins
    this handler to HTTPProblemError, so any other type re-raises into the
    unhandled-exception handler's typed internal_error path.
    """
    if not isinstance(exc, door_errors.HTTPProblemError):
        raise exc
    document = door_error_adapters.as_http_problem(exc.fault)
    return JSONResponse(
        document,
        status_code=exc.fault.http_status,
        media_type="application/problem+json",
    )


async def _relay_request_validation_handler(
    _request: Request,
    exc: Exception,
) -> JSONResponse:
    """Serve framework request validation through the relay error owner."""
    if not isinstance(exc, RequestValidationError):
        raise exc
    fault = door_errors.fault_for_reason(
        "request_validation_failed",
        "Request validation failed.",
    )
    return JSONResponse(
        door_error_adapters.as_http_problem(fault),
        status_code=fault.http_status,
        media_type="application/problem+json",
    )


async def _relay_framework_http_handler(
    _request: Request,
    exc: Exception,
) -> JSONResponse:
    """Serve Starlette HTTP failures without losing framework status or headers."""
    if not isinstance(exc, StarletteHTTPException):
        raise exc
    reason = {
        404: "route_not_found",
        405: "method_not_allowed",
    }.get(exc.status_code, "framework_http_error")
    fault = door_errors.fault_for_http_status(reason, exc.status_code)
    return JSONResponse(
        door_error_adapters.as_http_problem(fault),
        status_code=exc.status_code,
        headers=exc.headers,
        media_type="application/problem+json",
    )
