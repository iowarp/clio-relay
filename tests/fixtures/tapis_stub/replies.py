"""The envelope every Tapis v3 reply uses."""

from __future__ import annotations

from typing import Any

from fastapi.responses import JSONResponse


def envelope(result: Any, message: str = "ok", status: int = 200) -> JSONResponse:
    return JSONResponse(
        {"result": result, "status": "success", "message": message, "version": "stub"},
        status_code=status,
        media_type="application/json",
    )


def error(status: int, message: str) -> JSONResponse:
    return JSONResponse(
        {"result": None, "status": "error", "message": message, "version": "stub"},
        status_code=status,
        media_type="application/json",
    )
