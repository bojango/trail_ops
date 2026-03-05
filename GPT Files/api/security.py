from __future__ import annotations

import os
from fastapi import Header, HTTPException, status


def api_key_dependency(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    """Optional API key auth.

    If TRAILOPS_API_KEY is set in the environment, callers must provide:
      X-API-Key: <value>

    If TRAILOPS_API_KEY is not set, this dependency is a no-op.
    """
    required = os.getenv("TRAILOPS_API_KEY")
    if not required:
        return

    if not x_api_key or x_api_key != required:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
