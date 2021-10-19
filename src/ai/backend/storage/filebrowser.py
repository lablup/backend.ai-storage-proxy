from __future__ import annotations

import logging
from typing import (
    AsyncIterator,
)

import aiodocker
import aiotools

from ai.backend.common.logging import BraceStyleAdapter

from .context import Context

log = BraceStyleAdapter(logging.getLogger(__name__))

__all__ = (
    "create_or_update",
    "destroy",
    "cleanup",
)


async def create_or_update(
    ctx: Context,
    auth_token: str,
    vfolders: list[str],
) -> tuple[str, int]:
    return "host", 1234


async def destroy(ctx: Context, auth_token: str) -> None:
    pass


async def cleanup(ctx: Context, interval: float) -> None:
    log.info("filebrowser.cleanup")
    pass


async def _enumerate_containers() -> AsyncIterator[str]:
    pass


async def _check_active_connections(container_id: str) -> bool:
    return True
