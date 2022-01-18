from __future__ import annotations

import logging
from typing import AsyncIterator

import aiodocker

from ai.backend.common.logging import BraceStyleAdapter

from .context import Context

log = BraceStyleAdapter(logging.getLogger(__name__))

__all__ = (
    "create_or_update",
    "destroy",
    "cleanup",
)

docker = aiodocker.Docker()


async def create_or_update(
    ctx: Context,
    auth_token: str,
    vfolders: list[str],
) -> tuple[str, int]:

    image = ctx.local_config["filebrowser"]["image"]
    service_ip = ctx.local_config["filebrowser"]["service_ip"]
    service_port = ctx.local_config["filebrowser"]["service_port"]
    settings_path = ctx.local_config["filebrowser"]["settings_path"]

    container = await docker.containers.create_or_replace(
        config={
            "Cmd": [
                "/bin/filebrowser",
                "-c",
                "/filebrowser_dir/settings.json",
                "-d",
                "/filebrowser_dir/filebrowser.db",
            ],
            "ExposedPorts": {
                "f{service_port}/tcp": {},
            },
            "Image": image,
            "HostConfig": {
                "PortBindings": {
                    f"{service_port}/tcp": [
                        {
                            "HostIp": {service_ip},
                            "HostPort": f"{service_port}/tcp",
                        },
                    ],
                },
                "Mounts": [
                    {
                        "Target": "/filebrowser_dir/",
                        "Source": settings_path,
                        "Type": "bind",
                    },
                    {
                        "Target": "/data/",
                        "Source": ctx.local_config["volume.volume1"]["path"],
                        "Type": "bind",
                    },
                ],
            },
        },
        name="FileBrowser",
    )

    await container.start()

    await docker.close()

    return service_ip, service_port


async def destroy(ctx: Context, auth_token: str) -> None:
    pass


async def cleanup(ctx: Context, interval: float) -> None:
    log.info("filebrowser.cleanup")
    pass


async def _enumerate_containers() -> AsyncIterator[str]:
    pass


async def _check_active_connections(container_id: str) -> bool:
    return True
