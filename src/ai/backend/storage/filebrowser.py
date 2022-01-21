from __future__ import annotations

import json
import logging
import pathlib
from pathlib import Path
from typing import AsyncIterator

import aiodocker
import aiofiles

from ai.backend.common.logging import BraceStyleAdapter

from .context import Context

log = BraceStyleAdapter(logging.getLogger(__name__))

__all__ = (
    "create_or_update",
    "destroy",
    "cleanup",
)


def mangle_path(mount_path, vfid):
    prefix1 = vfid[0:2]
    prefix2 = vfid[2:4]
    rest = vfid[4:]
    return Path(mount_path, prefix1, prefix2, rest)


async def create_or_update(ctx: Context, vfolders: list[str]) -> tuple[str, int]:

    vfolders = vfolders["vfolders"]

    image = ctx.local_config["filebrowser"]["image"]
    service_ip = ctx.local_config["filebrowser"]["service-ip"]
    service_port = ctx.local_config["filebrowser"]["service_port"]
    settings_path = ctx.local_config["filebrowser"]["settings_path"]
    mount_path = ctx.local_config["filebrowser"]["mount_path"]

    settings_file = pathlib.Path(settings_path + "settings.json")

    if not settings_file.exists():
        filebrowser_default_settings = {
            "port": service_port,
            "baseURL": "",
            "address": "",
            "log": "stdout",
            "database": "/filebrowser_dir/filebrowser.db",
            "root": "/data/",
        }

        async with aiofiles.open(settings_path + "settings.json", mode="w") as file:
            await file.write(json.dumps(filebrowser_default_settings))

    docker = aiodocker.Docker()
    config = {
        "Cmd": [
            "/bin/filebrowser",
            "-c",
            "/filebrowser_dir/settings.json",
            "-d",
            "/filebrowser_dir/filebrowser.db",
        ],
        "ExposedPorts": {
            f"{service_port}/tcp": {},
        },
        "Image": image,
        "HostConfig": {
            "PortBindings": {
                f"{service_port}/tcp": [
                    {
                        "HostIp": f"{service_ip}",
                        "HostPort": f"{service_port}/tcp",
                    },
                ],
            },
            "Mounts": [
                {
                    "Target": "/filebrowser_dir/",
                    "Source": f"{settings_path}",
                    "Type": "bind",
                },
            ],
        },
    }

    for vfolder in vfolders:
        config["HostConfig"]["Mounts"].append(
            {
                "Target": f"/data/{vfolder['name']}",
                "Source": f"{mangle_path(mount_path, vfolder['vfid'])}",
                "Type": "bind",
            },
        )

    container = await docker.containers.create_or_replace(
        config=config,
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
