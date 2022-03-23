from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import AsyncIterator

import aiodocker
import aiofiles

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.validators import BinarySize

from ..context import Context
from .database import (
    create_connection,
    delete_container_record,
    get_all_containers,
    insert_new_container,
)
from ..utils import mangle_path

log = BraceStyleAdapter(logging.getLogger(__name__))

__all__ = (
    "create_or_update",
    "destroy",
    "cleanup",
)


async def create_or_update(ctx: Context, vfolders: list[str]) -> tuple[str, int, str]:
    """ Create or update new docker image. """
    image = ctx.local_config["filebrowser"]["image"]
    service_ip = ctx.local_config["filebrowser"]["service-ip"]
    service_port = ctx.local_config["filebrowser"]["service_port"]
    max_containers = ctx.local_config["filebrowser"]["max-containers"]
    settings_path = ctx.local_config["filebrowser"]["settings_path"]
    settings_file = ctx.local_config["filebrowser"]["settings_file"]
    mount_path = ctx.local_config["filebrowser"]["mount_path"]
    cpu_count = ctx.local_config["filebrowser"]["max-cpu"]
    memory = ctx.local_config["filebrowser"]["max-mem"]
    db_path = ctx.local_config["filebrowser"]["db-path"]
    memory = int(BinarySize().check_and_return(memory))

    if not settings_path.exists():
        filebrowser_default_settings = {
            "port": service_port,
            "baseURL": "",
            "address": "",
            "log": "stdout",
            "database": "/filebrowser_dir/filebrowser.db",
            "root": "/data/",
        }
        async with aiofiles.open(settings_path / settings_file, mode="w") as file:
            await file.write(json.dumps(filebrowser_default_settings))

    docker = aiodocker.Docker()
    config = {
        "Cmd": [
            "/filebrowser_dir/start.sh",
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
                "CpuCount": cpu_count,
                "Memory": memory,
            },
        )

    container = await docker.containers.create_or_replace(
        config=config,
        name="FileBrowser",
    )
    container_id = container._id
    await container.start()
    await docker.close()

    engine, conn = await create_connection(db_path)
    rows, _ = await get_all_containers(engine, conn)
    rows_list = [row for row in rows]
    if len(rows_list) > max_containers:
        print(
            "Can't create new container. Number of containers exceed the maximum limit.",
        )
        return ["0", 0, "0"]
    await insert_new_container(
        conn,
        container_id,
        service_ip,
        service_port,
        config,
        "RUNNING",
        str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )
    return service_ip, service_port, container_id


async def recreate_container(container_id, config):
    docker = aiodocker.Docker()
    await docker.containers.create_or_replace(
        container_id,
        config=config,
    )
    await docker.close()


async def destroy(ctx: Context, container_id: str) -> None:
    db_path = ctx.local_config["filebrowser"]["db_path"]
    docker = aiodocker.Docker()
    _, conn = await create_connection(db_path)

    for container in await docker.containers.list():
        if container._id == container_id:
            await container.stop()
            await container.delete()
            delete_container_record(conn, container_id)
    await docker.close()


async def cleanup(ctx: Context, interval: float) -> None:
    log.info("filebrowser.cleanup")
    pass


async def _enumerate_containers() -> AsyncIterator[str]:
    pass


async def _check_active_connections(container_id: str) -> bool:
    return True
