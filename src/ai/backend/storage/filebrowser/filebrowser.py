from __future__ import annotations

import logging
from datetime import datetime

import aiodocker
from .config import prepare_filebrowser_app_config

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.validators import BinarySize
from ai.backend.storage.context import Context
from ai.backend.storage.utils import (
    get_available_port,
    is_port_in_use,
    mangle_path,
)

from .database import (
    create_connection,
    delete_container_record,
    insert_new_container,
)

log = BraceStyleAdapter(logging.getLogger(__name__))

__all__ = (
    "create_or_update",
    "destroy_container",
    "get_container_by_id",
    "get_filebrowsers",
    "get_network_stats",
)


async def create_or_update(ctx: Context, vfolders: list[dict]) -> tuple[str, int, str]:
    """Create or update new docker image."""
    image = ctx.local_config["filebrowser"]["image"]
    service_ip = ctx.local_config["filebrowser"]["service-ip"]
    service_port = ctx.local_config["filebrowser"]["service_port"]
    max_containers = ctx.local_config["filebrowser"]["max-containers"]
    settings_path = ctx.local_config["filebrowser"]["settings_path"]
    mount_path = ctx.local_config["filebrowser"]["mount_path"]
    cpu_count = ctx.local_config["filebrowser"]["max-cpu"]
    memory = ctx.local_config["filebrowser"]["max-mem"]
    db_path = ctx.local_config["filebrowser"]["db-path"]
    memory = int(BinarySize().check_and_return(memory))

    if is_port_in_use(service_port):
        service_port = get_available_port()
    running_docker_containers = await get_filebrowsers()
    if len(running_docker_containers) >= max_containers:
        print(
            "Can't create new container. Number of containers exceed the maximum limit.",
        )
        return ("0", 0, "0")

    await prepare_filebrowser_app_config(settings_path, service_port)

    docker = aiodocker.Docker()
    config = {
        "Cmd": [
            "/filebrowser_dir/start.sh",
            f"{service_port}",
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
            "CpuCount": cpu_count,
            "Memory": memory,
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
                "Target": f"/data/{str(vfolder['name'])}",
                "Source": f"{str(mangle_path(mount_path, vfolder['vfid']))}",
                "Type": "bind",
            },
        )
    container_name = f"FileBrowser-{service_port}"
    container = await docker.containers.create_or_replace(
        config=config,
        name=container_name,
    )
    container_id = container._id
    await container.start()
    await docker.close()

    _, conn = await create_connection(db_path)
    await insert_new_container(
        conn,
        container_id,
        container_name,
        service_ip,
        service_port,
        config,
        "RUNNING",
        str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )
    return service_ip, service_port, container_id


async def recreate_container(container_name, config):
    pass
    """
    docker = aiodocker.Docker()
    container = await docker.containers.create_or_replace(
        config=config,
        name=container_name,
    )
    await container.start()
    await docker.close()
    """


async def destroy_container(ctx: Context, container_id: str) -> None:
    db_path = ctx.local_config["filebrowser"]["db-path"]
    docker = aiodocker.Docker()
    _, conn = await create_connection(db_path)
    for container in await docker.containers.list():
        if container._id == container_id:
            try:
                await container.stop()
                await container.delete()
                await delete_container_record(conn, container_id)
            except Exception:
                pass
    await docker.close()


async def get_container_by_id(container_id: str):
    docker = aiodocker.Docker()
    container = aiodocker.docker.DockerContainers(docker).container(
        container_id=container_id,
    )
    await docker.close()
    return container


async def get_filebrowsers():
    docker = aiodocker.Docker()
    container_list = []
    containers = await aiodocker.docker.DockerContainers(docker).list()
    for container in containers:
        stats = await container.stats(stream=False)
        name = stats[0]["name"]
        cnt_id = stats[0]["id"]
        if "FileBrowser" in name:
            container_list.append(cnt_id)
    await docker.close()
    return container_list


async def get_network_stats(container_id):
    docker = aiodocker.Docker()
    container = aiodocker.docker.DockerContainers(docker).container(
        container_id=container_id,
    )
    stats = await container.stats(stream=False)
    await docker.close()
    return (
        stats[0]["networks"]["eth0"]["rx_bytes"],
        stats[0]["networks"]["eth0"]["tx_bytes"],
    )


async def _check_active_connections(container_id: str) -> bool:
    if len(await get_filebrowsers()) > 0:
        return True
    else:
        return False
