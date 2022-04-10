from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Mapping, Type
from uuid import UUID

import aiodocker
import pkg_resources

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.validators import BinarySize
from ai.backend.storage.abc import AbstractVolume
from ai.backend.storage.context import Context
from ai.backend.storage.utils import get_available_port
from ai.backend.storage.vfs import BaseVolume

from .config_browser_app import prepare_filebrowser_app_config
from .database import FilebrowserTrackerDB

BACKENDS: Mapping[str, Type[AbstractVolume]] = {
    "vfs": BaseVolume,
}


log = BraceStyleAdapter(logging.getLogger(__name__))

__all__ = (
    "create_or_update",
    "destroy_container",
    "get_container_by_id",
    "get_filebrowsers",
    "get_network_stats",
)


@asynccontextmanager
async def closing_async(thing):
    try:
        yield thing
    finally:
        await thing.close()


async def create_or_update(ctx: Context, vfolders: list[dict]) -> tuple[str, int, str]:
    image = ctx.local_config["filebrowser"]["image"]
    service_ip = ctx.local_config["filebrowser"]["service_ip"]
    service_port = ctx.local_config["filebrowser"]["service_port"]
    max_containers = ctx.local_config["filebrowser"]["max_containers"]
    cgroup = ctx.local_config["filebrowser"]["cgroup"]
    cpu_count = ctx.local_config["filebrowser"]["max_cpu"]
    memory = ctx.local_config["filebrowser"]["max_mem"]
    memory = int(BinarySize().check_and_return(memory))
    db_path = ctx.local_config["filebrowser"]["db_path"]
    mount_path = Path(ctx.local_config["filebrowser"]["mount_path"])
    p = Path(pkg_resources.resource_filename(__name__, ""))
    storage_proxy_root_path_index = p.parts.index("storage-proxy")
    settings_path = (
        Path(*p.parts[0 : storage_proxy_root_path_index + 1]) / "config/filebrowser/"
    )
    volume_cls: Type[AbstractVolume] = BACKENDS["vfs"]
    volume_obj = volume_cls(
        local_config=ctx.local_config,
        mount_path=Path(mount_path),
        fsprefix=None,
        options={},
    )
    port_range = ctx.local_config["filebrowser"]["port_range"].split("-")
    service_port = get_available_port(port_range)
    running_docker_containers = await get_filebrowsers()
    if len(running_docker_containers) >= max_containers:
        print(
            "Can't create new container. Number of containers exceed the maximum limit.",
        )
        return ("0", 0, "0")
    await prepare_filebrowser_app_config(settings_path, service_port)
    async with closing_async(aiodocker.Docker()) as docker:
        config = {
            "Cmd": [
                "/filebrowser_dir/start.sh",
                f"{service_port}",
            ],
            "ExposedPorts": {
                "8080/tcp": {},
            },
            "Image": image,
            "HostConfig": {
                "PortBindings": {
                    "8080/tcp": [
                        {
                            "HostIp": f"{service_ip}",
                            "HostPort": f"{service_port}/tcp",
                        },
                    ],
                },
                "CpuCount": cpu_count,
                "Memory": memory,
                "Cgroup": cgroup,
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
            filebrowser_mount_path = str(
                volume_obj.mangle_vfpath(UUID(vfolder["vfid"])),
            )
            config["HostConfig"]["Mounts"].append(
                {
                    "Target": f"/data/{str(vfolder['name'])}",
                    "Source": filebrowser_mount_path,
                    "Type": "bind",
                },
            )
        container_name = f"ai.backend.container-filebrowser-{service_port}"
        container = await docker.containers.create_or_replace(
            config=config,
            name=container_name,
        )
        container_id = container._id
        await container.start()
    tracker_db = FilebrowserTrackerDB(db_path)
    await tracker_db.insert_new_container(
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
    async with closing_async(aiodocker.Docker()) as docker:
        try:
            docker = aiodocker.Docker()
            container = await docker.containers.create_or_replace(
                config=config,
                name=container_name,
            )
            await container.start()
        except Exception as e:
            print("Failure to recreate container ", e)


async def destroy_container(ctx: Context, container_id: str) -> None:
    db_path = ctx.local_config["filebrowser"]["db_path"]
    tracker_db = FilebrowserTrackerDB(db_path)
    async with closing_async(aiodocker.Docker()) as docker:
        for container in await docker.containers.list():
            if container._id == container_id:
                try:
                    await container.stop()
                    await container.delete()
                    await tracker_db.delete_container_record(container_id)
                except Exception as e:
                    print(f"Failure to destroy container {container_id[0:7]} ", e)
                else:
                    break


async def get_container_by_id(container_id: str):
    async with closing_async(aiodocker.Docker()) as docker:
        container = aiodocker.docker.DockerContainers(docker).container(
            container_id=container_id,
        )
    return container


async def get_filebrowsers():
    container_list = []
    async with closing_async(aiodocker.Docker()) as docker:
        containers = await aiodocker.docker.DockerContainers(docker).list()
        for container in containers:
            stats = await container.stats(stream=False)
            name = stats[0]["name"]
            cnt_id = stats[0]["id"]
            if "ai.backend.container-filebrowser" in name:
                container_list.append(cnt_id)
    return container_list


async def get_network_stats(container_id):
    async with closing_async(aiodocker.Docker()) as docker:
        container = aiodocker.docker.DockerContainers(docker).container(
            container_id=container_id,
        )
        stats = await container.stats(stream=False)
    return (
        stats[0]["networks"]["eth0"]["rx_bytes"],
        stats[0]["networks"]["eth0"]["tx_bytes"],
    )


async def _check_active_connections() -> bool:
    if len(await get_filebrowsers()) > 0:
        return True
    return False
