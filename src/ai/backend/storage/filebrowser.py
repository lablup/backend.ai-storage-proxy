from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import AsyncIterator

import aiodocker
import aiofiles
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    inspect,
)

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.validators import BinarySize

from .context import Context
from .utils import mangle_path
from pathlib import Path

log = BraceStyleAdapter(logging.getLogger(__name__))

__all__ = (
    "create_or_update",
    "destroy",
    "cleanup",
)


async def create_or_update(ctx: Context, vfolders: list[str]) -> tuple[str, int, str]:

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

    engine = create_engine(f"sqlite:///{str(db_path)}" , echo=True)
    conn = engine.connect()
    insp = inspect(engine)
    meta = MetaData()
    containers = Table(
        "containers",
        meta,
        Column("container_id", String, primary_key=True),
        Column("service_ip", String),
        Column("service_port", Integer),
        Column("config", Text),
        Column("status", String),
        Column("timestamp", String),
    )

    if "containers" not in insp.get_table_names():
        meta.create_all(engine)
    rows = conn.execute(containers.select())

    rows_list = [row for row in rows]
    if len(rows_list) > max_containers:
        print(
            "Can't create new container. Number of containers exceed the maximum limit.",
        )
        return ["0", 0, "0"]

    ins = containers.insert().values(
        container_id=container_id,
        service_ip=service_ip,
        service_port=int(service_port),
        config=str(config),
        status="RUNNING",
        timestamp=str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )
    conn.execute(ins)
    return service_ip, service_port, container_id


async def destroy(ctx: Context, container_id: str) -> None:
    db_path = ctx.local_config["filebrowser"]["db_path"]
    docker = aiodocker.Docker()
    engine = create_engine("sqlite:///" / db_path, echo=True)
    conn = engine.connect()

    meta = MetaData()
    containers = Table(
        "containers",
        meta,
        Column("container_id", String, primary_key=True),
        Column("service_ip", String),
        Column("service_port", Integer),
        Column("config", Text),
        Column("status", String),
        Column("timestamp", String),
    )

    for container in await docker.containers.list():
        if container._id == container_id:
            await container.stop()
            await container.delete()
            del_sql = containers.delete().where(
                containers.c.container_id == container_id,
            )
            conn.execute(del_sql)
    await docker.close()


async def cleanup(ctx: Context, interval: float) -> None:
    log.info("filebrowser.cleanup")
    pass


async def _enumerate_containers() -> AsyncIterator[str]:
    pass


async def _check_active_connections(container_id: str) -> bool:
    return True
