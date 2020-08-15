from __future__ import annotations

from contextlib import asynccontextmanager as actxmgr
from pathlib import Path, PurePosixPath
from typing import (
    Any,
    AsyncIterator,
    Mapping,
    Type,
)

from ai.backend.common.etcd import AsyncEtcd

from .abc import AbstractVolume
from .exception import InvalidVolumeError
from .vfs import BaseVolume
from .purestorage import FlashBladeVolume


BACKENDS: Mapping[str, Type[AbstractVolume]] = {
    'purestorage': FlashBladeVolume,
    'vfs': BaseVolume,
}


class Context:

    __slots__ = ('pid', 'etcd', 'local_config')

    pid: int
    etcd: AsyncEtcd
    local_config: Mapping[str, Any]

    def __init__(self, pid: int, local_config: Mapping[str, Any], etcd: AsyncEtcd) -> None:
        self.pid = pid
        self.etcd = etcd
        self.local_config = local_config

    @actxmgr
    async def get_volume(self, host: str) -> AsyncIterator[AbstractVolume]:
        try:
            storage_config = self.local_config['storage'][host]
        except KeyError:
            raise InvalidVolumeError(host)
        host_cls: Type[AbstractVolume] = BACKENDS[storage_config['backend']]
        host_obj = host_cls(
            mount_path=Path(storage_config['path']),
            fsprefix=PurePosixPath(storage_config['fsprefix']),
            options=storage_config['options'] or {},
        )
        await host_obj.init()
        try:
            yield host_obj
        finally:
            await host_obj.shutdown()
