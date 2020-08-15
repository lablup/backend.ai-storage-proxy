from __future__ import annotations

from contextlib import asynccontextmanager as actxmgr
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Mapping,
    Type,
)

from ai.backend.common.etcd import AsyncEtcd

from .abc import AbstractVFolderHost
from .vfs import BaseVFolderHost
from .purestorage import FlashBladeVFolderHost


BACKENDS: Mapping[str, Type[AbstractVFolderHost]] = {
    'purestorage': FlashBladeVFolderHost,
    'vfs': BaseVFolderHost,
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
    async def get_vfhost(self, host: str) -> AsyncIterator[AbstractVFolderHost]:
        storage_config = self.local_config['storage'][host]
        host_cls: Type[AbstractVFolderHost] = BACKENDS[storage_config['backend']]
        host_obj = host_cls(
            mount_path=Path(storage_config['path']),
            options=storage_config['options'] or {},
        )
        await host_obj.init()
        try:
            yield host_obj
        finally:
            await host_obj.shutdown()
