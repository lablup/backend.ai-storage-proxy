from __future__ import annotations

from contextlib import asynccontextmanager as actxmgr
from pathblib import Path
from typing import (
    AsyncIterator,
    Mapping,
    Type,
)

from .abc import AbstractVFolderHost
from .vfs import BaseVFolderHost
from .purestorage import FlashBladeVFolderHost

import attr


BACKENDS: Mapping[str, Type[AbstractVFolderHost]] = {
    'purestorage': FlashBladeVFolderHost,
    'vfs': BaseVFolderHost,
}


@attr.s(auto_attribs=True, slots=True)
class Context:
    pid: int

    @actxmgr
    async def get_vfhost(self, host: str) -> AsyncIterator[AbstractVFolderHost]:
        test_config = {
            'local': {
                'backend': 'vfs',
                'mount': '/mnt/test',
            },
            'pure_nfs3': {
                'backend': 'purestorage',
                'mount': '/mnt/pure_nfs3',
            }
        }
        host_cls: Type[AbstractVFolderHost] = BACKENDS[test_config[host]['backend']]
        host_obj = host_cls(Path(test_config[host]['mount']))
        await host_obj.init()
        try:
            yield host_obj
        finally:
            await host_obj.shutdown()
