
import asyncio
import os
from pathlib import Path, PurePath
from typing import Any, FrozenSet, Mapping
from uuid import UUID

from .exceptions import WekaInitError, WekaNotFoundError
from .weka_client import WekaAPIClient

from ai.backend.common.types import BinarySize, HardwareMetadata
from ai.backend.storage.abc import CAP_METRIC, CAP_QUOTA, CAP_VFOLDER
from ai.backend.storage.types import FSUsage, FSPerfMetric
from ai.backend.storage.vfs import BaseVolume


class WekaVolume(BaseVolume):

    api_client: WekaAPIClient

    _fs_uid: str

    def __init__(
        self,
        local_config: Mapping[str, Any],
        mount_path: Path,
        *,
        fsprefix: PurePath = None,
        options: Mapping[str, Any] = None
    ) -> None:
        super().__init__(local_config, mount_path, fsprefix=fsprefix, options=options)
        self.api_client = WekaAPIClient(
            self.config['weka_endpoint'],
            self.config['weka_username'],
            self.config['weka_password'],
            self.config['weka_organization'],
        )

    async def init(self) -> None:
        await super().init()
        for fs in await self.api_client.list_fs():
            if fs.name == self.config['weka_fs_name']:
                self._fs_uid = fs.uid
                return
        else:
            raise WekaInitError(f'FileSystem {fs.name} not found')

    async def _get_inode_id(self, path: Path) -> int:
        return await asyncio.get_running_loop().run_in_executor(None, lambda: os.stat(path).st_ino)

    async def get_capabilities(self) -> FrozenSet[str]:
        return frozenset([CAP_VFOLDER, CAP_QUOTA, CAP_METRIC])

    async def get_hwinfo(self) -> HardwareMetadata:
        raise NotImplementedError  # TODO: Implement

    async def get_fs_usage(self) -> FSUsage:
        assert self._fs_uid is not None
        fs = await self.api_client.get_fs(self._fs_uid)
        return FSUsage(
            fs.total_budget,
            fs.used_total,
        )

    async def get_performance_metric(self) -> FSPerfMetric:
        raise NotImplementedError  # TODO: Find out way to collect per-fs metric

    async def delete_vfolder(self, vfid: UUID) -> None:
        assert self._fs_uid is not None
        vfpath = self.mangle_vfpath(vfid)
        inode_id = await self._get_inode_id(vfpath)
        try:
            await self.api_client.remove_quota(self._fs_uid, inode_id)
        except WekaNotFoundError:
            pass
        return await super().delete_vfolder(vfid)

    async def get_quota(self, vfid: UUID) -> BinarySize:
        assert self._fs_uid is not None
        vfpath = self.mangle_vfpath(vfid)
        inode_id = await self._get_inode_id(vfpath)
        quota = await self.api_client.get_quota(self._fs_uid, inode_id)
        return quota.hard_limit

    async def set_quota(self, vfid: UUID, size_bytes: BinarySize) -> None:
        assert self._fs_uid is not None
        vfpath = self.mangle_vfpath(vfid)
        inode_id = await self._get_inode_id(vfpath)
        weka_path = vfpath.absolute().as_posix().replace(self.mount_path, '')
        if not weka_path.startswith('/'):
            weka_path = '/' + weka_path
        await self.api_client.set_quota_v1(weka_path, inode_id, hard_limit=size_bytes)
