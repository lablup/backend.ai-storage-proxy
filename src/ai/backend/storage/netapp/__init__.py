from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import FrozenSet
from uuid import UUID

from ai.backend.common.types import BinarySize, HardwareMetadata

from ..abc import CAP_METRIC, CAP_VFHOST_QUOTA, CAP_VFOLDER
from ..exception import ExecutionError
from ..types import FSPerfMetric, FSUsage, VFolderCreationOptions
from ..vfs import BaseVolume
from .netappclient import NetAppClient
from .quotamanager import QuotaManager


async def run(cmd: str) -> str:
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    if err:
        raise ExecutionError(err.decode())
    return out.decode()


class NetAppVolume(BaseVolume):

    endpoint: str
    netapp_admin: str
    netapp_password: str
    netapp_svm: str
    netapp_volume_name: str
    netapp_volume_uuid: str
    netapp_qtree_name: str
    netapp_qtree_id: str

    async def init(self) -> None:

        self.endpoint = self.config["netapp_endpoint"]
        self.netapp_admin = self.config["netapp_admin"]
        self.netapp_password = str(self.config["netapp_password"])
        self.netapp_svm = self.config["netapp_svm"]
        self.netapp_volume_name = self.config["netapp_volume_name"]

        self.netapp_client = NetAppClient(
            str(self.endpoint),
            self.netapp_admin,
            self.netapp_password,
            str(self.netapp_svm),
            self.netapp_volume_name,
        )

        self.quota_manager = QuotaManager(
            endpoint=str(self.endpoint),
            user=self.netapp_admin,
            password=self.netapp_password,
            svm=str(self.netapp_svm),
            volume_name=self.netapp_volume_name,
        )

        # assign qtree info after netapp_client and quotamanager are initiated
        self.netapp_volume_uuid = await self.netapp_client.get_volume_uuid_by_name()
        default_qtree = await self.get_default_qtree_by_volume_id(
            self.netapp_volume_uuid,
        )
        self.netapp_qtree_name = default_qtree.get(
            "name",
            self.config["netapp_qtree_name"],
        )
        self.netapp_qtree_id = await self.get_qtree_id_by_name(self.netapp_qtree_name)

        # adjust mount path (volume + qtree)
        self.mount_path = (self.mount_path / Path(self.netapp_qtree_name)).resolve()

    async def get_capabilities(self) -> FrozenSet[str]:
        return frozenset([CAP_VFOLDER, CAP_VFHOST_QUOTA, CAP_METRIC])

    async def get_hwinfo(self) -> HardwareMetadata:
        raw_metadata = await self.netapp_client.get_metadata()
        qtree_info = await self.get_default_qtree_by_volume_id(self.netapp_volume_uuid)
        self.netapp_qtree_name = qtree_info["name"]
        quota = await self.quota_manager.get_quota_by_qtree_name(self.netapp_qtree_name)
        # add quota in hwinfo
        metadata = {"quota": json.dumps(quota), **raw_metadata}
        return {"status": "healthy", "status_info": None, "metadata": {**metadata}}

    async def get_fs_usage(self) -> FSUsage:
        volume_usage = await self.netapp_client.get_usage()
        qtree_info = await self.get_default_qtree_by_volume_id(self.netapp_volume_uuid)
        self.netapp_qtree_name = qtree_info["name"]
        quota = await self.quota_manager.get_quota_by_qtree_name(self.netapp_qtree_name)
        space = quota.get("space")
        if space and space.get("hard_limit"):
            capacity_bytes = space["hard_limit"]
        else:
            capacity_bytes = volume_usage["capacity_bytes"]
        return FSUsage(
            capacity_bytes=capacity_bytes,
            used_bytes=volume_usage["used_bytes"],
        )

    async def get_performance_metric(self) -> FSPerfMetric:
        uuid = await self.get_volume_uuid_by_name()
        volume_info = await self.get_volume_info(uuid)
        metric = volume_info["metric"]
        return FSPerfMetric(
            iops_read=metric["iops"]["read"],
            iops_write=metric["iops"]["write"],
            io_bytes_read=metric["throughput"]["read"],
            io_bytes_write=metric["throughput"]["write"],
            io_usec_read=metric["latency"]["read"],
            io_usec_write=metric["latency"]["write"],
        )

    async def create_vfolder(
        self,
        vfid: UUID,
        options: VFolderCreationOptions = None,
    ) -> None:
        vfpath = self.mangle_vfpath(vfid)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: vfpath.mkdir(0o755, parents=True, exist_ok=False),
        )

    async def shutdown(self) -> None:
        await self.netapp_client.aclose()
        await self.quota_manager.aclose()

    # ------ volume operations ------
    async def get_list_volumes(self):
        resp = await self.netapp_client.get_list_volumes()

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_volume_uuid_by_name(self):
        resp = await self.netapp_client.get_volume_uuid_by_name()

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_volume_info(self, volume_uuid):
        resp = await self.netapp_client.get_volume_info(volume_uuid)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    # ------ qtree and quotas operations ------
    async def get_default_qtree_by_volume_id(self, volume_uuid):
        volume_uuid = volume_uuid if volume_uuid else self.netapp_volume_uuid
        resp = await self.netapp_client.get_default_qtree_by_volume_id(volume_uuid)
        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_qtree_id_by_name(self, qtree_name):
        qtree_name = (
            qtree_name if qtree_name else await self.get_default_qtree_by_volume_id()
        )
        resp = await self.netapp_client.get_qtree_id_by_name(qtree_name)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_quota(self, vfid: UUID) -> BinarySize:
        raise NotImplementedError

    async def set_quota(self, vfid: UUID, size_bytes: BinarySize) -> None:
        raise NotImplementedError
