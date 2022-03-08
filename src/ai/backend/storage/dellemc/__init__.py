from __future__ import annotations

import asyncio
# from contextlib import aclosing
import glob
import json
from multiprocessing.connection import Client
import os
import time
from pathlib import Path, PurePosixPath
from typing import FrozenSet
from uuid import UUID

import aiofiles

from ai.backend.common.types import BinarySize, HardwareMetadata

from ..abc import CAP_METRIC, CAP_VFHOST_QUOTA, CAP_VFOLDER, AbstractVolume
from ..exception import ExecutionError, StorageProxyError, VFolderCreationError
from ..types import FSPerfMetric, FSUsage, VFolderCreationOptions, VFolderUsage
from ..vfs import BaseVolume
from .dellemc_client import DellEMCClient
from .dellemc_quota_manager import QuotaManager


class DellEMCVolume(BaseVolume):

    endpoint: str
    dell_admin: str
    dell_password: str

    async def init(self) -> None:

        self.endpoint = self.config["dell_endpoint"]
        self.dell_admin = self.config["dell_admin"]
        self.dell_password = str(self.config["dell_password"])
        self.dell_api_version = self.config["dell_api_version"]

        self.dellEMC_client = DellEMCClient(
            str(self.endpoint),
            self.dell_admin,
            self.dell_password,
            api_version=self.dell_api_version,
        )

        self.quota_manager = QuotaManager(
            str(self.endpoint),
            self.dell_admin,
            self.dell_password,
            api_version=self.dell_api_version,
        )

    async def shutdown(self) -> None:
        await self.dellEMC_client.aclose()
        await self.quota_manager.aclose()

    async def get_capabilities(self) -> FrozenSet[str]:
        return frozenset([CAP_VFOLDER, CAP_VFHOST_QUOTA, CAP_METRIC])

    async def get_hwinfo(self) -> HardwareMetadata:
        raw_metadata = await self.dellEMC_client.get_metadata()
        quotas = await self.quota_manager.list_all_quota()
        metadata = {"quotas": json.dumps(quotas), **raw_metadata}
        return {"status": "healthy", "status_info": None, "metadata": {**metadata}}

    async def get_fs_usage(self) -> FSUsage:
        usage = await self.dellEMC_client.get_usage()
        return FSUsage(
            capacity_bytes=usage["capacity_bytes"],
            used_bytes=usage["used_bytes"],
        )

    async def get_quota_id(self):
        quotas = await self.quota_manager.list_all_quota()
        quota_id = []
        for quota in quotas:
            quota_id.append(quota["id"])
        return quota_id

    async def get_quota(self, vfid: UUID) -> BinarySize:
        quota = await self.dellEMC_client.list_all_quota()
        return quota[0]["usage"]

    async def set_quota(self, vfid: UUID, size_bytes: BinarySize) -> None:
        msg = await self.dellEMC_client.create_quota()
        return msg

    async def get_drive_stats(self):
        resp = await self.dellEMC_client.get_drive_stats()

        if "errors" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_system_stats(self):
        resp = await self.dellEMC_client.get_system_stats()

        if "errors" in resp:
            raise ExecutionError("api error")
        return resp
    
    async def get_workload_stats(self):
        resp = await self.dellEMC_client.get_workload_stats()

        if "errors" in resp:
            raise ExecutionError("api error")
        return resp
    
    def sum_of_stats(self, param: str, stats: list):
        return sum(stat.get(param) for stat in stats)

    async def get_performance_metric(self) -> FSPerfMetric:
        # drive_stats = await self.get_drive_stats()
        # system_stats = await self.get_system_stats()
        # return FSPerfMetric(
        #     iops_read=sum(drive["bytes_in"] for drive in drive_stats),
        #     iops_write=sum(drive["bytes_out"] for drive in drive_stats),
        #     io_bytes_read=system_stats["disk_in"],
        #     io_bytes_write=system_stats["disk_out"],
        #     io_usec_read=sum(drive["xfers_in"] for drive in drive_stats),
        #     io_usec_write=sum(drive["xfers_out"] for drive in drive_stats),
        # )
        workload_stats = await self.get_workload_stats()
        return FSPerfMetric(
            iops_read=self.sum_of_stats("bytes_in", workload_stats),
            iops_write=self.sum_of_stats("bytes_out", workload_stats),
            io_bytes_read=self.sum_of_stats("reads", workload_stats),
            io_bytes_write=self.sum_of_stats("writes", workload_stats),
            io_usec_read=self.sum_of_stats("latency_read", workload_stats),
            io_usec_write=self.sum_of_stats("latency_write", workload_stats),
        )

    async def create_quota(self, path, type):
        # path would be start like '/ifs'
        # type should be selected from the below string.
        # "directory",
        # "user",
        # "group",
        # "default-directory",
        # "default-user",
        # "default-group"
        quota_id = self.quota_manager.create_quota(path, type)
        return quota_id
    '''
    async def get_usage(
        self,
        vfid: UUID,
        relpath: PurePosixPath = None,
    ) -> VFolderUsage:
        target_path = self.sanitize_vfpath(vfid, relpath)
        total_size = 0
        total_count = 0
        raw_target_path = bytes(target_path)
    '''