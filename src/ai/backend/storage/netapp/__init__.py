from __future__ import annotations

import asyncio
from pathlib import Path
from typing import FrozenSet
from uuid import UUID

from ai.backend.common.types import HardwareMetadata

from ..abc import CAP_METRIC, CAP_VFOLDER
from ..exception import ExecutionError
from ..types import FSPerfMetric, FSUsage, VFolderCreationOptions
from ..vfs import BaseVolume
from .netappclient import NetAppClient
from .quotamanager import QuotaManager


async def read_file(loop: asyncio.AbstractEventLoop, filename: str) -> str:
    def _read():
        with open(filename, "r") as fr:
            return fr.read()

    return await loop.run_in_executor(None, lambda: _read())


async def write_file(
    loop: asyncio.AbstractEventLoop, filename: str, contents: str, perm="w"
):
    def _write():
        with open(filename, perm) as fw:
            fw.write(contents)

    await loop.run_in_executor(None, lambda: _write())


async def run(cmd: str) -> str:
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
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
        # Temporaily comment out volume mount checking
        # available = True
        # try:
        #     proc = await asyncio.create_subprocess_exec(
        #         b"mount",
        #         stdout=asyncio.subprocess.PIPE,
        #         stderr=asyncio.subprocess.STDOUT,
        #     )

        # except FileNotFoundError:
        #     available = False
        # else:
        #     try:
        #         stdout, stderr = await proc.communicate()
        #         if b"type nfs" not in stdout or proc.returncode != 0:
        #             available = False
        #     finally:
        #         await proc.wait()
        # if not available:
        #     raise RuntimeError("NetApp volumes are not mounted or not supported.")

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
        self.netapp_qtree_name = self.config["netapp_qtree_name"]
        self.netapp_qtree_id = await self.get_qtree_id_by_name(self.netapp_qtree_name)

        # adjust mount path (volume + qtree)
        self.mount_path = (self.mount_path / Path(self.netapp_qtree_name)).resolve()

    async def get_capabilities(self) -> FrozenSet[str]:
        return frozenset([CAP_VFOLDER, CAP_METRIC])

    async def get_hwinfo(self) -> HardwareMetadata:
        metadata = await self.netapp_client.get_metadata()
        return {"status": "healthy", "status_info": None, "metadata": {**metadata}}

    async def get_fs_usage(self) -> FSUsage:
        volume_usage = await self.netapp_client.get_usage()
        qtree_info = await self.get_default_qtree_by_volume_id(self.netapp_volume_uuid)
        self.netapp_qtree_name = qtree_info["name"]
        quota = await self.quota_manager.get_quota_by_qtree_name(self.netapp_qtree_name)
        space = quota.get("space")
        if space:
            capacity_bytes = space["hard_limit"]
        else:
            capacity_bytes = volume_usage["capacity_bytes"]
        return FSUsage(
            capacity_bytes=capacity_bytes, used_bytes=volume_usage["used_bytes"]
        )

    async def get_performance_metric(self) -> FSPerfMetric:
        uuid = await self.get_volume_uuid_by_name()
        volume_info = await self.get_volume_info(uuid)
        # if volume_info is None:
        #     raise RuntimeError(
        #         "no metric found for the configured netapp ontap filesystem"
        #     )
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
        self, vfid: UUID, options: VFolderCreationOptions = None
    ) -> None:
        vfpath = self.mangle_vfpath(vfid)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda: vfpath.mkdir(0o755, parents=True, exist_ok=False)
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

    async def get_qtree_info(self, qtree_id):
        resp = await self.netapp_client.get_qtree_info(qtree_id)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_qtree_name_by_id(self, qtree_id):
        resp = await self.netapp_client.get_qtree_name_by_id(qtree_id)

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

    async def get_qtree_path(self, qtree_id):
        resp = await self.netapp_client.get_qtree_path(self, qtree_id)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    # For now, Only Read / Update operation for qtree is available
    # in NetApp ONTAP Plugin of Backend.AI
    async def create_qtree(self, name: str):
        resp = await self.netapp_client.create_qtree(name)

        if "error" in resp:
            raise ExecutionError("qtree creation was not succesfull")
        return resp

    async def update_qtree(
        self, qtree_id, qtree_name, security_style, unix_permission, export_policy_name
    ):
        resp = await self.netapp_client.update_qtree(
            self,
            qtree_id,
            qtree_name,
            security_style,
            unix_permission,
            export_policy_name,
        )

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    # For now, Only Read / Update operation for qtree is available
    # in NetApp ONTAP Plugin of Backend.AI
    async def delete_qtree(self, qtree_id):
        resp = await self.netapp_client.delete_qtree(self, qtree_id)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def list_quotarules(self):
        resp = await self.quota_manager.list_quotarules()

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def list_all_qtrees_with_quotas(self):
        resp = await self.quota_manager.list_all_qtrees_with_quotas(self)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_quota(self) -> Mapping[str, Any]:
        qtree = await self.get_default_qtree_by_volume_id(self.netapp_volume_uuid)
        resp = await self.quota_manager.get_quota(qtree["name"])

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def update_quota(self, quota):
        qtree = await self.get_default_qtree_by_volume_id(self.netapp_volume_uuid)
        resp = await self.quota_manager.get_quota(qtree["name"])
        rule_uuid = resp["uuid"]

        await self.update_quotarule_qtree(
            quota["space"]["hard_limit"],
            quota["space"]["soft_limit"],
            quota["files"]["hard_limit"],
            quota["files"]["soft_limit"],
            rule_uuid,
        )

        if "error" in resp:
            raise ExecutionError("api error")

    async def get_quota_by_rule(self, rule_uuid):
        resp = await self.quota_manager.get_quota_by_rule(self, rule_uuid)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    # For now, Only Read / Update operation for qtree is available
    # in NetApp ONTAP Plugin of Backend.AI
    async def create_quotarule_qtree(
        self,
        qtree_name,
        space_hard_limit,
        space_soft_limit,
        files_hard_limit,
        files_soft_limit,
    ):
        resp = await self.quota_manager.create_quotarule_qtree(
            self,
            qtree_name,
            space_hard_limit,
            space_soft_limit,
            files_hard_limit,
            files_soft_limit,
        )

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def update_quotarule_qtree(
        self,
        # qtree_name,
        space_hard_limit,
        space_soft_limit,
        files_hard_limit,
        files_soft_limit,
        rule_uuid,
    ):
        resp = await self.quota_manager.update_quotarule_qtree(
            # qtree_name,
            space_hard_limit,
            space_soft_limit,
            files_hard_limit,
            files_soft_limit,
            rule_uuid,
        )
        if "error" in resp:
            raise ExecutionError("api error")

    # For now, Only Read / Update operation for qtree is available
    # in NetApp ONTAP Plugin of Backend.AI
    async def delete_quotarule_qtree(self, rule_uuid):
        resp = await self.quota_manager.update_quotarule_qtree(rule_uuid)
        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_qtree_config(self):
        qtree_metadata = await self.netapp_client.get_default_qtree_by_volume_id(
            self.netapp_volume_uuid
        )
        resp = await self.netapp_client.get_qtree_config(qtree_metadata.get("id"))
        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def update_qtree_config(self, raw_config):
        qtree_metadata = await self.get_default_qtree_by_volume_id(
            self.netapp_volume_uuid
        )
        config = {
            "name": raw_config["input"]["name"],
            "security_style": raw_config["input"]["security_style"],
        }
        config.update({"id": qtree_metadata.get("id")})
        resp = await self.netapp_client.update_qtree_config(config)

        # adjust mount path (volume + qtree) according to qtree_name
        self.netapp_qtree_name = config["name"]
        self.mount_path = (
            self.mount_path.parent / Path(self.netapp_qtree_name)
        ).resolve()
        if "error" in resp:
            raise ExecutionError("api error")

    async def get_qos(self, qos_name):
        resp = await self.netapp_client.get_qos_by_qos_name(qos_name)
        if resp and "error" in resp:
            raise ExecutionError
        return resp

    async def create_qos(self, qos):
        resp = await self.netapp_client.create_qos(qos)
        # if successfully created when resp will be an empty dict
        if resp and "error" in resp:
            raise ExecutionError("api error")

    async def update_qos(self, qos):
        resp = await self.netapp_client.update_qos(qos)
        # if successfully created when resp will be an empty dict
        if resp and "error" in resp:
            raise ExecutionError("api error")

    async def delete_qos(self, qos_list):
        qos_names = qos_list["input"]["name_list"]
        for name in qos_names:
            resp = await self.netapp_client.delete_qos(name)
            # if successfully created when resp will be an empty dict
            if resp and "error" in resp:
                raise ExecutionError("api error")

    async def update_volume_config(self, config):
        resp = await self.netapp_client.update_volume_config(config)
        # if successfully created when resp will be an empty dict
        if resp and "error" in resp:
            raise ExecutionError("api error")
