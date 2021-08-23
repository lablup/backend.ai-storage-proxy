from __future__ import annotations

import asyncio

from ..exception import ExecutionError
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

    async def init(self) -> None:
        self.endpoint = (str(self.config["netapp_endpoint"]),)
        self.netapp_admin = (str(self.config["netapp_admin"]),)
        self.netapp_password = (str(self.config["netapp_password"]),)
        self.netapp_svm = (self.config["netapp_svm"],)
        self.netapp_volume_name = self.config["netapp_volume_name"]

        available = True

        try:
            proc = await asyncio.create_subprocess_exec(
                b"mount | grep nfs",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError:
            available = False
        else:
            try:
                stdout, stderr = await proc.communicate()
                if b"type nfs" not in stdout or proc.returncode != 0:
                    available = False
            finally:
                await proc.wait()

        if not available:
            raise RuntimeError("NetApp volume is not available or not mounted.")

        self.netappclient = NetAppClient(
            str(self.endpoint),
            self.netapp_admin,
            self.netapp_password,
            str(self.netapp_svm),
            self.netapp_volume_name,
        )

        self.quotaManager = QuotaManager(
            endpoint=str(self.endpoint),
            user=self.netapp_admin,
            password=self.netapp_password,
            svm=str(self.netapp_svm),
            volume_name=self.netapp_volume_name,
        )

    async def get_list_volumes(self):
        resp = await self.netappclient.get_list_volumes()

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_volume_uuid_by_name(self):
        resp = await self.netappclient.get_volume_uuid_by_name(self.netapp_volume_name)
        self.volume_uuid = resp["uuid"]

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_volume_info(self, volume_uuid):
        resp = await self.netappclient.get_volume_info(volume_uuid)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_qtree_info(self, qtree_id):
        resp = await self.netappclient.get_qtree_info(qtree_id)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_qtree_name_by_id(self, qtree_id):
        resp = await self.netappclient.get_volume_info(qtree_id)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_qtree_id_by_name(self, qtree_name):
        resp = await self.netappclient.get_volume_info(qtree_name)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_qtree_path(self, qtree_id):
        resp = await self.netappclient.get_qtree_path(self, qtree_id)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def create_qtree(self, name: str):
        resp = await self.netappclient.create_qtree(name)

        if "error" in resp:
            raise ExecutionError("qtree creation was not succesfull")
        return resp

    async def update_qtree(
        self, qtree_id, qtree_name, security_style, unix_permission, export_policy_name
    ):
        resp = await self.netappclient.update_qtree(
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

    async def delete_qtree(self, qtree_id):
        resp = await self.netappclient.delete_qtree(self, qtree_id)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def list_quotarules(self):
        resp = await self.quotaManager.list_quotarules()

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def list_all_qtrees_with_quotas(self):
        resp = await self.quotaManager.list_all_qtrees_with_quotas(self)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def get_quota(self, rule_uuid):
        resp = await self.quotaManager.get_quota(self, rule_uuid)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp

    async def create_quotarule_qtree(
        self,
        qtree_name,
        space_hard_limit,
        space_soft_limit,
        files_hard_limit,
        files_soft_limit,
    ):
        resp = await self.quotaManager.create_quotarule_qtree(
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
        qtree_name,
        space_hard_limit,
        space_soft_limit,
        files_hard_limit,
        files_soft_limit,
    ):
        resp = await self.quotaManager.update_quotarule_qtree(
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

    async def delete_quotarule_qtree(self, rule_uuid):
        resp = await self.quotaManager.update_quotarule_qtree(rule_uuid)

        if "error" in resp:
            raise ExecutionError("api error")
        return resp
