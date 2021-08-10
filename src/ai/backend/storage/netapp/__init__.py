from __future__ import annotations

import asyncio

from yarl import URL

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

    endpoint: URL
    netapp_admin: str
    netapp_password: str

    async def init(self) -> None:
        self.endpoint = (URL(self.config["netapp_endpoint"]),)
        self.netapp_admin = (str(self.config["netapp_admin"]),)
        self.netapp_password = (str(self.config["netapp_password"]),)
        self.netapp_svm = (str(self.config["netapp_svm"]),)
        self.netapp_volume_name = str(self.config["netapp_volume_name"])

        available = True
        try:
            proc = await asyncio.create_subprocess_exec(
                b"nfsstat",
                b"-m",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError:
            available = False
        else:
            try:
                stdout, stderr = await proc.communicate()
                if b"NFS parameters" not in stdout or proc.returncode != 0:
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
        )

    async def get_quota(self):
        quota_rule = await self.quotaManager.show_quotarule()
        return quota_rule

    async def create_qtree(self, name: str) -> None:
        resp = await self.netappclient.create_qtree(name)

        if "errot" in resp:
            raise ExecutionError("qtree creation was not succesfull")
