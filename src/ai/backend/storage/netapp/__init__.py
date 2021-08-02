from __future__ import annotations

import asyncio

from ..exception import ExecutionError
from ..vfs import BaseVolume
from .netappclient import NetAppClient


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
    async def init(self) -> None:

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

        self.netappclient_client = NetAppClient(
            self.config["netapp_endpoint"],
            self.config["netapp_admin"],
            self.config["netapp_password"],
        )
