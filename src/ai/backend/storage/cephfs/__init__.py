import asyncio

from typing import Dict, List
from uuid import UUID

from ai.backend.common.types import BinarySize

from ..exception import (
    ExecutionError
)
from ..types import FSUsage, VFolderCreationOptions
from ..vfs import BaseVolume


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


class CephFSVolume(BaseVolume):
    loop: asyncio.AbstractEventLoop
    registry: Dict[str, int]
    project_id_pool: List[int]

    async def init(self) -> None:
        available = True
        try:
            proc = await asyncio.create_subprocess_exec(
                b"ceph",
                b"--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError:
            available = False
        else:
            try:
                stdout, stderr = await proc.communicate()
                if b"Ceph" not in stdout or proc.returncode != 0:
                    available = False
            finally:
                await proc.wait()
        if not available:
            raise RuntimeError(
                "Ceph is not installed. "
                "You cannot use the CephFS backend for the storage proxy."
                             )

    # ----- volume opeartions -----
    async def create_vfolder(self, vfid: UUID, options: VFolderCreationOptions = None) -> None:

        vfpath = self.mangle_vfpath(vfid)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda: vfpath.mkdir(0o755, parents=True, exist_ok=False)
        )

    async def get_fs_usage(self) -> FSUsage:
        stat = await run(f"df -h {self.mount_path} | grep {self.mount_path}")
        if len(stat.split()) != 6:
            raise ExecutionError("'df -h' stdout is in an unexpected format")
        _, capacity, used, _, _, path = stat.split()
        if str(self.mount_path) != path:
            raise ExecutionError("'df -h' stdout is in an unexpected format")
        return FSUsage(
            capacity_bytes=BinarySize.finite_from_str(capacity),
            used_bytes=BinarySize.finite_from_str(used),
        )

    async def get_quota(self, vfid: UUID) -> BinarySize:
        report = await run(
            f"getfattr -n ceph.quota.max_bytes {self.mount_path}"
            f" | grep {str(vfid)[:-5]}"
        )
        if len(report.split()) != 6:
            raise ExecutionError("ceph quota report output is in unexpected format")
        proj_name, _, _, quota, _, _ = report.split()
        if not str(vfid).startswith(proj_name):
            raise ExecutionError("vfid and project name does not match")
        return BinarySize.finite_from_str(quota)

    async def set_quota(self, vfid: UUID, size_bytes: BinarySize) -> None:
        await run(
            f'setfattr -n ceph.quota.max_bytes -v {int(size_bytes)} {vfid}'
        )
