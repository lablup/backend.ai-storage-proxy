import asyncio
import os
import shutil
from pathlib import Path
from typing import Dict, List
from uuid import UUID

from ai.backend.common.types import BinarySize

from ..exception import (
    ExecutionError,
    VFolderCreationError,
    VFolderNotFoundError,
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


class XfsVolume(BaseVolume):
    loop: asyncio.AbstractEventLoop
    registry: Dict[str, int]
    project_id_pool: List[int]

    async def init(self, uid=None, gid=None, loop=None) -> None:
        self.registry = {}
        self.project_id_pool = []
        if uid is not None:
            self.uid = uid
        else:
            self.uid = os.getuid()
        if gid is not None:
            self.gid = gid
        else:
            self.gid = os.getgid()
        self.loop = loop or asyncio.get_running_loop()

        if os.path.isfile("/etc/projid"):
            raw_projid = await read_file(self.loop, "/etc/projid")
            for line in raw_projid.splitlines():
                proj_name, proj_id = line.split(":")[:2]
                self.project_id_pool.append(int(proj_id))
                self.registry[proj_name] = int(proj_id)
            self.project_id_pool = sorted(self.project_id_pool)
        else:
            await self.loop.run_in_executor(None, lambda: Path("/etc/projid").touch())

        if not os.path.isfile("/etc/projects"):
            await self.loop.run_in_executor(None, lambda: Path("/etc/projects").touch())

    # ----- volume opeartions -----
    async def create_vfolder(
        self, vfid: UUID, options: VFolderCreationOptions = None
    ) -> None:
        if str(vfid) in self.registry.keys():
            raise VFolderCreationError("VFolder id {} already exists".format(str(vfid)))

        project_id = -1
        # set project_id to the smallest integer not being used
        for i in range(len(self.project_id_pool) - 1):
            if self.project_id_pool[i] + 1 != self.project_id_pool[i + 1]:
                project_id = self.project_id_pool[i] + 1
                break
        if len(self.project_id_pool) == 0:
            project_id = 1
        if project_id == -1:
            project_id = self.project_id_pool[-1] + 1

        vfpath = self.mangle_vfpath(vfid)
        if options is None or options.quota is None:  # max quota i.e. the whole fs size
            fs_usage = await self.get_fs_usage()
            quota = fs_usage.capacity_bytes
        else:
            quota = options.quota
        await self.loop.run_in_executor(
            None, lambda: vfpath.mkdir(0o755, parents=True, exist_ok=False)
        )
        await self.loop.run_in_executor(
            None, lambda: os.chown(vfpath, self.uid, self.gid)
        )

        await write_file(
            self.loop, "/etc/projects", f"{project_id}:{vfpath}\n", perm="a"
        )
        await write_file(self.loop, "/etc/projid", f"{vfid}:{project_id}\n", perm="a")
        await run(f'sudo xfs_quota -x -c "project -s {vfid}" {self.mount_path}')
        await run(
            f'sudo xfs_quota -x -c "limit -p bhard={int(quota)} {vfid}" {self.mount_path}'
        )
        self.registry[str(vfid)] = project_id
        self.project_id_pool += [project_id]
        self.project_id_pool.sort()

    async def delete_vfolder(self, vfid: UUID) -> None:
        if str(vfid) not in self.registry.keys():
            raise VFolderNotFoundError("VFolder with id {} does not exist".format(vfid))

        await run(
            f'sudo xfs_quota -x -c "limit -p bsoft=0 bhard=0 {vfid}" {self.mount_path}'
        )

        raw_projects = await read_file(self.loop, "/etc/projects")
        raw_projid = await read_file(self.loop, "/etc/projid")
        new_projects = ""
        new_projid = ""
        for line in raw_projects.splitlines():
            if line.startswith(str(self.registry[str(vfid)]) + ":"):
                continue
            new_projects += line + "\n"
        for line in raw_projid.splitlines():
            if line.startswith(str(vfid) + ":"):
                continue
            new_projid += line + "\n"
        await write_file(self.loop, "/etc/projects", new_projects)
        await write_file(self.loop, "/etc/projid", new_projid)

        vfpath = self.mangle_vfpath(vfid)

        def _delete_vfolder():
            shutil.rmtree(vfpath)
            if not os.listdir(vfpath.parent):
                vfpath.parent.rmdir()
            if not os.listdir(vfpath.parent.parent):
                vfpath.parent.parent.rmdir()

        await self.loop.run_in_executor(None, lambda: _delete_vfolder())
        self.project_id_pool.remove(self.registry[str(vfid)])
        del self.registry[str(vfid)]

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
            f"sudo xfs_quota -x -c 'report -h' {self.mount_path}"
            f" | grep {str(vfid)[:-5]}"
        )
        if len(report.split()) != 6:
            raise ExecutionError("xfs_quota report output is in unexpected format")
        proj_name, _, _, quota, _, _ = report.split()
        if not str(vfid).startswith(proj_name):
            raise ExecutionError("vfid and project name does not match")
        return BinarySize.finite_from_str(quota)

    async def set_quota(self, vfid: UUID, size_bytes: BinarySize) -> None:
        await run(
            f'sudo xfs_quota -x -c "limit -p bsoft=0 bhard={int(size_bytes)} {vfid}"'
            f" {self.mount_path}"
        )
