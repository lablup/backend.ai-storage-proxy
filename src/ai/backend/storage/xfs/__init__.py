import asyncio
import os
import shutil
from pathlib import Path
from typing import Dict, List, Union
from uuid import UUID

from ai.backend.common.types import BinarySize

from ..exception import (
    ExecutionError,
    VFolderCreationError,
    VFolderNotFoundError,
)
from ..types import FSUsage, VFolderCreationOptions
from ..vfs import BaseVolume, run


async def read_file(filename: str) -> str:
    def _read():
        with open(filename, "r") as fr:
            return fr.read()

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: _read())


async def write_file(filename: str, contents: str, mode: str = "w"):
    def _write():
        with open(filename, mode) as fw:
            fw.write(contents)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: _write())


class XfsProjectRegistry:
    # TODO: do we need to use these file?
    file_projects: str = "/etc/projects"
    file_projid: str = "/etc/projid"
    mount_path: Path
    name_id_map: Dict[str, int] = dict()
    project_id_pool: List[int] = list()

    def __init__(self, mount_path: Path) -> None:
        self.mount_path = mount_path

    async def init(self) -> None:
        if Path(self.file_projid).is_file():
            raw_projid = await read_file(self.file_projid)
            for line in raw_projid.splitlines():
                proj_name, proj_id = line.split(":")[:2]
                self.project_id_pool.append(int(proj_id))
                self.name_id_map[proj_name] = int(proj_id)
            self.project_id_pool = sorted(self.project_id_pool)
        else:
            await run(f"sudo touch {self.file_projid}")
            # await loop.run_in_executor(None, lambda: Path("/etc/projid").touch())
        if not Path(self.file_projects).is_file():
            await run(f"sudo touch {self.file_projects}")
            # await loop.run_in_executor(None, lambda: Path("/etc/projects").touch())

    async def add_project(
        self,
        *,
        vfpath: Union[str, Path],
        vfid: UUID,
        quota: int,
        project_id: int = None,
        mode: str = "a",
    ) -> None:
        if project_id is None:
            project_id = self.get_project_id()
        await run(f"sudo sh -c \"echo '{project_id}:{vfpath}' >> {self.file_projects}\"")
        await run(f"sudo sh -c \"echo '{str(vfid)}:{project_id}' >> {self.file_projid}\"")
        await run(f"sudo xfs_quota -x -c 'project -s {str(vfid)}' {self.mount_path}")
        await run(
            f"sudo xfs_quota -x -c 'limit -p bsoft={int(quota)} bhard={int(quota)} {str(vfid)}' "
            f"{self.mount_path}"
        )
        self.name_id_map[str(vfid)] = project_id
        self.project_id_pool.append(project_id)
        self.project_id_pool.sort()

    async def remove_project(self, vfid: UUID) -> None:
        await run(f"sudo sed -e \"/{str(vfid)[4:]}/d\" {self.file_projects}")
        await run(f"sudo sed -e \"/{str(vfid)}/d\" {self.file_projid}")
        self.project_id_pool.remove(self.name_id_map[str(vfid)])
        del self.name_id_map[str(vfid)]

    def get_project_id(self) -> int:
        """
        Get the next project_id, which is the smallest unused integer.
        """
        project_id = -1
        for i in range(len(self.project_id_pool) - 1):
            if self.project_id_pool[i] + 1 != self.project_id_pool[i + 1]:
                project_id = self.project_id_pool[i] + 1
                break
        if len(self.project_id_pool) == 0:
            project_id = 1
        if project_id == -1:
            project_id = self.project_id_pool[-1] + 1
        return project_id


class XfsVolume(BaseVolume):
    """
    XFS volume backend. XFS natively supports per-directory quota through
    the project qutoa. To enalbe project quota, the XFS volume should be
    mounted with `-o pquota` option.

    This backend requires `root` or no password `sudo` permission to run
    `xfs_quota` command and write to `/etc/projects` and `/etc/projid`.
    """
    registry: XfsProjectRegistry

    async def init(self, uid: int = None, gid: int = None) -> None:
        self.uid = uid if uid is not None else os.getuid()
        self.gid = gid if gid is not None else os.getgid()
        self.registry = XfsProjectRegistry(self.mount_path)
        await self.registry.init()

    # ----- volume opeartions -----
    async def create_vfolder(
        self,
        vfid: UUID,
        options: VFolderCreationOptions = None,
    ) -> None:
        if str(vfid) in self.registry.name_id_map.keys():
            raise VFolderCreationError("VFolder ID {} already exists".format(str(vfid)))

        vfpath = self.mangle_vfpath(vfid)
        if options is None or options.quota is None:  # max quota i.e. the whole fs size
            fs_usage = await self.get_fs_usage()
            quota = fs_usage.capacity_bytes
        else:
            quota = options.quota
        await run(f"sudo mkdir -m 755 -p {vfpath}")
        await run(f"sudo chown {self.uid}.{self.gid} {vfpath}")
        await run(f"sudo chown {self.uid}.{self.gid} {vfpath.parent}")
        await run(f"sudo chown {self.uid}.{self.gid} {vfpath.parent.parent}")

        # TODO: Do we need to register project ID for a directory without quota?
        await self.registry.add_project(
            vfpath=vfpath, vfid=vfid, quota=quota, mode="a",
        )

    async def delete_vfolder(self, vfid: UUID) -> None:
        loop = asyncio.get_running_loop()
        vfpath = self.mangle_vfpath(vfid)

        if str(vfid) not in self.registry.name_id_map.keys():
            raise VFolderNotFoundError("VFolder with id {} does not exist".format(vfid))

        # TODO: Do we need to unset the quota even if we are deleting
        #       the directory and project information?
        await run(
            f'sudo xfs_quota -x -c "limit -p bsoft=0 bhard=0 {vfid}" {self.mount_path}'
        )
        await self.registry.remove_project(vfid)

        def _delete_vfolder():
            shutil.rmtree(vfpath)
            if not os.listdir(vfpath.parent):
                vfpath.parent.rmdir()
            if not os.listdir(vfpath.parent.parent):
                vfpath.parent.parent.rmdir()

        await loop.run_in_executor(None, lambda: _delete_vfolder())

    async def get_fs_usage(self) -> FSUsage:
        stat = await run(f"\\df -h {self.mount_path} | grep {self.mount_path}")
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
