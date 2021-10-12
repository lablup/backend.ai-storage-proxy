import asyncio
import logging
import os
from pathlib import Path
from typing import Dict, List
from uuid import UUID

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import BinarySize

from ..exception import ExecutionError, VFolderCreationError
from ..types import VFolderCreationOptions
from ..vfs import BaseVolume, run

log = BraceStyleAdapter(logging.getLogger(__name__))


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
    file_projects: str = "/etc/projects"
    file_projid: str = "/etc/projid"
    backend: BaseVolume
    name_id_map: Dict[UUID, int] = dict()
    project_id_pool: List[int] = list()

    def __init__(self, backend: BaseVolume) -> None:
        self.backend = backend

    async def init(self) -> None:
        if Path(self.file_projid).is_file():
            raw_projid = await read_file(self.file_projid)
            for line in raw_projid.splitlines():
                proj_name, proj_id = line.split(":")[:2]
                self.project_id_pool.append(int(proj_id))
                self.name_id_map[UUID(proj_name)] = int(proj_id)
            self.project_id_pool = sorted(self.project_id_pool)
        else:
            await run(f"sudo touch {self.file_projid}")
        if not Path(self.file_projects).is_file():
            await run(f"sudo touch {self.file_projects}")

    async def add_project_entry(
        self,
        *,
        vfid: UUID,
        quota: int,
        project_id: int = None,
    ) -> None:
        if project_id is None:
            project_id = self.get_project_id()

        # Register project entry.
        vfpath = self.backend.mangle_vfpath(vfid)
        await run(f"sudo sh -c \"echo '{project_id}:{vfpath}' >> {self.file_projects}\"")
        await run(f"sudo sh -c \"echo '{str(vfid)}:{project_id}' >> {self.file_projid}\"")

        self.name_id_map[vfid] = project_id
        self.project_id_pool.append(project_id)
        self.project_id_pool.sort()

    async def remove_project_entry(self, vfid: UUID) -> None:
        # Remove project entry.
        await run(f"sudo sed -i.bak '/{vfid.hex[4:]}/d' {self.file_projects}")
        await run(f"sudo sed -i.bak '/{vfid}/d' {self.file_projid}")

        try:
            self.project_id_pool.remove(self.name_id_map[vfid])
        except ValueError:
            pass
        self.name_id_map.pop(vfid, None)

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
        self.registry = XfsProjectRegistry(self)
        await self.registry.init()

    # ----- volume opeartions -----
    async def create_vfolder(
        self,
        vfid: UUID,
        options: VFolderCreationOptions = None,
    ) -> None:
        await super().create_vfolder(vfid, options)

        # NOTE: Do we need to register project ID for a directory without quota?
        if options is None or options.quota is None:  # max quota i.e. the whole fs size
            fs_usage = await self.get_fs_usage()
            quota = fs_usage.capacity_bytes
        else:
            quota = options.quota
        if quota:
            try:
                log.info("setting project quota (f:{}, q:{})", vfid, str(quota))
                await self.registry.add_project_entry(vfid=vfid, quota=quota)
                await self.set_quota(vfid, quota)
            except Exception as e:
                log.exception("vfolder creation error", exc_info=e)
                await self.delete_vfolder(vfid)
                raise VFolderCreationError("problem in setting vfolder quota")

    async def delete_vfolder(self, vfid: UUID) -> None:
        if vfid in self.registry.name_id_map.keys():
            try:
                log.info("removing project quota (f:{})", vfid)
                await self.set_quota(vfid, BinarySize(0))
            except Exception as e:
                log.exception("vfolder deletion error", exc_info=e)
                pass  # Pass to delete the physical directlry anyway.
            finally:
                await self.registry.remove_project_entry(vfid)
        await super().delete_vfolder(vfid)

    async def get_quota(self, vfid: UUID) -> BinarySize:
        report = await run(
            f"sudo xfs_quota -x -c 'report -h' {self.mount_path}"
            f" | grep {str(vfid)[:-5]}"
        )
        if len(report.split()) != 6:
            raise ExecutionError("unexpected format for xfs_quota report")
        proj_name, _, _, quota, _, _ = report.split()
        if not str(vfid).startswith(proj_name):
            raise ExecutionError("vfid and project name does not match")
        return BinarySize.finite_from_str(quota)

    async def set_quota(self, vfid: UUID, size_bytes: BinarySize) -> None:
        if vfid not in self.registry.name_id_map.keys():
            await run(f"sudo xfs_quota -x -c 'project -s {vfid}' {self.mount_path}")
        await run(
            f"sudo xfs_quota -x -c "
            f"'limit -p bsoft={int(size_bytes)} bhard={int(size_bytes)} {vfid}' "
            f"{self.mount_path}"
        )
